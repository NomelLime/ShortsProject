"""
pipeline/agents/commander.py — COMMANDER: интерфейс пользователя ↔ система.

Принимает команды через Telegram или CLI, анализирует их через Ollama,
даёт советы, делегирует нужным агентам, отчитывается.

Поток команды:
  Пользователь → COMMANDER.handle_command()
    → Ollama: разбор намерений + советы
    → Director: делегирование агентам
    → Отчёт пользователю через Telegram/CLI
"""
from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from datetime import datetime
from typing import Any, Dict, List, Optional

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

# Шаблон анализа команды через Ollama
_PARSE_PROMPT = """Ты — помощник системы автоматической публикации видео.
Тебе дана команда от оператора. Ответь ТОЛЬКО валидным JSON без пояснений.

Команда: {command}

Текущее состояние системы:
{state}

Формат ответа:
{{
  "intent": "одно из: start|stop|config|query|add_accounts|set_limits|set_content|restart|status|custom",
  "targets": ["список агентов которых затрагивает команда, из: SCOUT,METRICS_SCOUT_PLATFORM,CURATOR,VISIONARY,NARRATOR,EDITOR,STRATEGIST,GUARDIAN,PUBLISHER,ACCOUNTANT,SENTINEL,DIRECTOR"],
  "params": {{"ключ": "значение", "...": "..."}},
  "risks": ["список потенциальных рисков, пустой массив если нет"],
  "advice": "совет оператору (1-2 предложения, на русском)",
  "requires_confirmation": true
}}

requires_confirmation = true если команда затрагивает > 5 аккаунтов, меняет лимиты или останавливает агентов."""


class CommandResult:
    """Результат обработки команды."""
    def __init__(
        self,
        command: str,
        intent: str,
        parsed: Dict,
        executed: bool,
        message: str,
    ) -> None:
        self.command   = command
        self.intent    = intent
        self.parsed    = parsed
        self.executed  = executed
        self.message   = message
        self.ts        = datetime.now().isoformat(timespec="seconds")

    def to_dict(self) -> Dict:
        return {
            "command":  self.command,
            "intent":   self.intent,
            "parsed":   self.parsed,
            "executed": self.executed,
            "message":  self.message,
            "ts":       self.ts,
        }


class Commander(BaseAgent):
    """
    Агент-интерфейс между пользователем и системой.

    Примеры команд:
      "покажи статус"
      "добавь 5 аккаунтов VK Video"
      "остановить всё"
      "публиковать только cooking контент"
      "установить лимит 3 видео/день для новых аккаунтов"
    """

    def __init__(
        self,
        director: Any = None,          # Director — передаётся после инициализации
        memory: Optional[AgentMemory] = None,
        notify: Any = None,
        auto_confirm: bool = False,    # True = не спрашивать подтверждение
    ) -> None:
        super().__init__("COMMANDER", memory or get_memory(), notify)
        self.director      = director
        self.auto_confirm  = auto_confirm
        self._command_queue: deque = deque()
        self._history: List[CommandResult] = []
        self._pending_confirm: Optional[Dict] = None
        self._queue_lock   = threading.Lock()

    # ------------------------------------------------------------------
    # Публичный API — принимает команды извне (Telegram, CLI)
    # ------------------------------------------------------------------

    def handle_command(self, command: str, confirmed: bool = False) -> str:
        """
        Обрабатывает текстовую команду.

        Returns:
            Строка с ответом для пользователя.
        """
        command = command.strip()
        if not command:
            return "⚠️ Пустая команда"

        logger.info("[COMMANDER] Получена команда: %s", command)
        self.memory.log_event("COMMANDER", "command_received", {"command": command})

        # Быстрые команды без LLM
        quick = self._quick_command(command)
        if quick is not None:
            return quick

        # Если ожидаем подтверждения
        if self._pending_confirm and command.lower() in ("да", "yes", "y", "подтверди", "ок"):
            return self._execute_pending()
        if self._pending_confirm and command.lower() in ("нет", "no", "n", "отмена", "отменить"):
            self._pending_confirm = None
            return "✅ Команда отменена"

        # Основной путь: анализ через Ollama
        try:
            parsed = self._parse_command(command)
        except Exception as e:
            logger.error("[COMMANDER] Ошибка парсинга команды: %s", e)
            return f"❌ Не удалось разобрать команду: {e}"

        # Формируем ответ
        advice   = parsed.get("advice", "")
        risks    = parsed.get("risks", [])
        requires = parsed.get("requires_confirmation", False)

        reply_parts = []
        if advice:
            reply_parts.append(f"💡 {advice}")
        if risks:
            reply_parts.append("⚠️ Риски:\n" + "\n".join(f"  • {r}" for r in risks))

        if requires and not confirmed and not self.auto_confirm:
            self._pending_confirm = {"command": command, "parsed": parsed}
            reply_parts.append(
                "\n❓ Команда требует подтверждения. Напиши «да» для выполнения или «нет» для отмены."
            )
            return "\n".join(reply_parts)

        # Выполняем
        result = self._execute(command, parsed)
        reply_parts.append(result.message)

        self._history.append(result)
        self.memory.set_agent_report("COMMANDER", {
            "last_command": command,
            "last_result":  result.to_dict(),
        })

        return "\n".join(reply_parts)

    def confirm(self) -> str:
        """Подтвердить ожидающую команду."""
        return self._execute_pending()

    def cancel(self) -> str:
        """Отменить ожидающую команду."""
        self._pending_confirm = None
        return "✅ Отменено"

    def get_history(self, last_n: int = 10) -> List[Dict]:
        return [r.to_dict() for r in self._history[-last_n:]]

    # ------------------------------------------------------------------
    # run() — слушаем очередь команд (для фонового режима)
    # ------------------------------------------------------------------

    def run(self) -> None:
        logger.info("[COMMANDER] Ожидание команд...")
        while not self.should_stop:
            with self._queue_lock:
                if self._command_queue:
                    cmd = self._command_queue.popleft()
                else:
                    cmd = None
            if cmd:
                self.set_human_detail(f"Разбираю команду: {cmd[:80]}{'…' if len(cmd) > 80 else ''}")
                result = self.handle_command(cmd)
                self._send(result)
                self.set_human_detail("Жду следующую команду в Telegram или CLI")
            else:
                self.set_human_detail("Жду команды оператора")
            self.sleep(1.0)

    def enqueue(self, command: str) -> None:
        """Добавить команду в очередь (для асинхронного вызова)."""
        with self._queue_lock:
            self._command_queue.append(command)

    # ------------------------------------------------------------------
    # Быстрые команды (без LLM)
    # ------------------------------------------------------------------

    def _quick_command(self, command: str) -> Optional[str]:
        """Обработка простых команд без вызова Ollama."""
        cmd = command.lower()

        if cmd in ("статус", "status", "/status"):
            return self._status_report()

        if cmd in ("/collect_native_metrics", "collect_native_metrics", "собери нативные метрики"):
            if not self.director:
                return "⚠️ DIRECTOR не подключён"
            agent = self.director.get_agent("METRICS_SCOUT_PLATFORM")
            if not agent:
                self.memory.set("metrics_scout_platform_force", True)
                return "⚠️ METRICS_SCOUT_PLATFORM не найден в реестре. Флаг принудительного запуска установлен."
            trigger = getattr(agent, "trigger_now", None)
            if callable(trigger):
                trigger()
                return "✅ METRICS_SCOUT_PLATFORM: запуск сбора нативных метрик запрошен."
            self.memory.set("metrics_scout_platform_force", True)
            return "✅ Флаг принудительного сбора установлен."

        if cmd in ("помощь", "help", "/help", "?"):
            return self._help_text()

        if cmd in ("история", "history", "/history"):
            history = self.get_history(5)
            if not history:
                return "📋 История команд пуста"
            lines = [f"📋 Последние команды:"]
            for h in reversed(history):
                lines.append(f"  [{h['ts']}] {h['command']} → {h['intent']}")
            return "\n".join(lines)

        return None

    # ------------------------------------------------------------------
    # Парсинг через Ollama
    # ------------------------------------------------------------------

    def _parse_command(self, command: str) -> Dict:
        """Разбирает команду через Ollama, возвращает структурированный dict."""
        # Проверяем кешированный статус Ollama — избегаем 30-60с timeout на каждую команду
        ollama_ok = self.memory.get("ollama_available")
        if ollama_ok is False:
            return self._fallback_parse(command)

        try:
            import ollama  # type: ignore
        except ImportError:
            # Fallback: базовый парсер без LLM
            return self._fallback_parse(command)

        state = json.dumps(self.memory.summary(), ensure_ascii=False)
        prompt = _PARSE_PROMPT.format(command=command, state=state[:2000])

        try:
            response = ollama.generate(
                model="qwen2.5-vl:7b",
                prompt=prompt,
                options={"temperature": 0.1},
            )
        except Exception as e:
            logger.warning("[COMMANDER] Ollama недоступен: %s — переключаюсь на fallback", e)
            self.memory.set("ollama_available", False)
            return self._fallback_parse(command)

        raw = response.get("response", "{}")
        # Убираем возможные markdown-теги
        raw = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("[COMMANDER] Ollama вернул не-JSON: %s", raw[:200])
            return self._fallback_parse(command)

        # Валидация — не доверяем LLM-ответу напрямую
        VALID_INTENTS = {"start", "stop", "config", "query", "add_accounts",
                         "set_limits", "set_content", "restart", "status", "custom"}
        VALID_TARGETS = {"SCOUT", "METRICS_SCOUT_PLATFORM", "CURATOR", "VISIONARY", "NARRATOR", "EDITOR",
                         "STRATEGIST", "GUARDIAN", "PUBLISHER", "ACCOUNTANT",
                         "SENTINEL", "DIRECTOR"}
        if parsed.get("intent") not in VALID_INTENTS:
            parsed["intent"] = "custom"
        parsed["targets"] = [t for t in parsed.get("targets", []) if t in VALID_TARGETS]
        parsed["requires_confirmation"] = bool(parsed.get("requires_confirmation", False))
        return parsed

    def _fallback_parse(self, command: str) -> Dict:
        """Простой парсер без LLM для базовых команд."""
        cmd = command.lower()
        intent = "custom"
        targets: List[str] = []

        if any(w in cmd for w in ["старт", "start", "запуст"]):
            intent, targets = "start", ["DIRECTOR"]
        elif any(w in cmd for w in ["стоп", "stop", "останов"]):
            intent, targets = "stop", ["DIRECTOR"]
        elif any(w in cmd for w in ["статус", "status"]):
            intent, targets = "status", ["DIRECTOR"]
        elif "аккаунт" in cmd:
            intent, targets = "add_accounts", ["GUARDIAN", "ACCOUNTANT"]
        elif "лимит" in cmd:
            intent, targets = "set_limits", ["ACCOUNTANT"]
        elif "контент" in cmd:
            intent, targets = "set_content", ["SCOUT", "CURATOR"]

        return {
            "intent":                intent,
            "targets":               targets,
            "params":                {},
            "risks":                 [],
            "advice":                "",
            "requires_confirmation": intent in ("stop", "add_accounts"),
        }

    # ------------------------------------------------------------------
    # Исполнение
    # ------------------------------------------------------------------

    def _execute(self, command: str, parsed: Dict) -> CommandResult:
        """Делегирует команду нужным агентам через Director."""
        intent  = parsed.get("intent", "custom")
        targets = parsed.get("targets", [])
        params  = parsed.get("params", {})

        if not self.director:
            return CommandResult(
                command, intent, parsed, False,
                "⚠️ DIRECTOR не подключён — команда записана, но не выполнена"
            )

        try:
            message = self._dispatch(intent, targets, params, command)
            executed = True
        except Exception as e:
            message  = f"❌ Ошибка выполнения: {e}"
            executed = False
            logger.exception("[COMMANDER] Ошибка dispatch: %s", e)

        return CommandResult(command, intent, parsed, executed, message)

    def _dispatch(self, intent: str, targets: List[str], params: Dict, raw: str) -> str:
        """Выполняет команду."""
        d = self.director

        if intent == "status":
            status = d.full_status()
            lines = ["📊 Статус системы:"]
            for name, info in status.get("agents", {}).items():
                emoji = {"running": "🟢", "idle": "⚪", "error": "🔴",
                         "stopped": "⛔", "waiting": "🟡"}.get(info["status"].lower().split(":")[0], "❓")
                lines.append(f"  {emoji} {name}: {info['status']}")
            gpu = status.get("gpu", {})
            lines.append(f"\n🖥️ GPU: активных={len(gpu.get('active', {}))}, очередь={gpu.get('queue_size', 0)}")
            return "\n".join(lines)

        elif intent == "start":
            d.start_all()
            return "✅ Все агенты запущены"

        elif intent == "stop":
            d.stop_all()
            return "🛑 Все агенты остановлены"

        elif intent == "restart":
            results = []
            for name in (targets or ["DIRECTOR"]):
                ok = d.restart_agent(name)
                results.append(f"{'✅' if ok else '❌'} {name}")
            return "Перезапуск:\n" + "\n".join(results)

        elif intent == "add_accounts":
            count    = int(params.get("count", 1))
            platform = params.get("platform", "all")
            # Сохраняем задачу в memory для GUARDIAN/ACCOUNTANT
            self.memory.set("pending_add_accounts", {
                "count": count, "platform": platform, "params": params
            })
            return f"✅ Задача добавить {count} аккаунт(ов) [{platform}] поставлена в очередь"

        elif intent == "set_limits":
            self.memory.set("custom_limits", params)
            return f"✅ Лимиты обновлены: {params}"

        elif intent == "set_content":
            keywords = params.get("keywords", [])
            if keywords:
                self.memory.set("scout_keywords_override", keywords)
            return f"✅ Контент-настройки обновлены"

        else:
            # Общая команда — сохраняем в memory и логируем
            self.memory.set("last_custom_command", {"raw": raw, "params": params})
            self.memory.log_event("COMMANDER", "custom_command", {"raw": raw})
            return f"✅ Команда '{raw}' принята и записана"

    def _execute_pending(self) -> str:
        if not self._pending_confirm:
            return "⚠️ Нет ожидающих команд"
        cmd    = self._pending_confirm["command"]
        parsed = self._pending_confirm["parsed"]
        self._pending_confirm = None
        result = self._execute(cmd, parsed)
        self._history.append(result)
        return result.message

    # ------------------------------------------------------------------
    # Отчёты
    # ------------------------------------------------------------------

    def _status_report(self) -> str:
        if not self.director:
            return "⚠️ DIRECTOR не подключён"
        status = self.director.full_status()
        lines = ["📊 *Статус ShortsProject*\n"]
        for name, info in status.get("agents", {}).items():
            raw_status = info.get("status", "")
            emoji = {"running": "🟢", "idle": "⚪", "error": "🔴",
                     "stopped": "⛔", "waiting": "🟡"}.get(raw_status.lower().split(":")[0], "❓")
            uptime = info.get("uptime")
            uptime_str = f" (работает {int(uptime)}с)" if uptime else ""
            lines.append(f"{emoji} {name}{uptime_str}")
        return "\n".join(lines)

    def _help_text(self) -> str:
        return (
            "📖 *Доступные команды:*\n\n"
            "• `статус` — показать состояние всех агентов\n"
            "• `история` — последние 5 команд\n"
            "• `старт` — запустить всех агентов\n"
            "• `стоп` — остановить всех агентов\n"
            "• `добавь N аккаунтов [платформа]` — добавить аккаунты\n"
            "• `установи лимит X видео/день` — изменить лимиты\n"
            "• `публикуй только [тема] контент` — фильтр контента\n"
            "• `перезапусти [АГЕНТ]` — перезапустить агента\n\n"
            "• `/collect_native_metrics` — принудительный сбор нативных метрик\n\n"
            "Или пиши свободным текстом — Ollama разберёт 🤖"
        )
