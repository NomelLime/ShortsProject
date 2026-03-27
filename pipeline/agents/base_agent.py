"""
pipeline/agents/base_agent.py — Базовый класс для всех агентов ShortsProject.

Каждый агент:
  - запускается в отдельном потоке через start() / stop()
  - имеет доступ к AgentMemory (self.memory)
  - автоматически регистрирует статус в памяти
  - поддерживает прерываемый sleep() (stop() будит поток)
  - отправляет Telegram-уведомления через self._send()
  - сохраняет отчёты через self.report()
"""
from __future__ import annotations

import logging
import threading
import time
import traceback
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, Callable, Dict, Optional


class AgentStatus(Enum):
    IDLE    = "IDLE"
    RUNNING = "RUNNING"
    WAITING = "WAITING"   # ждёт GPU или ресурса
    ERROR   = "ERROR"
    STOPPED = "STOPPED"


class BaseAgent(ABC):
    """
    Базовый класс для всех агентов.

    Подклассы обязаны реализовать run().
    run() вызывается внутри потока, запущенного через start().

    Паттерн использования в run():
        def run(self) -> None:
            while not self.should_stop:
                self._set_status(AgentStatus.RUNNING, "работа")
                # ... логика ...
                if not self.sleep(interval):
                    break
    """

    def __init__(
        self,
        name: str,
        memory=None,       # AgentMemory — опционально
        notify: Any = None,
    ) -> None:
        self.name          = name
        self.memory        = memory
        self._notify       = notify
        self.status        = AgentStatus.IDLE
        self._last_error: Optional[str] = None
        self._start_time:  Optional[float] = None

        self._stop_event   = threading.Event()
        self._thread:      Optional[threading.Thread] = None

        self.logger = logging.getLogger(f"agent.{name.lower()}")

        # Регистрируем в памяти при создании
        if self.memory:
            self.memory.register_agent(name)

    # ------------------------------------------------------------------
    # Абстрактный метод
    # ------------------------------------------------------------------

    @abstractmethod
    def run(self) -> None:
        """Основная логика агента. Запускается в отдельном потоке."""
        ...

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Запустить агента в фоновом потоке."""
        if self._thread and self._thread.is_alive():
            self.logger.warning("[%s] Уже запущен", self.name)
            return
        self._stop_event.clear()
        self._start_time = time.monotonic()
        self._thread = threading.Thread(
            target=self._run_wrapper,
            name=f"agent-{self.name.lower()}",
            daemon=True,
        )
        self._thread.start()
        self.logger.info("[%s] Поток запущен", self.name)

    def stop(self, timeout: float = 10.0) -> None:
        """Запросить остановку и дождаться завершения потока."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        self._set_status(AgentStatus.STOPPED)
        self.logger.info("[%s] Остановлен", self.name)

    def _run_wrapper(self) -> None:
        """Обёртка: перехватывает ошибки, обновляет статус."""
        try:
            self.run()
        except Exception as exc:
            self._last_error = str(exc)
            self._set_status(AgentStatus.ERROR, str(exc)[:120])
            self.logger.error(
                "[%s] Необработанная ошибка: %s\n%s",
                self.name, exc, traceback.format_exc(),
            )

    # ------------------------------------------------------------------
    # Управление потоком
    # ------------------------------------------------------------------

    @property
    def should_stop(self) -> bool:
        """True если получен сигнал на остановку."""
        return self._stop_event.is_set()

    def sleep(self, seconds: float) -> bool:
        """
        Прерываемый sleep. Возвращает False если был вызван stop().

        Использование:
            if not self.sleep(60):
                return  # агент остановлен
        """
        return not self._stop_event.wait(timeout=seconds)

    def get_uptime(self) -> Optional[float]:
        """Uptime агента в секундах или None если не запущен."""
        if self._start_time is None:
            return None
        return round(time.monotonic() - self._start_time, 1)

    # ------------------------------------------------------------------
    # Статус и отчётность
    # ------------------------------------------------------------------

    def _set_status(self, status: AgentStatus, detail: str = "") -> None:
        """Обновить статус + записать в AgentMemory."""
        self.status = status
        if self.memory:
            status_str = status.value if not detail else f"{status.value}: {detail}"
            self.memory.set_agent_status(self.name, status_str)

    def report(self, data: Dict[str, Any]) -> None:
        """Сохранить произвольный отчёт агента в AgentMemory."""
        if self.memory:
            self.memory.set_agent_report(self.name, data)

    def set_human_detail(self, text: str) -> None:
        """Краткое описание текущего действия для панели (ContentHub)."""
        if self.memory:
            self.memory.set_human_detail(self.name, text)

    def _send(self, message: str) -> None:
        """Отправить Telegram-уведомление (если notify задан)."""
        try:
            if callable(self._notify):
                self._notify(message)
            else:
                from pipeline.notifications import send_telegram
                send_telegram(message)
        except Exception as e:
            self.logger.debug("[%s] Уведомление не отправлено: %s", self.name, e)

    # ------------------------------------------------------------------
    # LLM-коммуникация между агентами
    # ------------------------------------------------------------------

    def _build_llm_context(self, peer_agents: list) -> str:
        """
        Собирает рекомендации от peer_agents и возвращает строку для включения в промпт.

        Иерархия: STRATEGIST первым (наивысший приоритет), остальные по порядку.
        Если рекомендаций нет — возвращает пустую строку.

        Пример вывода:
            [КОНТЕКСТ ОТ АГЕНТОВ]
            - STRATEGIST (цикл 3): Используй динамичные заголовки для Reels
            - SCOUT (цикл 7): Ниша 'cooking shorts' +60% за последние 2 часа
        """
        if not self.memory:
            return ""

        all_recs = self.memory.read_all_recommendations_for(self.name)
        if not all_recs:
            return ""

        # Фильтруем только запрошенных агентов
        peer_lower = [p.lower() for p in peer_agents]
        filtered = {
            agent: rec
            for agent, rec in all_recs.items()
            if agent in peer_lower
        }
        if not filtered:
            return ""

        lines = ["[КОНТЕКСТ ОТ АГЕНТОВ]"]
        for agent, rec in filtered.items():
            cycle   = rec.get("cycle", "?")
            content = rec.get("content", "").strip()
            lines.append(f"- {agent.upper()} (цикл {cycle}): {content}")

        return "\n".join(lines)

    def _call_ollama_with_fallback(
        self,
        prompt: str,
        fallback_value,
        context_description: str,
    ):
        """
        Вызывает Ollama с промптом. При недоступности или ошибке парсинга
        возвращает fallback_value и логирует событие в AgentMemory.

        Все LLM-вызовы агентов должны идти через этот метод (не напрямую).
        GPU-lock (GPUManager) накладывается снаружи, где нужно.

        Возвращает строку ответа или fallback_value.
        """
        from pipeline.ai import check_ollama, ollama_generate_with_timeout
        from pipeline.config import OLLAMA_MODEL

        def _fallback(reason: str):
            msg = (
                f"[{self.name}] решение принято без LLM совета: "
                f"{context_description}, использован дефолт ({reason})"
            )
            self.logger.warning(msg)
            if self.memory:
                self.memory.log_event(
                    self.name,
                    "llm_fallback",
                    {"reason": reason, "context": context_description},
                )
            return fallback_value

        try:
            if not check_ollama():
                return _fallback("Ollama недоступен")
        except Exception as exc:
            return _fallback(f"check_ollama() ошибка: {exc}")

        try:
            response = ollama_generate_with_timeout(OLLAMA_MODEL, prompt)
            text = response.get("response", "").strip()
            if not text:
                return _fallback("пустой ответ от Ollama")
            return text
        except TimeoutError:
            return _fallback("Ollama timeout")
        except Exception as exc:
            return _fallback(f"Ollama ошибка: {exc}")

    def __repr__(self) -> str:
        return f"<Agent {self.name} [{self.status.value}]>"
