"""
pipeline/agents/director.py — DIRECTOR: центральный оркестратор ShortsProject.

Отвечает за:
  - Запуск и остановку всех агентов
  - Управление очерёдностью задач
  - Перезапуск упавших агентов
  - Предоставление статуса системы по запросу
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agents.gpu_manager import get_gpu_manager
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

# Порядок запуска агентов (по зависимостям)
BOOT_ORDER = [
    "SENTINEL",     # мониторинг — первым
    "SCOUT",        # поиск контента
    "CURATOR",      # фильтрация
    "VISIONARY",    # метаданные
    "NARRATOR",     # TTS
    "EDITOR",       # монтаж
    "STRATEGIST",   # аналитика
    "GUARDIAN",     # безопасность
    "PUBLISHER",    # загрузка
    "ACCOUNTANT",   # лимиты
]


class Director(BaseAgent):
    """
    Центральный оркестратор.

    Запускается первым (после COMMANDER), управляет всеми
    остальными агентами через общий реестр.
    """

    def __init__(
        self,
        memory: Optional[AgentMemory] = None,
        notify: Any = None,
    ) -> None:
        super().__init__("DIRECTOR", memory or get_memory(), notify)
        self._agents: Dict[str, BaseAgent] = {}
        self._gpu = get_gpu_manager()
        self._restart_count: Dict[str, int] = {}
        self._restart_count_reset_ts: float = time.monotonic()
        self._max_restarts = 3
        self._RESTART_COUNT_RESET_SEC = 3600  # сбрасываем счётчики раз в час

    # ------------------------------------------------------------------
    # Управление реестром
    # ------------------------------------------------------------------

    def register(self, agent: BaseAgent) -> None:
        """Зарегистрировать агента в реестре."""
        self._agents[agent.name] = agent
        logger.info("[DIRECTOR] Зарегистрирован агент: %s", agent.name)

    def get_agent(self, name: str) -> Optional[BaseAgent]:
        return self._agents.get(name)

    # ------------------------------------------------------------------
    # Запуск / остановка
    # ------------------------------------------------------------------

    def start_all(self) -> None:
        """Запустить всех агентов в порядке BOOT_ORDER."""
        self._set_status(AgentStatus.RUNNING)
        for name in BOOT_ORDER:
            agent = self._agents.get(name)
            if agent:
                try:
                    agent.start()
                    time.sleep(0.3)  # небольшая пауза между запусками
                    logger.info("[DIRECTOR] Запущен: %s", name)
                except Exception as e:
                    logger.error("[DIRECTOR] Не удалось запустить %s: %s", name, e)
            else:
                logger.debug("[DIRECTOR] Агент %s не зарегистрирован, пропуск", name)

        self._send("🚀 [DIRECTOR] Все агенты запущены")

    def stop_all(self) -> None:
        """Остановить всех агентов в обратном порядке."""
        for name in reversed(BOOT_ORDER):
            agent = self._agents.get(name)
            if agent:
                try:
                    agent.stop()
                    logger.info("[DIRECTOR] Остановлен: %s", name)
                except Exception as e:
                    logger.error("[DIRECTOR] Ошибка остановки %s: %s", name, e)
        self._set_status(AgentStatus.STOPPED)
        self._send("🛑 [DIRECTOR] Все агенты остановлены")

    def restart_agent(self, name: str) -> bool:
        """Перезапустить конкретного агента."""
        agent = self._agents.get(name)
        if not agent:
            logger.warning("[DIRECTOR] Агент %s не найден для перезапуска", name)
            return False

        count = self._restart_count.get(name, 0)
        if count >= self._max_restarts:
            logger.error(
                "[DIRECTOR] Агент %s превысил лимит перезапусков (%d)",
                name, self._max_restarts
            )
            self._send(f"❌ [DIRECTOR] {name} недоступен после {count} перезапусков")
            return False

        try:
            agent.stop(timeout=5.0)
            time.sleep(1.0)
            agent.start()
            self._restart_count[name] = count + 1
            logger.info("[DIRECTOR] Агент %s перезапущен (%d/%d)", name, count + 1, self._max_restarts)
            self._send(f"🔄 [DIRECTOR] {name} перезапущен ({count + 1}/{self._max_restarts})")
            return True
        except Exception as e:
            logger.error("[DIRECTOR] Ошибка перезапуска %s: %s", name, e)
            return False

    # ------------------------------------------------------------------
    # Мониторинг здоровья
    # ------------------------------------------------------------------

    def health_check(self) -> Dict[str, Any]:
        """Проверка состояния всех агентов."""
        result = {}
        for name, agent in self._agents.items():
            result[name] = {
                "status":  agent.status.value,
                "uptime":  agent.get_uptime(),
                "error":   agent._last_error,
            }
        return result

    def full_status(self) -> Dict[str, Any]:
        """Полный статус системы."""
        return {
            "director":  self.status.value,
            "gpu":       self._gpu.status(),
            "agents":    self.health_check(),
            "memory":    self.memory.summary(),
        }

    # ------------------------------------------------------------------
    # run() — сторожевой цикл
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Сторожевой цикл: каждые 60 секунд проверяет агентов
        и перезапускает упавших.
        """
        logger.info("[DIRECTOR] Сторожевой цикл запущен")
        while not self.should_stop:
            self._watchdog()
            if not self.sleep(60.0):
                break

    def _watchdog(self) -> None:
        self.set_human_detail("Слежу за агентами и перезапускаю при ошибках")
        # 0. Сбрасываем счётчики перезапуска раз в час (чтобы временные сбои не блокировали навсегда)
        if time.monotonic() - self._restart_count_reset_ts >= self._RESTART_COUNT_RESET_SEC:
            if self._restart_count:
                logger.info("[DIRECTOR] Сброс счётчиков перезапусков: %s", self._restart_count)
                self._restart_count.clear()
            self._restart_count_reset_ts = time.monotonic()

        # 1. Обрабатываем запросы на рестарт от SENTINEL
        just_restarted = self._process_sentinel_requests()

        # 2. Собственный watchdog: агенты в ERROR без участия SENTINEL
        for name, agent in self._agents.items():
            if agent.status == AgentStatus.ERROR and name not in just_restarted:
                logger.warning(
                    "[DIRECTOR] Агент %s в статусе ERROR — пробую перезапустить", name
                )
                self.restart_agent(name)

    def _process_sentinel_requests(self) -> set:
        """
        Читает список AgentMemory["sentinel_restart_requests"] и перезапускает
        каждого агента из списка. После обработки очищает список.

        SENTINEL пишет в этот ключ имена агентов, которые были в ERROR > 2 мин.
        Возвращает set имён агентов, которые были обработаны (для предотвращения
        двойного рестарта в основном watchdog цикле).
        """
        requests: list = self.memory.get("sentinel_restart_requests", [])
        if not requests:
            return set()

        processed = []
        for agent_name in requests:
            agent = self._agents.get(agent_name)
            if agent is None:
                logger.warning(
                    "[DIRECTOR] SENTINEL запросил рестарт %s, но агент не зарегистрирован",
                    agent_name,
                )
                processed.append(agent_name)
                continue

            # Не перезапускаем агентов, которые уже восстановились сами
            if agent.status not in (AgentStatus.ERROR,):
                logger.info(
                    "[DIRECTOR] Агент %s уже в статусе %s, рестарт SENTINEL пропущен",
                    agent_name, agent.status.value,
                )
                processed.append(agent_name)
                continue

            logger.info("[DIRECTOR] Рестарт по запросу SENTINEL: %s", agent_name)
            self.restart_agent(agent_name)
            processed.append(agent_name)

        if processed:
            # Удаляем обработанные запросы
            remaining = [r for r in requests if r not in processed]
            self.memory.set("sentinel_restart_requests", remaining)

        return set(processed)
