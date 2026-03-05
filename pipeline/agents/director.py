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
        self._max_restarts = 3

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
        for name, agent in self._agents.items():
            if agent.status == AgentStatus.ERROR:
                logger.warning("[DIRECTOR] Агент %s в статусе ERROR — пробую перезапустить", name)
                self.restart_agent(name)
