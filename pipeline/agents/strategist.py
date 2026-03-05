"""pipeline/agents/strategist.py — STRATEGIST: A/B анализ, расписание, оптимизация."""
from __future__ import annotations
import logging
from typing import Any, Optional
from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

class Strategist(BaseAgent):
    def __init__(self, memory: Optional[AgentMemory] = None, notify: Any = None) -> None:
        super().__init__("STRATEGIST", memory or get_memory(), notify)
        self._interval_sec = 21600  # каждые 6 часов

    def run(self) -> None:
        from pipeline.analytics import load_analytics
        logger.info("[STRATEGIST] Запущен")
        while not self.should_stop:
            self._analyse()
            self.sleep(self._interval_sec)

    def _analyse(self) -> None:
        self._set_status(AgentStatus.RUNNING, "анализ A/B")
        try:
            from pipeline.analytics import load_analytics
            data = load_analytics()
            # Сохраняем рекомендации в memory для PUBLISHER/ACCOUNTANT
            self.memory.set("strategist_recommendations", {
                "best_time_slots": self._find_best_times(data),
                "top_content_types": self._find_top_content(data),
            })
            self._set_status(AgentStatus.IDLE)
        except Exception as e:
            logger.warning("[STRATEGIST] Ошибка анализа: %s", e)
            self._set_status(AgentStatus.IDLE)

    def _find_best_times(self, data) -> list:
        return []  # TODO Этап 6

    def _find_top_content(self, data) -> list:
        return []  # TODO Этап 6
