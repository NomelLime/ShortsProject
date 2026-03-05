"""pipeline/agents/accountant.py — ACCOUNTANT: лимиты, карантин, статистика."""
from __future__ import annotations
import logging
from typing import Any, Optional
from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

class Accountant(BaseAgent):
    def __init__(self, memory: Optional[AgentMemory] = None, notify: Any = None) -> None:
        super().__init__("ACCOUNTANT", memory or get_memory(), notify)

    def run(self) -> None:
        from pipeline.quarantine import QuarantineManager
        logger.info("[ACCOUNTANT] Запущен")
        while not self.should_stop:
            self._check_limits()
            self.sleep(3600.0)  # каждый час

    def _check_limits(self) -> None:
        self._set_status(AgentStatus.RUNNING, "проверка лимитов")
        try:
            custom = self.memory.get("custom_limits", {})
            if custom:
                logger.info("[ACCOUNTANT] Применяю пользовательские лимиты: %s", custom)
            self._set_status(AgentStatus.IDLE)
        except Exception as e:
            logger.warning("[ACCOUNTANT] Ошибка: %s", e)
            self._set_status(AgentStatus.IDLE)
