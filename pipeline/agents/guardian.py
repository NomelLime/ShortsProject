"""pipeline/agents/guardian.py — GUARDIAN: прокси, сессии, антибан."""
from __future__ import annotations
import logging
from typing import Any, Optional
from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

class Guardian(BaseAgent):
    def __init__(self, memory: Optional[AgentMemory] = None, notify: Any = None) -> None:
        super().__init__("GUARDIAN", memory or get_memory(), notify)

    def run(self) -> None:
        from pipeline.session_manager import SessionManager
        logger.info("[GUARDIAN] Запущен")
        while not self.should_stop:
            self._check_sessions()
            self.sleep(300.0)  # каждые 5 минут

    def _check_sessions(self) -> None:
        self._set_status(AgentStatus.RUNNING, "проверка сессий")
        try:
            from pipeline.session_manager import SessionManager
            # TODO: проверять статус сессий и ротировать при необходимости
            self._set_status(AgentStatus.IDLE)
        except Exception as e:
            logger.warning("[GUARDIAN] Ошибка проверки сессий: %s", e)
            self._set_status(AgentStatus.IDLE)
