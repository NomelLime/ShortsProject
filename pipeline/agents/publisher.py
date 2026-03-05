"""pipeline/agents/publisher.py — PUBLISHER: загрузка видео на платформы."""
from __future__ import annotations
import logging
from typing import Any, Optional
from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

class Publisher(BaseAgent):
    def __init__(self, memory: Optional[AgentMemory] = None, notify: Any = None) -> None:
        super().__init__("PUBLISHER", memory or get_memory(), notify)

    def run(self) -> None:
        from pipeline.uploader import run_upload_phase
        logger.info("[PUBLISHER] Запущен")
        while not self.should_stop:
            self._set_status(AgentStatus.IDLE)
            self.sleep(60.0)
