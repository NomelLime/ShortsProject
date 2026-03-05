"""pipeline/agents/curator.py — CURATOR: качество, дедупликация, вирусный потенциал."""
from __future__ import annotations
import logging
from typing import Any, Optional
from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

class Curator(BaseAgent):
    def __init__(self, memory: Optional[AgentMemory] = None, notify: Any = None) -> None:
        super().__init__("CURATOR", memory or get_memory(), notify)

    def run(self) -> None:
        """Слушает очередь загруженных видео и фильтрует их."""
        from pipeline.utils import compute_phash
        logger.info("[CURATOR] Запущен")
        while not self.should_stop:
            self._set_status(AgentStatus.IDLE)
            self.sleep(30.0)
