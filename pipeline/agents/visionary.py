"""pipeline/agents/visionary.py — VISIONARY: метаданные, хуки, A/B тексты."""
from __future__ import annotations
import logging
from typing import Any, Optional
from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agents.gpu_manager import get_gpu_manager, GPUPriority
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

class Visionary(BaseAgent):
    def __init__(self, memory: Optional[AgentMemory] = None, notify: Any = None) -> None:
        super().__init__("VISIONARY", memory or get_memory(), notify)
        self._gpu = get_gpu_manager()

    def run(self) -> None:
        from pipeline.ai import generate_video_metadata
        logger.info("[VISIONARY] Запущен")
        while not self.should_stop:
            self._set_status(AgentStatus.IDLE)
            self.sleep(30.0)

    def generate_metadata(self, video_path, num_variants: int = 2):
        """Генерирует метаданные через Ollama (с GPU-блокировкой)."""
        from pipeline.ai import generate_video_metadata
        with self._gpu.acquire("VISIONARY", GPUPriority.LLM):
            return generate_video_metadata(video_path, num_variants=num_variants)
