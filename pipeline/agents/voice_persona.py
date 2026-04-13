from __future__ import annotations

from typing import Any, Dict

from pipeline.agents.base_agent import AgentStatus, BaseAgent
from pipeline.agent_memory import AgentMemory, get_memory


class VoicePersonaAgent(BaseAgent):
    """Подмешивает voice persona hints в метаданные (опционально)."""

    def __init__(self, memory: AgentMemory | None = None, notify: Any = None) -> None:
        super().__init__("VOICE_PERSONA", memory or get_memory(), notify)

    def run(self) -> None:
        while not self.should_stop:
            self._set_status(AgentStatus.IDLE)
            self.set_human_detail("Ожидаю задачу на тональность и voice persona")
            self.sleep(180.0)

    @staticmethod
    def apply_persona(meta: Dict[str, Any], persona: str = "confident_fast") -> Dict[str, Any]:
        m = dict(meta or {})
        m["voice_persona"] = persona
        if persona == "confident_fast":
            m["narration_speed_hint"] = 1.08
            m["narration_energy_hint"] = "high"
        elif persona == "calm_authoritative":
            m["narration_speed_hint"] = 0.95
            m["narration_energy_hint"] = "medium"
        return m
