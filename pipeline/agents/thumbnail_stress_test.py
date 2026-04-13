from __future__ import annotations

from typing import Any, Dict

from pipeline.agents.base_agent import AgentStatus, BaseAgent
from pipeline.agent_memory import AgentMemory, get_memory


class ThumbnailStressTestAgent(BaseAgent):
    """Оценивает заметность превью/титульного кадра (быстрый эвристический тест)."""

    def __init__(self, memory: AgentMemory | None = None, notify: Any = None) -> None:
        super().__init__("THUMB_STRESS", memory or get_memory(), notify)

    def run(self) -> None:
        while not self.should_stop:
            self._set_status(AgentStatus.IDLE)
            self.set_human_detail("Оцениваю thumbnail readability")
            self.sleep(180.0)

    @staticmethod
    def estimate_score(meta: Dict[str, Any]) -> float:
        title = str((meta or {}).get("title") or "")
        if not title:
            return 0.2
        score = 0.3
        if len(title) <= 55:
            score += 0.25
        if any(ch.isdigit() for ch in title):
            score += 0.15
        if "?" in title:
            score += 0.2
        return round(min(0.95, score), 4)
