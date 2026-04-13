from __future__ import annotations

from typing import Any, Dict, Tuple

from pipeline.agents.base_agent import AgentStatus, BaseAgent
from pipeline.agent_memory import AgentMemory, get_memory


class RiskGuardAgent(BaseAgent):
    """Pre-upload риск-гейт для метаданных и подачи."""

    BLOCKED_WORDS = {
        "violence",
        "self-harm",
        "drugs",
        "hate",
        "суицид",
        "наркот",
        "экстрем",
    }

    def __init__(self, memory: AgentMemory | None = None, notify: Any = None) -> None:
        super().__init__("RISK_GUARD", memory or get_memory(), notify)

    def run(self) -> None:
        while not self.should_stop:
            self._set_status(AgentStatus.IDLE)
            self.set_human_detail("Проверяю риск-политики перед публикацией")
            self.sleep(120.0)

    @classmethod
    def score_metadata_risk(cls, meta: Dict[str, Any]) -> Tuple[float, bool, str]:
        title = str((meta or {}).get("title") or "").lower()
        desc = str((meta or {}).get("description") or "").lower()
        blob = f"{title} {desc}"
        score = 0.05
        for w in cls.BLOCKED_WORDS:
            if w in blob:
                score += 0.35
        if len(blob) > 1200:
            score += 0.15
        score = min(1.0, round(score, 4))
        blocked = score >= 0.7
        reason = "policy_keywords" if blocked else "ok"
        return score, blocked, reason
