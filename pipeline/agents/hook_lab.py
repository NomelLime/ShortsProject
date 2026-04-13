from __future__ import annotations

import re
from typing import Any, Dict, List

from pipeline.agents.base_agent import AgentStatus, BaseAgent
from pipeline.agent_memory import AgentMemory, get_memory


class HookLabAgent(BaseAgent):
    """Генератор и скорер первых 1-3 секунд (hook-first)."""

    def __init__(self, memory: AgentMemory | None = None, notify: Any = None) -> None:
        super().__init__("HOOK_LAB", memory or get_memory(), notify)

    def run(self) -> None:
        while not self.should_stop:
            self._set_status(AgentStatus.IDLE)
            self.set_human_detail("Ожидаю метаданные для hook scoring")
            self.sleep(120.0)

    @staticmethod
    def score_hook(hook_text: str) -> float:
        txt = (hook_text or "").strip()
        if not txt:
            return 0.05
        score = 0.2
        if len(txt) <= 80:
            score += 0.2
        if "?" in txt:
            score += 0.2
        if re.search(r"\b(why|how|secret|truth|шок|неожидан|секрет|почему|как)\b", txt, flags=re.IGNORECASE):
            score += 0.25
        if re.search(r"[0-9]", txt):
            score += 0.1
        return min(0.99, round(score, 4))

    @classmethod
    def annotate_variants(cls, variants: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for idx, item in enumerate(variants):
            if not isinstance(item, dict):
                continue
            v = dict(item)
            hook = str(v.get("hook_text") or "")
            v["hook_score"] = cls.score_hook(hook)
            v["creative_id"] = str(v.get("creative_id") or f"creative_{idx:02d}")
            v["hook_type"] = str(v.get("hook_type") or ("question" if "?" in hook else "statement"))
            out.append(v)
        return out
