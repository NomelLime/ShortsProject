from __future__ import annotations

from typing import Any, Dict, List

from pipeline.agents.base_agent import AgentStatus, BaseAgent
from pipeline.agent_memory import AgentMemory, get_memory


class CommentToContentAgent(BaseAgent):
    """Преобразует обратную связь в очередь новых идей."""

    def __init__(self, memory: AgentMemory | None = None, notify: Any = None) -> None:
        super().__init__("COMMENT_TO_CONTENT", memory or get_memory(), notify)

    def run(self) -> None:
        while not self.should_stop:
            self._set_status(AgentStatus.IDLE)
            self.set_human_detail("Ожидаю комментарии/аналитику для генерации идей")
            self.sleep(180.0)

    @staticmethod
    def build_ideas_from_comments(comments: List[str]) -> List[Dict[str, str]]:
        ideas: List[Dict[str, str]] = []
        seen = set()
        for raw in comments:
            text = (raw or "").strip()
            if not text:
                continue
            key = text.lower()[:80]
            if key in seen:
                continue
            seen.add(key)
            idea_type = "question" if "?" in text else "objection"
            ideas.append(
                {
                    "idea_type": idea_type,
                    "prompt_seed": text[:220],
                    "title_hint": f"{'Ответ на вопрос' if idea_type == 'question' else 'Разбор возражения'}: {text[:60]}",
                }
            )
        return ideas[:30]
