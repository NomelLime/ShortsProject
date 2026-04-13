from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from pipeline.agents.base_agent import AgentStatus, BaseAgent
from pipeline.agent_memory import AgentMemory, get_memory


class AutoReporterAgent(BaseAgent):
    """Формирует ежедневную выжимку 'что менять завтра'."""

    def __init__(self, memory: AgentMemory | None = None, notify: Any = None) -> None:
        super().__init__("AUTO_REPORTER", memory or get_memory(), notify)

    def run(self) -> None:
        while not self.should_stop:
            self._set_status(AgentStatus.RUNNING, "формирую отчёт")
            report = self._build_daily_report()
            self.memory.set("auto_reporter_last", report)
            self.memory.emit_agent_event(
                "AUTO_REPORTER",
                "daily_report_generated",
                report,
                experiment_id=str(report.get("experiment_id") or ""),
            )
            self._set_status(AgentStatus.IDLE)
            self.sleep(6 * 3600)

    def _build_daily_report(self) -> Dict[str, Any]:
        recs = self.memory.get("strategist_recommendations", {}) or {}
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "experiment_id": "daily_autoreport",
            "actions": [
                "Увеличить долю hooks с вопросом в первых 2 секундах",
                "Снизить риск-порог публикаций с низким hook_score",
                "Повторить лучший формат прошлого цикла и протестировать 2 новых варианта",
            ],
            "signals": {
                "has_strategist_data": bool(recs),
            },
        }
