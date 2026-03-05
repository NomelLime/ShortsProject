"""pipeline/agents/scout.py — SCOUT: мониторинг трендов и сбор URL."""
from __future__ import annotations
import logging, time
from typing import Any, Optional
from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

class Scout(BaseAgent):
    """Периодически ищет новые видео по ключевым словам."""

    def __init__(self, memory: Optional[AgentMemory] = None, notify: Any = None) -> None:
        super().__init__("SCOUT", memory or get_memory(), notify)
        self._interval_sec = 3600  # каждый час

    def run(self) -> None:
        from pipeline.downloader import run_download_phase
        logger.info("[SCOUT] Запущен, интервал %ds", self._interval_sec)
        while not self.should_stop:
            self._set_status(AgentStatus.RUNNING, "поиск")
            try:
                # Проверяем override от COMMANDER
                kw_override = self.memory.get("scout_keywords_override")
                logger.info("[SCOUT] Запуск сбора URL (override=%s)", bool(kw_override))
                run_download_phase()
                self.memory.log_event("SCOUT", "crawl_done", {})
                self._set_status(AgentStatus.IDLE)
                self._send("🔍 [SCOUT] Сбор URL завершён")
            except Exception as e:
                logger.error("[SCOUT] Ошибка сбора: %s", e)
                self._set_status(AgentStatus.ERROR, str(e))
                raise
            if not self.sleep(self._interval_sec):
                break
