"""
pipeline/agents/scout.py — SCOUT: мониторинг трендов и сбор URL.

Оборачивает pipeline/downloader.py:
  - search_and_save()          → полный цикл поиска
  - _expand_keywords_with_ai() → расширение ключевых слов (с GPU lock)
  - merge_and_save_urls()      → сохранение без дубликатов

Цикл: каждые interval_sec секунд запускает поиск.
COMMANDER может установить scout_keywords_override в AgentMemory.
"""
from __future__ import annotations

import logging
from typing import Any, List, Optional

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agents.gpu_manager import get_gpu_manager, GPUPriority
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)
_DEFAULT_INTERVAL = 3600  # 1 час


class Scout(BaseAgent):
    def __init__(
        self,
        memory: Optional[AgentMemory] = None,
        notify: Any = None,
        interval_sec: int = _DEFAULT_INTERVAL,
    ) -> None:
        super().__init__("SCOUT", memory or get_memory(), notify)
        self._interval    = interval_sec
        self._gpu         = get_gpu_manager()
        self._total_found = 0

    def run(self) -> None:
        logger.info("[SCOUT] Запущен, интервал=%ds", self._interval)
        self._crawl_cycle()
        while not self.should_stop:
            if not self.sleep(self._interval):
                break
            self._crawl_cycle()

    # ------------------------------------------------------------------

    def _crawl_cycle(self) -> None:
        self._set_status(AgentStatus.RUNNING, "поиск URL")
        try:
            from pipeline import config
            from pipeline.utils import load_keywords, merge_and_save_urls

            override = self.memory.get("scout_keywords_override")
            if override:
                keywords = override if isinstance(override, list) else [override]
                logger.info("[SCOUT] Override-ключевые слова: %s", keywords[:5])
            else:
                keywords = load_keywords()

            if not keywords:
                logger.warning("[SCOUT] Нет ключевых слов — пропускаю цикл")
                return

            logger.info("[SCOUT] Ключевых слов: %d", len(keywords))
            expanded = self._expand_keywords(keywords)

            new_urls = self._search_ytdlp(expanded)
            if self.memory.get("scout_browser_enabled", True):
                new_urls = list(dict.fromkeys(new_urls + self._search_browser(expanded)))

            if not new_urls:
                logger.info("[SCOUT] Новых URL не найдено")
                return

            saved = merge_and_save_urls(new_urls, config.URLS_FILE)
            self._total_found += saved
            logger.info("[SCOUT] Сохранено %d новых URL (сессия: %d)", saved, self._total_found)
            self.memory.log_event("SCOUT", "crawl_done", {
                "found": len(new_urls), "saved_new": saved,
                "total_session": self._total_found,
            })
            self.report({"last_saved": saved, "total_found": self._total_found})
            if saved > 0:
                self._send(f"🔍 [SCOUT] Найдено {saved} новых URL (сессия: {self._total_found})")

        except Exception as e:
            logger.error("[SCOUT] Ошибка: %s", e)
            raise
        finally:
            if self.status != AgentStatus.ERROR:
                self._set_status(AgentStatus.IDLE)

    def _expand_keywords(self, keywords: List[str]) -> List[str]:
        try:
            from pipeline import config
            if not getattr(config, "AI_KEYWORD_EXPANSION", False):
                return keywords
            self._set_status(AgentStatus.WAITING, "ожидание GPU")
            with self._gpu.acquire("SCOUT", GPUPriority.LLM):
                self._set_status(AgentStatus.RUNNING, "AI расширение KW")
                from pipeline.downloader import _expand_keywords_with_ai
                return _expand_keywords_with_ai(keywords)
        except Exception as e:
            logger.warning("[SCOUT] AI расширение не удалось: %s", e)
            return keywords

    def _search_ytdlp(self, keywords: List[str]) -> List[str]:
        try:
            from pipeline.utils import load_proxy
            from pipeline.downloader import _search_ytdlp
            urls = _search_ytdlp(keywords, load_proxy())
            logger.info("[SCOUT] yt-dlp: %d URL", len(urls))
            return urls
        except Exception as e:
            logger.warning("[SCOUT] yt-dlp не удался: %s", e)
            return []

    def _search_browser(self, keywords: List[str]) -> List[str]:
        try:
            from pipeline.utils import load_proxy
            from pipeline.downloader import _search_browser
            urls = _search_browser(keywords, load_proxy())
            logger.info("[SCOUT] Браузер: %d URL", len(urls))
            return urls
        except Exception as e:
            logger.warning("[SCOUT] Браузерный поиск не удался: %s", e)
            return []
