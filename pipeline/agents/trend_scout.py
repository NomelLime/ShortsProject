"""
pipeline/agents/trend_scout.py — TREND_SCOUT: Агент мониторинга трендов.

Запускается каждые TREND_SCOUT_INTERVAL_H часов (по умолчанию 2).

Алгоритм:
  1. Опрашивает источники: Google Trends, YouTube Trending, TikTok Creative Center
  2. Взвешивает ключевые слова по частоте появления во всех источниках
  3. Записывает топ-N в agent_memory["trend_scores"] (dict: keyword → score)
  4. Scout.py читает trend_scores и приоритизирует ключевые слова выше threshold

Конфиг (через .env):
  TREND_SCOUT_ENABLED       = 1           — включить/выключить
  TREND_SCOUT_INTERVAL_H    = 2           — интервал в часах
  TREND_SCOUT_THRESHOLD     = 2           — min score для передачи Scout-у
  TREND_SCOUT_TOP_N         = 30          — максимум ключевых слов в trend_scores
  TREND_SCOUT_GEO           = ""          — ISO-код страны для Google Trends (пусто = глобально)
  TREND_SCOUT_SOURCES       = google,yt,tiktok — какие источники использовать
"""
from __future__ import annotations

import logging
from collections import Counter
from typing import Any, Dict, List, Optional

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)


class TrendScout(BaseAgent):

    def __init__(
        self,
        memory: Optional[AgentMemory] = None,
        notify: Any = None,
    ) -> None:
        super().__init__("TREND_SCOUT", memory or get_memory(), notify)

    def run(self) -> None:
        from pipeline import config as cfg
        logger.info("[TREND_SCOUT] Запущен")

        interval_sec = int(getattr(cfg, "TREND_SCOUT_INTERVAL_H", 2)) * 3600

        # Первый запуск сразу
        self._collect_trends()

        while not self.should_stop:
            if not self.sleep(interval_sec):
                break
            self._collect_trends()

    # ── Основной сбор ────────────────────────────────────────────────────

    def _collect_trends(self) -> None:
        from pipeline import config as cfg

        if not getattr(cfg, "TREND_SCOUT_ENABLED", True):
            logger.debug("[TREND_SCOUT] Отключён — пропускаем")
            return

        self._set_status(AgentStatus.RUNNING, "сбор трендов")
        self.set_human_detail("Собираю трендовые ключевые слова из внешних источников")
        try:
            sources_str = str(getattr(cfg, "TREND_SCOUT_SOURCES", "google,yt,tiktok"))
            sources = [s.strip() for s in sources_str.split(",") if s.strip()]
            geo     = str(getattr(cfg, "TREND_SCOUT_GEO", ""))
            top_n   = int(getattr(cfg, "TREND_SCOUT_TOP_N", 30))

            # Загружаем seed-ключевые слова из файла (для Google Related)
            seed_keywords = self._load_seed_keywords(cfg)

            # Опрашиваем источники
            all_keywords: List[str] = []
            self._fetch_sources(sources, geo, seed_keywords, all_keywords)

            if not all_keywords:
                logger.info("[TREND_SCOUT] Ни один источник не вернул данные")
                self._set_status(AgentStatus.IDLE)
                return

            # Взвешиваем по частоте
            counter = Counter(kw.lower().strip() for kw in all_keywords if kw.strip())
            trend_scores: Dict[str, int] = dict(counter.most_common(top_n))

            # Сохраняем в AgentMemory
            self.memory.set("trend_scores", trend_scores)
            self.memory.set("trend_scores_updated_at", __import__("datetime").datetime.utcnow().isoformat())

            threshold = int(getattr(cfg, "TREND_SCOUT_THRESHOLD", 2))
            top_trends = {k: v for k, v in trend_scores.items() if v >= threshold}

            logger.info(
                "[TREND_SCOUT] Обновлено: %d ключевых слов, %d выше порога %d",
                len(trend_scores), len(top_trends), threshold,
            )

            if top_trends:
                top_str = ", ".join(f"{k}({v})" for k, v in list(top_trends.items())[:10])
                self._send(f"📈 [TrendScout] Топ-тренды: {top_str}")

        except Exception as exc:
            logger.error("[TREND_SCOUT] Ошибка сбора трендов: %s", exc, exc_info=True)
        finally:
            self._set_status(AgentStatus.IDLE)

    def _fetch_sources(
        self,
        sources: List[str],
        geo: str,
        seed_keywords: List[str],
        output: List[str],
    ) -> None:
        """Опрашивает включённые источники, добавляет ключевые слова в output."""
        from pipeline.trend_sources import (
            fetch_google_trends,
            fetch_youtube_trending,
            fetch_tiktok_trends,
        )

        if "google" in sources:
            try:
                kws = fetch_google_trends(seed_keywords=seed_keywords, geo=geo)
                output.extend(kws)
                logger.debug("[TREND_SCOUT] Google: %d", len(kws))
            except Exception as exc:
                logger.warning("[TREND_SCOUT] Google Trends ошибка: %s", exc)

        if "yt" in sources or "youtube" in sources:
            try:
                kws = fetch_youtube_trending()
                output.extend(kws)
                logger.debug("[TREND_SCOUT] YouTube: %d", len(kws))
            except Exception as exc:
                logger.warning("[TREND_SCOUT] YouTube Trending ошибка: %s", exc)

        if "tiktok" in sources:
            try:
                kws = fetch_tiktok_trends()
                output.extend(kws)
                logger.debug("[TREND_SCOUT] TikTok: %d", len(kws))
            except Exception as exc:
                logger.warning("[TREND_SCOUT] TikTok Trends ошибка: %s", exc)

    @staticmethod
    def _load_seed_keywords(cfg) -> List[str]:
        """Читает keywords.txt для seed-ключевых слов (Google Related Queries)."""
        try:
            kw_file = cfg.KEYWORDS_FILE
            if kw_file.exists():
                return [
                    line.strip()
                    for line in kw_file.read_text(encoding="utf-8").splitlines()
                    if line.strip() and not line.startswith("#")
                ][:5]  # не более 5 seed для pytrends
        except Exception:
            pass
        return []
