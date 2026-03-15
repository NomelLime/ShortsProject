"""
pipeline/trend_sources.py — Адаптеры источников трендов для TrendScout.

Источники:
  1. pytrends  — Google Trends (требует: pip install pytrends)
  2. yt-dlp    — YouTube Trending (без аутентификации)
  3. TikTok    — Creative Center (простой HTTP-запрос)

Каждый адаптер возвращает список строк-ключевых слов/хэштегов.
TrendScout агрегирует их и взвешивает по частоте появления.
"""
from __future__ import annotations

import logging
import subprocess
import time
from typing import List, Optional

logger = logging.getLogger(__name__)


# ── Google Trends ──────────────────────────────────────────────────────────

def fetch_google_trends(
    seed_keywords: Optional[List[str]] = None,
    geo: str = "",
    timeframe: str = "now 1-d",
    max_results: int = 20,
) -> List[str]:
    """
    Возвращает топ-трендовые запросы из Google Trends.
    geo = "" → глобально; "RU", "US" и т.д. → по стране.
    Требует: pip install pytrends
    """
    try:
        from pytrends.request import TrendReq  # type: ignore
    except ImportError:
        logger.debug("[TrendSources] pytrends не установлен — пропускаем Google Trends")
        return []

    trends = []
    try:
        pt = TrendReq(hl="ru-RU", tz=180, timeout=(5, 15), retries=2, backoff_factor=0.5)

        # Ежедневные трендовые запросы
        try:
            daily = pt.trending_searches(pn=geo.lower() if geo else "global")
            trends.extend(daily[0].tolist()[:max_results])
        except Exception as exc:
            logger.debug("[TrendSources] Ошибка daily trends: %s", exc)

        # Связанные запросы для seed-ключевых слов
        if seed_keywords:
            seeds = seed_keywords[:3]  # не более 5 в одном запросе pytrends
            try:
                pt.build_payload(seeds, timeframe=timeframe, geo=geo)
                related = pt.related_queries()
                for kw in seeds:
                    top = related.get(kw, {}).get("top")
                    if top is not None and not top.empty:
                        trends.extend(top["query"].tolist()[:5])
            except Exception as exc:
                logger.debug("[TrendSources] Ошибка related queries: %s", exc)

    except Exception as exc:
        logger.warning("[TrendSources] Google Trends ошибка: %s", exc)

    # Удаляем дубликаты, сохраняем порядок
    seen = set()
    result = []
    for t in trends:
        t = str(t).strip()
        if t and t not in seen:
            seen.add(t)
            result.append(t)

    logger.debug("[TrendSources] Google Trends: %d ключевых слов", len(result))
    return result[:max_results]


# ── YouTube Trending (yt-dlp) ──────────────────────────────────────────────

def fetch_youtube_trending(max_results: int = 20) -> List[str]:
    """
    Скачивает список trending-видео с YouTube через yt-dlp (без аутентификации).
    Извлекает теги/категории как ключевые слова.
    """
    keywords = []
    try:
        result = subprocess.run(
            [
                "yt-dlp",
                "--flat-playlist",
                "--print", "%(title)s",
                "--max-downloads", str(max_results),
                "--no-warnings",
                "--quiet",
                "https://www.youtube.com/feed/trending",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            titles = [t.strip() for t in result.stdout.strip().splitlines() if t.strip()]
            # Превращаем заголовки в ключевые слова: берём первые 3 слова каждого
            for title in titles[:max_results]:
                words = title.split()
                if len(words) >= 2:
                    keywords.append(" ".join(words[:3]))
        else:
            logger.debug("[TrendSources] yt-dlp trending: code=%d", result.returncode)
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as exc:
        logger.debug("[TrendSources] YouTube Trending ошибка: %s", exc)

    logger.debug("[TrendSources] YouTube Trending: %d ключевых слов", len(keywords))
    return keywords[:max_results]


# ── TikTok Creative Center ─────────────────────────────────────────────────

def fetch_tiktok_trends(max_results: int = 20) -> List[str]:
    """
    Получает трендовые хэштеги из TikTok Creative Center через публичный API.
    """
    keywords = []
    try:
        import urllib.request
        url = "https://ads.tiktok.com/creative_radar_api/v1/popular_trend/hashtag/list?page=1&limit=50&period=7&country_code=US"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0",
            "Accept": "application/json",
            "Referer": "https://ads.tiktok.com/",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            import json
            data = json.loads(resp.read().decode("utf-8"))
            items = data.get("data", {}).get("list", [])
            for item in items[:max_results]:
                tag = item.get("hashtag_name", "")
                if tag:
                    keywords.append(tag)
    except Exception as exc:
        logger.debug("[TrendSources] TikTok Creative Center ошибка: %s", exc)

    logger.debug("[TrendSources] TikTok Trends: %d ключевых слов", len(keywords))
    return keywords[:max_results]
