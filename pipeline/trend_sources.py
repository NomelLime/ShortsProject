"""
pipeline/trend_sources.py — Адаптеры источников трендов для TrendScout.

Источники:
  1. pytrends  — Google Trends (требует: pip install pytrends)
  2. VK Video  — заглушка (до подключения нативного источника)
  3. RuTube    — заглушка (до подключения нативного источника)
  4. OK        — заглушка (до подключения нативного источника)

Каждый адаптер возвращает список строк-ключевых слов/хэштегов.
TrendScout агрегирует их и взвешивает по частоте появления.
"""
from __future__ import annotations

import logging
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
    Trending YouTube через yt-dlp с cookies из get_ytdlp_cookie_options()
    (фиксированный аккаунт в .env или контекст ротации в цикле TREND_SCOUT — см. pipeline_account_rotation).
    """
    keywords: List[str] = []
    try:
        from yt_dlp import YoutubeDL

        from pipeline import config as _cfg

        ydl_opts: dict = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,
            "skip_download": True,
            "ignoreerrors": True,
            "socket_timeout": 30,
        }
        ydl_opts.update(_cfg.get_ytdlp_cookie_options())
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(
                "https://www.youtube.com/feed/trending",
                download=False,
            )
        entries = info.get("entries") or []
        for ent in entries[:max_results]:
            title = ""
            if isinstance(ent, dict):
                title = (ent.get("title") or ent.get("fulltitle") or "").strip()
            elif isinstance(ent, str):
                title = ent.strip()
            if not title:
                continue
            words = title.split()
            if len(words) >= 2:
                keywords.append(" ".join(words[:3]))
    except Exception as exc:
        logger.warning("[TrendSources] YouTube Trending ошибка: %s", exc)

    logger.debug("[TrendSources] YouTube Trending: %d ключевых слов", len(keywords))
    return keywords[:max_results]


# ── TikTok Creative Center ─────────────────────────────────────────────────

def fetch_tiktok_trends(max_results: int = 20) -> List[str]:
    """
    Трендовые хэштеги TikTok Creative Center (публичный API).
    Прокси — load_proxy() (тот же mobileproxy / PROXY, что и у пайплайна).
    """
    keywords = []
    try:
        import json

        import requests

        from pipeline import utils as u

        url = (
            "https://ads.tiktok.com/creative_radar_api/v1/popular_trend/hashtag/list"
            "?page=1&limit=50&period=7&country_code=US"
        )
        proxies = u.requests_proxies_from_proxy_url(u.load_proxy())
        resp = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0",
                "Accept": "application/json",
                "Referer": "https://ads.tiktok.com/",
            },
            timeout=15,
            proxies=proxies,
        )
        if resp.status_code != 200:
            logger.debug("[TrendSources] TikTok API HTTP %s", resp.status_code)
            return keywords[:max_results]
        data = resp.json()
        items = data.get("data", {}).get("list", [])
        for item in items[:max_results]:
            tag = item.get("hashtag_name", "")
            if tag:
                keywords.append(tag)
    except Exception as exc:
        logger.debug("[TrendSources] TikTok Creative Center ошибка: %s", exc)

    logger.debug("[TrendSources] TikTok Trends: %d ключевых слов", len(keywords))
    return keywords[:max_results]


# ── VK Video / RuTube / OK (публичные заглушки) ──────────────────────────────

def fetch_vk_video_trends(max_results: int = 20) -> List[str]:
    """Тренды VK Video: пока без стабильного публичного API, возвращаем пусто."""
    logger.debug("[TrendSources] VK Video Trends источник пока не реализован")
    return []


def fetch_rutube_trends(max_results: int = 20) -> List[str]:
    """Тренды RuTube: пока без стабильного публичного API, возвращаем пусто."""
    logger.debug("[TrendSources] RuTube Trends источник пока не реализован")
    return []


def fetch_ok_trends(max_results: int = 20) -> List[str]:
    """Тренды OK: пока без стабильного публичного API, возвращаем пусто."""
    logger.debug("[TrendSources] OK Trends источник пока не реализован")
    return []
