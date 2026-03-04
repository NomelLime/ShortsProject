"""
pipeline/analytics.py — Сбор аналитики после загрузки видео.

Через 24–72 часа после публикации заходит на платформу и собирает:
  - просмотры (views)
  - лайки (likes)
  - комментарии (comments)

Данные сохраняются в data/analytics.json и позволяют понять,
какие теги, ключевые слова и темы дают лучшие результаты.

Структура analytics.json:
  {
    "video_stem": {
      "title": "...",
      "tags": [...],
      "uploads": {
        "youtube": {
          "url":          "https://...",
          "uploaded_at":  "2024-01-15T10:00:00",
          "collected_at": "2024-01-16T12:00:00",
          "views": 1500, "likes": 80, "comments": 12
        },
        "tiktok":    {...},
        "instagram": {...}
      }
    }
  }

Интеграция:
  - Запись о загруженном видео добавляется в analytics.json в uploader.py
    через register_upload().
  - Сбор статистики вызывается из scheduler.py или вручную:
      from pipeline.analytics import collect_pending_analytics
      collect_pending_analytics()
"""

from __future__ import annotations

import json
import logging
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from rebrowser_playwright.sync_api import BrowserContext, Page

from pipeline import config, utils
from pipeline.browser import launch_browser, close_browser
from pipeline.notifications import send_telegram

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Хранилище
# ─────────────────────────────────────────────────────────────────────────────

def _load_analytics() -> Dict:
    if not config.ANALYTICS_FILE.exists():
        return {}
    try:
        return json.loads(config.ANALYTICS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_analytics(data: Dict) -> None:
    try:
        config.ANALYTICS_FILE.parent.mkdir(parents=True, exist_ok=True)
        config.ANALYTICS_FILE.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.error("Не удалось сохранить analytics.json: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Регистрация загрузки
# ─────────────────────────────────────────────────────────────────────────────

def register_upload(
    video_stem: str,
    platform: str,
    video_url: str,
    meta: Dict,
) -> None:
    """
    Регистрирует факт загрузки видео в analytics.json.
    Вызывается из uploader.py сразу после успешной публикации.

    video_url — публичный URL опубликованного видео на платформе.
                Если URL неизвестен сразу — передать пустую строку,
                он будет заполнен при первом сборе статистики.
    """
    data = _load_analytics()
    entry = data.setdefault(video_stem, {
        "title":   meta.get("title", ""),
        "tags":    meta.get("tags", []),
        "uploads": {},
    })

    entry["uploads"][platform] = {
        "url":          video_url,
        "uploaded_at":  datetime.now().isoformat(timespec="seconds"),
        "collected_at": None,
        "views":        None,
        "likes":        None,
        "comments":     None,
    }

    _save_analytics(data)
    logger.debug("[analytics] Зарегистрирована загрузка: %s / %s", video_stem, platform)


def get_pending_collection() -> List[Dict]:
    """
    Возвращает список записей, для которых пора собирать аналитику:
      - загружены > ANALYTICS_COLLECT_AFTER_HOURS назад
      - загружены < ANALYTICS_COLLECT_MAX_HOURS назад (не слишком старые)
      - статистика ещё не собрана (collected_at is None)
    """
    data    = _load_analytics()
    pending = []
    now     = datetime.now()

    for stem, entry in data.items():
        for platform, upload in entry.get("uploads", {}).items():
            if upload.get("collected_at") is not None:
                continue  # уже собрано
            uploaded_at_str = upload.get("uploaded_at")
            if not uploaded_at_str:
                continue
            try:
                uploaded_at = datetime.fromisoformat(uploaded_at_str)
            except Exception:
                continue
            age_hours = (now - uploaded_at).total_seconds() / 3600
            if age_hours < config.ANALYTICS_COLLECT_AFTER_HOURS:
                continue  # ещё рано
            if age_hours > config.ANALYTICS_COLLECT_MAX_HOURS:
                logger.debug("[analytics] Пропуск старой записи: %s/%s (%.0f ч)", stem, platform, age_hours)
                continue  # слишком старое
            pending.append({
                "stem":     stem,
                "platform": platform,
                "url":      upload.get("url", ""),
                "title":    entry.get("title", ""),
                "tags":     entry.get("tags", []),
            })

    return pending


# ─────────────────────────────────────────────────────────────────────────────
# Парсеры статистики по платформам
# ─────────────────────────────────────────────────────────────────────────────

def _collect_youtube_stats(page: Page, video_url: str) -> Optional[Dict]:
    """
    Собирает статистику YouTube через YouTube Studio.
    Если video_url — публичный URL вида youtube.com/shorts/ID или /watch?v=ID,
    конвертирует в URL студии.
    """
    # Извлекаем video_id из URL
    video_id = None
    if "shorts/" in video_url:
        video_id = video_url.split("shorts/")[-1].split("?")[0].split("/")[0]
    elif "watch?v=" in video_url:
        video_id = video_url.split("watch?v=")[-1].split("&")[0]
    elif "youtu.be/" in video_url:
        video_id = video_url.split("youtu.be/")[-1].split("?")[0]

    if not video_id:
        logger.warning("[analytics][youtube] Не удалось извлечь video_id из URL: %s", video_url)
        return None

    studio_url = f"https://studio.youtube.com/video/{video_id}/analytics/tab-overview/period-default"
    logger.info("[analytics][youtube] Собираем статистику: %s", studio_url)

    try:
        page.goto(studio_url, wait_until="domcontentloaded", timeout=30_000)
        time.sleep(random.uniform(3, 5))
    except Exception as exc:
        logger.warning("[analytics][youtube] Не удалось открыть страницу: %s", exc)
        return None

    stats: Dict = {}

    # Просмотры
    try:
        views_el = page.locator(
            "span.style-scope.ytcp-ve:has-text('views'), "
            "[class*='metric-value']:first-of-type, "
            "#primary-metric span"
        ).first
        views_text = views_el.inner_text(timeout=5_000).strip().replace(",", "").replace(" ", "")
        stats["views"] = _parse_number(views_text)
    except Exception:
        stats["views"] = None

    # Лайки — YouTube Studio не всегда показывает напрямую,
    # пробуем через engagement section
    try:
        likes_el = page.locator(
            "[aria-label*='ike'], [class*='likes'] span, "
            "ytd-sentiment-bar-renderer span"
        ).first
        likes_text = likes_el.inner_text(timeout=3_000).strip()
        stats["likes"] = _parse_number(likes_text)
    except Exception:
        stats["likes"] = None

    # Комментарии — из вкладки engagement или публичной страницы
    try:
        comments_el = page.locator(
            "[class*='comment'] [class*='count'], "
            "#comments-count, ytcp-ve[class*='comment']"
        ).first
        comments_text = comments_el.inner_text(timeout=3_000).strip()
        stats["comments"] = _parse_number(comments_text)
    except Exception:
        stats["comments"] = None

    logger.info("[analytics][youtube] Собрано: %s", stats)
    return stats


def _collect_tiktok_stats(page: Page, video_url: str) -> Optional[Dict]:
    """Собирает статистику TikTok из публичной страницы видео."""
    if not video_url:
        return None

    logger.info("[analytics][tiktok] Собираем статистику: %s", video_url)
    try:
        page.goto(video_url, wait_until="domcontentloaded", timeout=30_000)
        time.sleep(random.uniform(3, 5))
    except Exception as exc:
        logger.warning("[analytics][tiktok] Не удалось открыть страницу: %s", exc)
        return None

    stats: Dict = {}

    try:
        views_el = page.locator(
            "[data-e2e='video-views'], strong[data-e2e='browse-video-play-count'], "
            "[class*='video-count']"
        ).first
        stats["views"] = _parse_number(views_el.inner_text(timeout=5_000))
    except Exception:
        stats["views"] = None

    try:
        likes_el = page.locator(
            "[data-e2e='like-count'], strong[data-e2e='browse-like-count']"
        ).first
        stats["likes"] = _parse_number(likes_el.inner_text(timeout=3_000))
    except Exception:
        stats["likes"] = None

    try:
        comments_el = page.locator(
            "[data-e2e='comment-count'], strong[data-e2e='browse-comment-count']"
        ).first
        stats["comments"] = _parse_number(comments_el.inner_text(timeout=3_000))
    except Exception:
        stats["comments"] = None

    logger.info("[analytics][tiktok] Собрано: %s", stats)
    return stats


def _collect_instagram_stats(page: Page, video_url: str) -> Optional[Dict]:
    """Собирает статистику Instagram Reels из публичной страницы поста."""
    if not video_url:
        return None

    logger.info("[analytics][instagram] Собираем статистику: %s", video_url)
    # Мобильный viewport для корректного отображения Instagram
    page.set_viewport_size({"width": 390, "height": 844})

    try:
        page.goto(video_url, wait_until="domcontentloaded", timeout=30_000)
        time.sleep(random.uniform(3, 5))
    except Exception as exc:
        logger.warning("[analytics][instagram] Не удалось открыть страницу: %s", exc)
        return None

    stats: Dict = {}

    try:
        views_el = page.locator(
            "span[class*='view'], [aria-label*='view'], "
            "span:has-text('views')"
        ).first
        stats["views"] = _parse_number(views_el.inner_text(timeout=5_000))
    except Exception:
        stats["views"] = None

    try:
        likes_el = page.locator(
            "section span[class*='like'], "
            "a[href*='liked_by'] span, "
            "[aria-label*='like']"
        ).first
        stats["likes"] = _parse_number(likes_el.inner_text(timeout=3_000))
    except Exception:
        stats["likes"] = None

    try:
        comments_el = page.locator(
            "a[href*='/comments/'] span, "
            "[aria-label*='comment'] span"
        ).first
        stats["comments"] = _parse_number(comments_el.inner_text(timeout=3_000))
    except Exception:
        stats["comments"] = None

    logger.info("[analytics][instagram] Собрано: %s", stats)
    return stats


_PLATFORM_COLLECTORS = {
    "youtube":   _collect_youtube_stats,
    "tiktok":    _collect_tiktok_stats,
    "instagram": _collect_instagram_stats,
}


def _parse_number(text: str) -> Optional[int]:
    """
    Парсит числа вида '1.5K', '2.3M', '150', '1,500'.
    Возвращает None если не удалось распознать.
    """
    if not text:
        return None
    text = text.strip().replace(",", "").replace(" ", "").upper()
    try:
        if text.endswith("K"):
            return int(float(text[:-1]) * 1_000)
        if text.endswith("M"):
            return int(float(text[:-1]) * 1_000_000)
        if text.endswith("B"):
            return int(float(text[:-1]) * 1_000_000_000)
        return int(float(text))
    except (ValueError, TypeError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Поиск аккаунта для платформы
# ─────────────────────────────────────────────────────────────────────────────

def _find_account_for_platform(platform: str) -> Optional[Dict]:
    """
    Возвращает первый аккаунт, у которого есть данная платформа.
    Используется для открытия браузера при сборе аналитики.
    """
    accounts = utils.get_all_accounts()
    for acc in accounts:
        if platform in acc.get("platforms", []):
            return acc
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Основная точка входа
# ─────────────────────────────────────────────────────────────────────────────

def collect_pending_analytics(dry_run: bool = False) -> int:
    """
    Собирает аналитику для всех видео, у которых подошло время сбора.

    Возвращает количество успешно обработанных записей.
    Результаты записываются в data/analytics.json.
    """
    pending = get_pending_collection()

    if not pending:
        logger.info("[analytics] Нет записей для сбора аналитики.")
        return 0

    logger.info("[analytics] Записей к обработке: %d", len(pending))

    if dry_run:
        for item in pending:
            logger.info("[dry_run][analytics] %s / %s — %s", item["stem"], item["platform"], item["url"])
        return len(pending)

    # Группируем по платформе — открываем один браузер на платформу
    by_platform: Dict[str, List[Dict]] = {}
    for item in pending:
        by_platform.setdefault(item["platform"], []).append(item)

    data    = _load_analytics()
    success = 0

    for platform, items in by_platform.items():
        collector_fn = _PLATFORM_COLLECTORS.get(platform)
        if not collector_fn:
            logger.warning("[analytics] Нет коллектора для платформы: %s", platform)
            continue

        account = _find_account_for_platform(platform)
        if not account:
            logger.warning("[analytics][%s] Нет аккаунта — пропуск.", platform)
            continue

        profile_dir = account["dir"] / "browser_profile"
        try:
            pw, context = launch_browser(account["config"], profile_dir)
        except RuntimeError as exc:
            logger.error("[analytics][%s] Прокси недоступен: %s", platform, exc)
            continue

        try:
            for item in items:
                stem = item["stem"]
                url  = item["url"]

                if not url:
                    logger.warning(
                        "[analytics][%s] URL не указан для %s — пропуск.", platform, stem
                    )
                    continue

                page = context.new_page()
                try:
                    stats = collector_fn(page, url)
                finally:
                    try:
                        page.close()
                    except Exception:
                        pass

                if stats is None:
                    logger.warning("[analytics][%s] Не удалось собрать данные для %s", platform, stem)
                    continue

                # Сохраняем в analytics.json
                if stem in data and platform in data[stem].get("uploads", {}):
                    data[stem]["uploads"][platform].update({
                        "collected_at": datetime.now().isoformat(timespec="seconds"),
                        "views":    stats.get("views"),
                        "likes":    stats.get("likes"),
                        "comments": stats.get("comments"),
                    })
                    _save_analytics(data)
                    success += 1
                    logger.info(
                        "[analytics][%s] %s — 👁 %s | 👍 %s | 💬 %s",
                        platform, stem,
                        stats.get("views"), stats.get("likes"), stats.get("comments"),
                    )

                time.sleep(random.uniform(3, 7))

        finally:
            close_browser(pw, context)

    if success:
        _send_analytics_report(data, success)

    return success


def _send_analytics_report(data: Dict, collected: int) -> None:
    """Отправляет краткий отчёт об аналитике в Telegram."""
    lines = [f"📊 <b>Аналитика собрана:</b> {collected} записей\n"]

    # Топ-5 по просмотрам
    all_records = []
    for stem, entry in data.items():
        total_views = sum(
            u.get("views") or 0
            for u in entry.get("uploads", {}).values()
            if u.get("views") is not None
        )
        if total_views > 0:
            all_records.append((total_views, entry.get("title") or stem, entry.get("tags", [])))

    all_records.sort(reverse=True)

    if all_records:
        lines.append("🏆 <b>Топ по просмотрам:</b>")
        for views, title, tags in all_records[:5]:
            tag_str = ", ".join(f"#{t}" for t in tags[:3])
            lines.append(f"  • <b>{title[:40]}</b> — {views:,} 👁  {tag_str}")

    send_telegram("\n".join(lines))
