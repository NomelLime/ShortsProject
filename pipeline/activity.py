# activity.py
# Симуляция человекоподобного поведения перед загрузкой видео

import time
import random
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from rebrowser_playwright.sync_api import BrowserContext, Page

from pipeline.config import (
    ACTIVITY_DURATION_MIN_SEC, ACTIVITY_DURATION_MAX_SEC,
    WATCH_TIME_MIN_SEC, WATCH_TIME_MAX_SEC,
    CLICK_DELAY_MIN_SEC, CLICK_DELAY_MAX_SEC,
    PLATFORM_URLS,
)
from pipeline.utils import human_sleep
from pipeline.notifications import check_and_handle_captcha

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# Вспомогательные функции
# ──────────────────────────────────────────────────────────────

def _random_scroll(page: Page, scrolls: int = None) -> None:
    """Случайная прокрутка страницы вниз/вверх."""
    count = scrolls or random.randint(3, 8)
    for _ in range(count):
        direction = random.choice([1, 1, 1, -1])       # чаще вниз
        delta = random.randint(300, 900) * direction
        page.mouse.wheel(0, delta)
        time.sleep(random.uniform(0.4, 1.5))


def _try_like_video(page: Page, platform: str) -> None:
    """Пытается поставить лайк текущему видео. Молча игнорирует ошибки."""
    selectors = {
        "youtube":   "ytd-toggle-button-renderer#top-level-buttons-computed "
                     "button[aria-label*='like']",
        "tiktok":    "[data-e2e='like-icon']",
        "instagram": "svg[aria-label='Like']",
    }
    sel = selectors.get(platform)
    if not sel:
        return
    try:
        btn = page.locator(sel).first
        if btn.is_visible(timeout=3_000):
            btn.click()
            logger.debug(f"[{platform}] Лайк поставлен")
            human_sleep(1, 3)
    except Exception:
        pass


def _build_search_keywords(metadata: dict) -> list[str]:
    """
    Формирует список поисковых запросов из метаданных видео.
    Использует теги целиком; если тегов нет — биграммы из заголовка.
    """
    tags = metadata.get("tags", [])
    if tags:
        return tags
    words = metadata.get("title", "").split()
    bigrams = [f"{words[i]} {words[i+1]}" for i in range(len(words) - 1)]
    return bigrams if bigrams else words


def _perform_search(page: Page, platform: str, keywords: list[str]) -> None:
    """Выполняет один поисковый запрос на основе ключевых слов из метаданных."""
    if not keywords:
        return
    query = random.choice(keywords)
    logger.info(f"[{platform}] Поиск по запросу: «{query}»")

    search_selectors = {
        "youtube":   "input#search",
        "tiktok":    "input[type='search']",
        "instagram": "input[placeholder='Search']",
    }
    submit_selectors = {
        "youtube":   "button#search-icon-legacy",
        "tiktok":    None,
        "instagram": None,
    }

    sel = search_selectors.get(platform)
    if not sel:
        return

    try:
        page.locator(sel).first.click(timeout=5_000)
        human_sleep(0.5, 1.5)
        for char in query:
            page.keyboard.type(char)
            time.sleep(random.uniform(0.05, 0.18))
        human_sleep(0.8, 2.0)
        sub = submit_selectors.get(platform)
        if sub:
            page.locator(sub).click()
        else:
            page.keyboard.press("Enter")
        human_sleep(2, 4)
        _random_scroll(page, scrolls=random.randint(2, 5))
    except Exception as e:
        logger.warning(f"[{platform}] Поиск не выполнен: {e}")


# ──────────────────────────────────────────────────────────────
# Основной модуль активности
# ──────────────────────────────────────────────────────────────

def run_activity(
    context: BrowserContext,
    platform: str,
    metadata: dict,
    *,
    acc_dir: Optional[Path] = None,
    acc_cfg: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Главная функция симуляции активности.
    Открывает страницу платформы, скроллит ленту, смотрит видео,
    ставит лайки и делает поиск. Работает ACTIVITY_DURATION_*_SEC секунд.
    При acc_dir/acc_cfg и прогреве заливки — сокращённая длительность (как VL).
    """
    urls     = PLATFORM_URLS.get(platform, {})
    feed_url = (
        urls.get("shorts")
        or urls.get("feed")
        or urls.get("reels")
        or urls.get("home")
    )

    duration = random.randint(ACTIVITY_DURATION_MIN_SEC, ACTIVITY_DURATION_MAX_SEC)
    if acc_dir is not None and acc_cfg is not None:
        try:
            from pipeline import config as _cfg
            from pipeline.upload_warmup import is_upload_warmup_active

            warm, _ = is_upload_warmup_active(acc_dir, platform, acc_cfg)
            wmult = float(getattr(_cfg, "ACTIVITY_WARMUP_DURATION_MULT", 1.0) or 1.0)
            if warm and 0 < wmult < 1.0:
                lo = max(60, int(ACTIVITY_DURATION_MIN_SEC * wmult))
                hi = max(lo + 30, int(ACTIVITY_DURATION_MAX_SEC * wmult))
                duration = random.randint(lo, hi)
                logger.info(
                    "[%s] Прогрев — сокращённая активность (~%.0f%% длительности)",
                    platform,
                    wmult * 100,
                )
        except Exception:
            pass
    logger.info(f"[{platform}] Начало симуляции активности на {duration // 60} мин.")

    page = context.new_page()
    # Stealth уже применяется через context.on("page", ...) в browser.py,
    # явный вызов здесь избыточен.

    try:
        page.goto(feed_url, wait_until="domcontentloaded", timeout=30_000)
        human_sleep(2, 5)

        deadline    = time.time() + duration
        search_done = False
        like_budget = random.randint(1, 4)
        keywords    = _build_search_keywords(metadata)

        while time.time() < deadline:
            check_and_handle_captcha(page, platform)

            _random_scroll(page)

            watch_time = random.randint(WATCH_TIME_MIN_SEC, WATCH_TIME_MAX_SEC)
            logger.debug(f"[{platform}] Просмотр видео ~{watch_time}с")
            time.sleep(watch_time)

            if like_budget > 0 and random.random() < 0.35:
                _try_like_video(page, platform)
                like_budget -= 1

            if not search_done and random.random() < 0.4:
                _perform_search(page, platform, keywords)
                search_done = True
                human_sleep(3, 6)
                page.goto(feed_url, wait_until="domcontentloaded", timeout=30_000)
                human_sleep(1, 3)

            human_sleep(CLICK_DELAY_MIN_SEC, CLICK_DELAY_MAX_SEC)

    except Exception as e:
        logger.error(f"[{platform}] Ошибка во время симуляции: {e}")
    finally:
        page.close()

    logger.info(f"[{platform}] Симуляция активности завершена.")
