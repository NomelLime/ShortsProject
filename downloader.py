"""
downloader.py — Этап «Поиск трендовых видео»

Совмещает два подхода:
  А) Playwright-браузер — имитирует живого пользователя, собирает URL с реальных страниц.
  Б) yt-dlp — быстрый API-поиск, собирает основной массив ссылок.

Дополнительно: AI-расширение ключевых слов через Ollama перед поиском.
"""

from __future__ import annotations

import random
import re
import time
from urllib.parse import quote_plus

from yt_dlp import YoutubeDL

from pipeline import config as cfg
from pipeline import utils

log = utils.get_logger("downloader")


# ─────────────────────────────────────────────────────────────────────────────
# AI-расширение ключевых слов
# ─────────────────────────────────────────────────────────────────────────────

def _expand_keywords_with_ai(keywords: list[str]) -> list[str]:
    """
    Отправляет ключевые слова в Ollama и просит сгенерировать
    расширенный список поисковых запросов для вирусных шортсов.
    Возвращает исходные + новые слова (без дубликатов).
    """
    if not cfg.AI_KEYWORD_EXPANSION:
        return keywords

    try:
        import ollama
        sample = keywords[:10]  # не перегружаем промпт
        prompt = (
            f"Given these video search keywords: {', '.join(sample)}\n"
            f"Generate {cfg.AI_KEYWORD_EXPANSION_COUNT} related trending search queries "
            f"for EACH keyword that would find viral short-form videos on YouTube, TikTok, Instagram.\n"
            f"Rules:\n"
            f"- Each query must be concise (2-5 words)\n"
            f"- Focus on trending, viral, emotional hooks\n"
            f"- Return ONLY queries, one per line, no numbering or explanation\n"
        )
        response = ollama.generate(
            model=cfg.OLLAMA_MODEL,
            prompt=prompt,
            options={"num_predict": 300},
        )
        new_lines = [
            line.strip() for line in response["response"].splitlines()
            if line.strip() and len(line.strip()) > 2
        ]
        combined = list(dict.fromkeys(keywords + new_lines))  # сохраняем порядок, убираем дубли
        log.info(
            "AI-расширение: %d исходных → %d запросов (+%d новых)",
            len(keywords), len(combined), len(combined) - len(keywords),
        )
        return combined
    except Exception as exc:
        log.warning("AI-расширение ключевых слов не удалось: %s — используем исходные", exc)
        return keywords


# ─────────────────────────────────────────────────────────────────────────────
# yt-dlp поиск (Вариант Б — быстрый)
# ─────────────────────────────────────────────────────────────────────────────

def _search_ydl_opts(proxy: str | None) -> dict:
    opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": "in_playlist",
        "skip_download": True,
        "ignoreerrors": True,
        "socket_timeout": cfg.SOCKET_TIMEOUT,
        "retries": 3,
        "noplaylist": False,
    }
    if proxy:
        opts["proxy"] = proxy
    return opts


def _extract_url(entry: dict) -> str | None:
    raw = entry.get("url") or entry.get("webpage_url") or entry.get("id")
    if not raw:
        return None
    if not raw.startswith("http"):
        return f"https://www.youtube.com/watch?v={raw}"
    return raw


def _passes_filters(entry: dict) -> bool:
    duration = entry.get("duration") or 0
    views    = entry.get("view_count") or 0
    if duration and duration > cfg.MAX_DURATION_SEC:
        return False
    if views and views < cfg.MIN_VIEWS:
        return False
    return True


def _run_search_query(query: str, ydl_opts: dict) -> list[str]:
    try:
        with YoutubeDL(ydl_opts) as ydl:
            result = ydl.extract_info(query, download=False)
    except Exception as exc:
        log.error("Ошибка запроса «%s»: %s", query, exc)
        return []

    if not result:
        log.warning("Нет результатов: %s", query)
        return []

    urls = []
    for entry in result.get("entries") or []:
        if not entry or not _passes_filters(entry):
            continue
        if url := _extract_url(entry):
            urls.append(url)
    return urls


def _search_ytdlp(keywords: list[str], proxy: str | None) -> list[str]:
    """Быстрый массовый поиск через yt-dlp (Вариант Б)."""
    ydl_opts  = _search_ydl_opts(proxy)
    found     = set()
    all_queries: list[tuple[str, str]] = []

    for platform in cfg.PLATFORMS:
        for keyword in keywords:
            for query in platform.build_queries(keyword, cfg.MAX_RESULTS_PER_QUERY):
                all_queries.append((platform.name, query))

    total = len(all_queries)
    log.info("[yt-dlp] Всего запросов: %d", total)

    for idx, (platform_name, query) in enumerate(all_queries, start=1):
        log.info("[yt-dlp] [%d/%d] %-20s | %s", idx, total, platform_name, query)
        new_urls = _run_search_query(query, ydl_opts)
        before   = len(found)
        found.update(new_urls)
        log.info("  → +%d новых | итого: %d", len(found) - before, len(found))

        pause = random.uniform(cfg.SLEEP_MIN, cfg.SLEEP_MAX)
        log.debug("Пауза %.1f с...", pause)
        time.sleep(pause)

    return list(found)


# ─────────────────────────────────────────────────────────────────────────────
# Браузерный поиск (Вариант А — симуляция живого человека)
# ─────────────────────────────────────────────────────────────────────────────

def _browser_search_platform(
    page,
    platform: str,
    keyword: str,
) -> list[str]:
    """
    Ищет видео на одной платформе через реальный браузер.
    Имитирует живого пользователя: ручной ввод запроса, скроллинг, паузы.
    Возвращает список URL найденных видео.
    """
    search_url_tpl = cfg.BROWSER_SEARCH_URLS.get(platform)
    if not search_url_tpl:
        return []

    query_encoded = quote_plus(keyword)
    url = search_url_tpl.format(query=query_encoded)
    urls_found: list[str] = []

    try:
        log.info("[browser][%s] Открываю страницу поиска: «%s»", platform, keyword)
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        _human_pause(2, 5)

        # Медленно прокручиваем страницу несколько раз — как живой человек
        scroll_rounds = random.randint(3, 6)
        for _ in range(scroll_rounds):
            delta = random.randint(400, 900)
            page.mouse.wheel(0, delta)
            _human_pause(0.8, 2.5)

        # Собираем ссылки в зависимости от платформы
        if platform == "youtube":
            # Селектор для карточек YouTube Shorts в результатах поиска
            selectors = [
                "a#video-title",
                "a.ytd-video-renderer",
                "ytd-video-renderer a[href*='/shorts/']",
                "ytd-video-renderer a[href*='watch?v=']",
            ]
            for sel in selectors:
                for el in page.locator(sel).all()[:30]:
                    try:
                        href = el.get_attribute("href")
                        if href and ("watch?v=" in href or "/shorts/" in href):
                            if not href.startswith("http"):
                                href = "https://www.youtube.com" + href
                            urls_found.append(href)
                    except Exception:
                        pass

        elif platform == "tiktok":
            selectors = [
                "a[href*='/@'][href*='/video/']",
                "div[data-e2e='search-card-item'] a",
            ]
            for sel in selectors:
                for el in page.locator(sel).all()[:30]:
                    try:
                        href = el.get_attribute("href")
                        if href and "/video/" in href:
                            if not href.startswith("http"):
                                href = "https://www.tiktok.com" + href
                            urls_found.append(href)
                    except Exception:
                        pass

        # Имитируем «просмотр» нескольких результатов — кратко зависаем
        watch_time = random.uniform(3, 8)
        log.debug("[browser][%s] «Изучаем» результаты %.1f сек", platform, watch_time)
        time.sleep(watch_time)

        # Случайно ставим лайк одному видео (с небольшой вероятностью)
        if random.random() < 0.25:
            _try_like_first_result(page, platform)

    except Exception as exc:
        log.warning("[browser][%s] Поиск «%s» не удался: %s", platform, keyword, exc)

    unique = list(dict.fromkeys(urls_found))
    log.info("[browser][%s] «%s» → %d URL", platform, keyword, len(unique))
    return unique


def _try_like_first_result(page, platform: str) -> None:
    """Пытается поставить лайк первому видео в результатах поиска."""
    selectors = {
        "youtube": "ytd-video-renderer #top-level-buttons button[aria-label*='like']",
        "tiktok":  "[data-e2e='like-icon']",
    }
    sel = selectors.get(platform)
    if not sel:
        return
    try:
        btn = page.locator(sel).first
        if btn.is_visible(timeout=2_000):
            btn.click()
            log.debug("[browser][%s] Лайк поставлен", platform)
            _human_pause(1, 2)
    except Exception:
        pass


def _human_pause(lo: float, hi: float) -> None:
    """Пауза с гауссовым шумом для имитации живого пользователя."""
    delay = max(0.3, random.gauss(random.uniform(lo, hi), 0.3))
    time.sleep(delay)


def _search_browser(keywords: list[str], proxy: str | None) -> list[str]:
    """
    Браузерный поиск (Вариант А).
    Открывает Playwright, проходит по BROWSER_SEARCH_KEYWORDS_MAX ключевым словам
    на YouTube и TikTok в реальном браузере.
    """
    if not cfg.BROWSER_SEARCH_ENABLED:
        return []

    try:
        from rebrowser_playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth
    except ImportError as e:
        log.warning("[browser] rebrowser-playwright не установлен: %s", e)
        return []

    # Берём подмножество keywords для браузерного поиска
    kws = keywords[:cfg.BROWSER_SEARCH_KEYWORDS_MAX]
    all_browser_urls: list[str] = []

    launch_opts: dict = {
        "headless": cfg.BROWSER_SEARCH_HEADLESS,
        "args": ["--disable-blink-features=AutomationControlled", "--no-sandbox"],
    }
    if proxy:
        launch_opts["proxy"] = {"server": proxy}

    stealth = Stealth()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(**launch_opts)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="America/New_York",
            viewport={"width": 1366, "height": 768},
        )

        # Применяем stealth ко всем новым страницам
        context.on("page", lambda p: stealth.apply_stealth_sync(p))

        page = context.new_page()
        stealth.apply_stealth_sync(page)

        for keyword in kws:
            for platform in ("youtube", "tiktok"):
                found = _browser_search_platform(page, platform, keyword)
                all_browser_urls.extend(found)

                # Пауза между платформами — как живой человек
                pause = random.uniform(cfg.SLEEP_MIN, cfg.SLEEP_MAX)
                log.debug("[browser] Пауза между платформами %.1f с", pause)
                time.sleep(pause)

            # Пауза между ключевыми словами
            pause = random.uniform(cfg.SLEEP_MIN * 2, cfg.SLEEP_MAX * 2)
            log.info("[browser] Пауза между keywords %.1f с", pause)
            time.sleep(pause)

        context.close()
        browser.close()

    unique = list(dict.fromkeys(all_browser_urls))
    log.info("[browser] Итого собрано: %d уникальных URL", len(unique))
    return unique


# ─────────────────────────────────────────────────────────────────────────────
# Сохранение URL
# ─────────────────────────────────────────────────────────────────────────────

def save_urls(urls: list[str]) -> int:
    return utils.merge_and_save_urls(urls, cfg.URLS_FILE)


# ─────────────────────────────────────────────────────────────────────────────
# Главная функция
# ─────────────────────────────────────────────────────────────────────────────

def search_and_save() -> None:
    """
    Полный цикл поиска трендовых видео:
      1. Загружаем ключевые слова
      2. Расширяем их через AI (Ollama)
      3. Быстрый поиск через yt-dlp (Вариант Б)
      4. Браузерный поиск с симуляцией живого пользователя (Вариант А)
      5. Объединяем и сохраняем все URL
    """
    log.info("═══ Поиск трендовых видео ═══")

    keywords = utils.load_keywords()
    if not keywords:
        log.error("Список ключевых слов пуст.")
        return

    proxy = utils.load_proxy()

    # 1. AI-расширение ключевых слов
    log.info("Шаг 1: AI-расширение ключевых слов (%d исходных)", len(keywords))
    expanded_keywords = _expand_keywords_with_ai(keywords)

    # 2. Быстрый yt-dlp поиск
    log.info("Шаг 2: Быстрый поиск через yt-dlp")
    ytdlp_urls = _search_ytdlp(expanded_keywords, proxy)
    log.info("[yt-dlp] Найдено URL: %d", len(ytdlp_urls))

    # 3. Браузерный поиск (на исходных keywords, не расширенных — меньше, но реалистичнее)
    log.info("Шаг 3: Браузерный поиск (симуляция живого человека)")
    browser_urls = _search_browser(keywords, proxy)
    log.info("[browser] Найдено URL: %d", len(browser_urls))

    # 4. Объединяем все URL
    all_urls = list(dict.fromkeys(ytdlp_urls + browser_urls))
    log.info("Всего уникальных URL: %d", len(all_urls))

    if not all_urls:
        log.warning("URL не найдены.")
        return

    added = save_urls(all_urls)
    log.info("═══ Поиск завершён: найдено %d, добавлено %d ═══", len(all_urls), added)


if __name__ == "__main__":
    search_and_save()
