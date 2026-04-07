"""
downloader.py — Этап «Поиск трендовых видео»

Совмещает два подхода:
  А) Playwright — persistent-профиль залогиненного аккаунта (SHORTS_PIPELINE_ACCOUNT / ротация SCOUT).
  Б) yt-dlp — быстрый поиск; cookies из того же аккаунта (get_ytdlp_cookie_options).

Задайте SHORTS_PIPELINE_ACCOUNT или YTDLP_COOKIES_ACCOUNT, либо PIPELINE_ACCOUNT_ROTATION=1
(пул: все accounts/* или PIPELINE_ACCOUNT_POOL) — см. pipeline_account_rotation.py.
Дополнительно: AI-расширение ключевых слов через Ollama перед поиском.
"""

from __future__ import annotations

import copy
import os
import random
import re
import time
import threading
from pathlib import Path
from urllib.parse import quote_plus

from yt_dlp import YoutubeDL

from pipeline import config as cfg
from pipeline import utils
from pipeline.humanize import HumanizeRisk, human_pause, human_scroll_step

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
    opts.update(cfg.get_ytdlp_cookie_options())
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
        msg = str(exc).lower()
        if "failed to load cookies" in msg:
            log.warning("Cookies не загрузились для «%s», повторяем без cookies", query)
            retry_opts = dict(ydl_opts)
            retry_opts.pop("cookiefile", None)
            retry_opts.pop("cookiesfrombrowser", None)
            try:
                with YoutubeDL(retry_opts) as ydl:
                    result = ydl.extract_info(query, download=False)
            except Exception as retry_exc:
                log.error("Ошибка запроса «%s» (retry без cookies): %s", query, retry_exc)
                return []
        else:
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


def _run_search_query_with_timeout(query: str, ydl_opts: dict, timeout_sec: int) -> list[str]:
    result_holder: dict[str, list[str]] = {"urls": []}
    err_holder: dict[str, Exception | None] = {"err": None}

    def _worker() -> None:
        try:
            result_holder["urls"] = _run_search_query(query, ydl_opts)
        except Exception as exc:
            err_holder["err"] = exc

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    t.join(timeout=max(5, timeout_sec))

    if t.is_alive():
        log.warning("[yt-dlp] Таймаут query (%ds), пропускаем: %s", timeout_sec, query)
        return []
    if err_holder["err"] is not None:
        log.warning("[yt-dlp] Query crashed, пропускаем «%s»: %s", query, err_holder["err"])
        return []
    return result_holder["urls"]


def _search_ytdlp(keywords: list[str], proxy: str | None) -> list[str]:
    """Быстрый массовый поиск через yt-dlp (Вариант Б)."""
    if not cfg.YTDLP_SEARCH_ENABLED:
        log.info("[yt-dlp] Поиск отключён (YTDLP_SEARCH_ENABLED=0)")
        return []

    ydl_opts  = _search_ydl_opts(proxy)
    found     = set()
    all_queries: list[tuple[str, str]] = []

    for platform in cfg.PLATFORMS:
        for keyword in keywords:
            for query in platform.build_queries(keyword, cfg.MAX_RESULTS_PER_QUERY):
                all_queries.append((platform.name, query))

    # Автоподдержка новых платформ из конфига через шаблоны yt-dlp.
    # Не дублируем уже добавленные платформы из cfg.PLATFORMS.
    base_platform_keys = {"youtube", "tiktok"}
    for platform_key in (cfg.BROWSER_SEARCH_URLS or {}).keys():
        pk = str(platform_key).strip().lower()
        if not pk or pk in base_platform_keys:
            continue
        templates = (cfg.YTDLP_PLATFORM_QUERIES or {}).get(pk, ())
        for keyword in keywords:
            for tpl in templates:
                query = tpl.format(n=cfg.MAX_RESULTS_PER_QUERY, keyword=keyword)
                all_queries.append((pk, query))

    if cfg.YTDLP_MAX_QUERIES > 0:
        all_queries = all_queries[: cfg.YTDLP_MAX_QUERIES]

    total = len(all_queries)
    log.info("[yt-dlp] Всего запросов: %d", total)

    _acc = None
    _b = utils.get_pipeline_account_bundle()
    if _b:
        _acc = _b["config"]

    for idx, (platform_name, query) in enumerate(all_queries, start=1):
        log.info("[yt-dlp] [%d/%d] %-20s | %s", idx, total, platform_name, query)
        new_urls = _run_search_query_with_timeout(query, ydl_opts, cfg.YTDLP_QUERY_TIMEOUT_SEC)
        before   = len(found)
        found.update(new_urls)
        log.info("  → +%d новых | итого: %d", len(found) - before, len(found))

        human_pause(
            cfg.SLEEP_MIN,
            cfg.SLEEP_MAX,
            account_cfg=_acc,
            agent="DOWNLOADER",
            context="ytdlp_between_queries",
            risk=HumanizeRisk.MEDIUM,
        )

    return list(found)


# ─────────────────────────────────────────────────────────────────────────────
# Браузерный поиск (Вариант А — симуляция живого человека)
# ─────────────────────────────────────────────────────────────────────────────

def _browser_search_platform(
    page,
    platform: str,
    keyword: str,
    account_cfg: dict | None = None,
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
        _enforce_search_query(page, platform, keyword)
        human_pause(
            2,
            5,
            account_cfg=account_cfg,
            agent="DOWNLOADER",
            context="search_open",
            risk=HumanizeRisk.LOW,
        )

        scroll_rounds = random.randint(3, 6)
        for _ in range(scroll_rounds):
            human_scroll_step(
                page,
                account_cfg=account_cfg,
                agent="DOWNLOADER",
                risk=HumanizeRisk.LOW,
            )

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

        elif platform == "instagram":
            selectors = [
                "a[href*='/reel/']",
                "a[href*='/p/']",
            ]
            for sel in selectors:
                for el in page.locator(sel).all()[:30]:
                    try:
                        href = el.get_attribute("href")
                        if href and ("/reel/" in href or "/p/" in href):
                            if not href.startswith("http"):
                                href = "https://www.instagram.com" + href
                            urls_found.append(href)
                    except Exception:
                        pass

        elif platform == "vk":
            selectors = [
                "a[href*='/video-']",
                "a[href*='/clip-']",
                "a[href*='z=video']",
                "[data-testid*='video'] a[href]",
            ]
            for sel in selectors:
                for el in page.locator(sel).all()[:30]:
                    try:
                        href = el.get_attribute("href")
                        if href and ("/video-" in href or "/clip-" in href or "z=video" in href):
                            if not href.startswith("http"):
                                href = "https://vkvideo.ru" + href
                            urls_found.append(href)
                    except Exception:
                        pass

        elif platform == "rutube":
            selectors = [
                "a[href*='/video/']",
                "a[href*='/shorts/']",
            ]
            for sel in selectors:
                for el in page.locator(sel).all()[:30]:
                    try:
                        href = el.get_attribute("href")
                        if href and ("/video/" in href or "/shorts/" in href):
                            if not href.startswith("http"):
                                href = "https://rutube.ru" + href
                            urls_found.append(href)
                    except Exception:
                        pass

        elif platform == "ok":
            selectors = [
                "a[href*='/video/']",
                "a[href*='/video/c']",
                "a[href*='st.mvId=']",
                "a[data-l*='video']",
                "a[data-video-id]",
                "[data-module='VideoCard'] a[href]",
            ]
            for sel in selectors:
                for el in page.locator(sel).all()[:30]:
                    try:
                        href = el.get_attribute("href")
                        if href and ("video" in href or "st.mvId=" in href or "movieId" in href):
                            if not href.startswith("http"):
                                href = "https://ok.ru" + href
                            urls_found.append(href)
                    except Exception:
                        pass

        # Имитируем «просмотр» нескольких результатов — кратко зависаем
        watch_time = random.uniform(3, 8)
        log.debug("[browser][%s] «Изучаем» результаты %.1f сек", platform, watch_time)
        time.sleep(watch_time)

        if random.random() < 0.25:
            _try_like_first_result(page, platform, account_cfg=account_cfg)

    except Exception as exc:
        log.warning("[browser][%s] Поиск «%s» не удался: %s", platform, keyword, exc)

    unique = list(dict.fromkeys(urls_found))
    log.info("[browser][%s] «%s» → %d URL", platform, keyword, len(unique))
    return unique


def _enforce_search_query(page, platform: str, keyword: str) -> None:
    """
    Для VK/OK URL-параметр иногда игнорируется и страница показывает рекомендации.
    Принудительно вводим keyword в поисковую строку и жмём Enter.
    """
    if platform not in ("vk", "ok"):
        return
    selectors = {
        "vk": [
            "input[name='q']",
            "input[placeholder*='Поиск']",
            "input[type='search']",
        ],
        "ok": [
            "input[name='st.query']",
            "input[placeholder*='Поиск видео']",
            "input[type='search']",
        ],
    }
    for sel in selectors.get(platform, []):
        try:
            inp = page.locator(sel).first
            if inp.is_visible(timeout=2_000):
                inp.click()
                inp.fill(keyword)
                inp.press("Enter")
                human_pause(1, 2, agent="DOWNLOADER", context=f"{platform}_query_submit")
                return
        except Exception:
            continue


def _try_like_first_result(
    page,
    platform: str,
    *,
    account_cfg: dict | None = None,
) -> None:
    """Пытается поставить лайк первому видео в результатах поиска."""
    selectors = {
        "youtube":   "ytd-video-renderer #top-level-buttons button[aria-label*='like']",
        "tiktok":    "[data-e2e='like-icon']",
        "instagram": "svg[aria-label='Like']",
    }
    sel = selectors.get(platform)
    if not sel:
        return
    try:
        btn = page.locator(sel).first
        if btn.is_visible(timeout=2_000):
            btn.click()
            log.debug("[browser][%s] Лайк поставлен", platform)
            human_pause(
                1,
                2,
                account_cfg=account_cfg,
                agent="DOWNLOADER",
                context="search_like",
                risk=HumanizeRisk.MEDIUM,
            )
    except Exception:
        pass


def _search_browser(keywords: list[str], _proxy: str | None) -> list[str]:
    """
    Браузерный поиск (Вариант А).
    Persistent-профиль залогиненного аккаунта: SHORTS_PIPELINE_ACCOUNT или
    YTDLP_COOKIES_ACCOUNT (имя папки в accounts/). Прокси берётся из конфига аккаунта
    (mobileproxy / resolve_working_proxy), а не из аргумента _proxy.
    """
    if not cfg.BROWSER_SEARCH_ENABLED:
        return []

    bundle = utils.get_pipeline_account_bundle()
    allow_no_account = os.getenv("BROWSER_SEARCH_NO_ACCOUNT", "").strip().lower() in (
        "1", "true", "yes", "on",
    )

    from pipeline.browser import close_browser, launch_browser

    stealth_apply = None
    try:
        from playwright_stealth import Stealth

        stealth_apply = Stealth().apply_stealth_sync
    except ImportError as e:
        log.warning("[browser] playwright-stealth не установлен: %s", e)

    acc_cfg = copy.deepcopy(bundle["config"]) if bundle else {}
    _acc_for_search = acc_cfg if bundle else None
    profile_dir = (bundle["dir"] / "browser_profile") if bundle else None
    plats = acc_cfg.get("platforms", ["youtube"]) if bundle else ["youtube"]
    if isinstance(plats, str):
        plats = [plats]
    plat0 = (plats[0] if plats else "youtube").lower()

    kws = keywords[:cfg.BROWSER_SEARCH_KEYWORDS_MAX]
    all_browser_urls: list[str] = []
    configured_platforms = [p.lower() for p in (cfg.BROWSER_SEARCH_URLS or {}).keys()]
    default_platforms = ["youtube", "tiktok"]
    search_platforms = list(dict.fromkeys(default_platforms + configured_platforms))

    try:
        if bundle:
            pw, context = launch_browser(acc_cfg, profile_dir, platform=plat0)
        elif allow_no_account:
            use_playwright_compat = os.getenv("BROWSER_COMPAT_PLAYWRIGHT", "").strip().lower() in (
                "1", "true", "yes", "on",
            )
            engine = os.getenv("BROWSER_NO_ACCOUNT_ENGINE", "chromium").strip().lower()

            if use_playwright_compat:
                try:
                    from playwright.sync_api import sync_playwright as compat_sync_playwright

                    pw = compat_sync_playwright().start()
                    if engine == "firefox":
                        browser = pw.firefox.launch(headless=cfg.BROWSER_SEARCH_HEADLESS)
                    else:
                        browser = pw.chromium.launch(headless=cfg.BROWSER_SEARCH_HEADLESS)
                except Exception as compat_exc:
                    raise RuntimeError(
                        f"Compat playwright launch error: {compat_exc}"
                    ) from compat_exc
            else:
                from rebrowser_playwright.sync_api import sync_playwright

                pw = sync_playwright().start()
                try:
                    if engine == "firefox":
                        browser = pw.firefox.launch(headless=cfg.BROWSER_SEARCH_HEADLESS)
                    else:
                        browser = pw.chromium.launch(headless=cfg.BROWSER_SEARCH_HEADLESS)
                except Exception as exc:
                    if engine == "firefox":
                        log.warning(
                            "[browser] Firefox launch error (%s) — fallback на chromium",
                            exc,
                        )
                        browser = pw.chromium.launch(headless=cfg.BROWSER_SEARCH_HEADLESS)
                        engine = "chromium"
                    else:
                        raise
            context = browser.new_context()
            log.warning(
                "[browser] Запуск без accounts: temporary context без логина/cookies (engine=%s, compat=%s)",
                engine,
                "on" if use_playwright_compat else "off",
            )
        else:
            log.error(
                "[browser] Нет accounts. Для теста без логина включите BROWSER_SEARCH_NO_ACCOUNT=1"
            )
            return []
    except Exception as exc:
        log.error("[browser] Не удалось запустить браузер: %s", exc)
        return []

    try:
        page = context.new_page()
        if stealth_apply:
            stealth_apply(page)

        for keyword in kws:
            for platform in search_platforms:
                found = _browser_search_platform(
                    page, platform, keyword, account_cfg=_acc_for_search,
                )
                all_browser_urls.extend(found)

                human_pause(
                    cfg.SLEEP_MIN,
                    cfg.SLEEP_MAX,
                    account_cfg=_acc_for_search,
                    agent="DOWNLOADER",
                    context="browser_between_platforms",
                    risk=HumanizeRisk.LOW,
                )

            human_pause(
                cfg.SLEEP_MIN * 2,
                cfg.SLEEP_MAX * 2,
                account_cfg=_acc_for_search,
                agent="DOWNLOADER",
                context="browser_between_keywords",
                risk=HumanizeRisk.MEDIUM,
            )
    finally:
        close_browser(pw, context)

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

    pan = utils.resolve_pipeline_account_name()
    if pan:
        log.info(
            "Подготовка контента под аккаунт «%s» (cookies yt-dlp + браузерный поиск)",
            pan,
        )

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

    auto_download = (
        os.getenv("AUTO_DOWNLOAD_AFTER_SEARCH", "").strip().lower() in ("1", "true", "yes", "on")
    )
    if auto_download and added > 0:
        try:
            from pipeline import download as _download

            queued = len(utils.unique_lines(cfg.URLS_FILE))
            log.info("[post-check] urls.txt: %d URL в очереди", queued)
            log.info("[post-check] AUTO_DOWNLOAD_AFTER_SEARCH=1 → запускаю download.download_all()")
            _download.download_all()
        except Exception as exc:
            log.warning("[post-check] Автозапуск download не удался: %s", exc)


if __name__ == "__main__":
    search_and_save()
