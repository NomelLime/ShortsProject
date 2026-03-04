# REBROWSER UPGRADE 2026
"""
browser.py – Инициализация браузера с поддержкой stealth.
"""

import logging
from pathlib import Path
from typing import List
from rebrowser_playwright.sync_api import sync_playwright, BrowserContext, Playwright
from playwright_stealth import Stealth

from pipeline import config as cfg
from pipeline import utils

logger = logging.getLogger(__name__)


def _build_proxy_config(proxy: dict) -> dict | None:
    """Формирует словарь прокси для Playwright из конфига аккаунта."""
    if not proxy or not proxy.get("host"):
        return None
    proxy_cfg = {
        "server": f"http://{proxy['host']}:{proxy['port']}",
    }
    if proxy.get("username"):
        proxy_cfg["username"] = proxy["username"]
        proxy_cfg["password"] = proxy.get("password", "")
    return proxy_cfg


def resolve_working_proxy(account_cfg: dict) -> dict | None:
    """
    Возвращает первый работающий прокси для аккаунта.

    Порядок проверки:
      1. Основной прокси: account_cfg["proxy"]
      2. Резервные прокси: account_cfg["fallback_proxies"] (список)

    Если рабочий прокси найден — обновляет account_cfg["_active_proxy"]
    для использования в этой сессии. Возвращает dict прокси или None.

    Пример конфига аккаунта с резервными прокси:
      {
        "proxy": {"host": "1.2.3.4", "port": 8080},
        "fallback_proxies": [
          {"host": "5.6.7.8", "port": 8080, "username": "u", "password": "p"},
          {"host": "9.10.11.12", "port": 3128}
        ]
      }
    """
    candidates: list[dict] = []

    primary = account_cfg.get("proxy", {})
    if primary and primary.get("host"):
        candidates.append(primary)

    for fb in account_cfg.get("fallback_proxies", []):
        if fb and fb.get("host"):
            candidates.append(fb)

    if not candidates:
        return None  # прокси не настроен вообще

    for proxy in candidates:
        label = f"{proxy.get('host')}:{proxy.get('port')}"
        logger.debug("[proxy] Проверяем %s...", label)
        if utils.check_proxy_health(proxy):
            logger.info("[proxy] Рабочий прокси: %s", label)
            account_cfg["_active_proxy"] = proxy
            return proxy
        else:
            logger.warning("[proxy] Недоступен: %s — пробуем следующий...", label)

    logger.error("[proxy] Все прокси (%d) недоступны для аккаунта.", len(candidates))
    return None


def _is_profile_empty(profile_dir: Path) -> bool:
    """Проверяет, пустая ли папка профиля."""
    if not profile_dir.exists():
        return True
    return not any(profile_dir.glob("**/Cookies"))


# URL для проверки авторизации — открываем страницу и смотрим, не редиректит ли нас на логин
_SESSION_CHECK_URLS: dict[str, str] = {
    "youtube":   "https://studio.youtube.com",
    "tiktok":    "https://www.tiktok.com/upload",
    "instagram": "https://www.instagram.com/accounts/edit/",
}

_LOGIN_REDIRECT_MARKERS: dict[str, list[str]] = {
    "youtube":   ["accounts.google.com/signin", "accounts.google.com/ServiceLogin"],
    "tiktok":    ["tiktok.com/login", "/login?redirect"],
    "instagram": ["instagram.com/accounts/login", "/login/?next="],
}


def check_session_valid(context: BrowserContext, platforms: list[str]) -> dict[str, bool]:
    """
    Проверяет, залогинен ли браузер на каждой из платформ.

    Открывает служебную страницу (требующую авторизации) и проверяет,
    не произошёл ли редирект на страницу логина.

    Возвращает словарь {platform: is_logged_in}.
    """
    results: dict[str, bool] = {}
    page = context.new_page()

    for platform in platforms:
        check_url = _SESSION_CHECK_URLS.get(platform)
        if not check_url:
            results[platform] = True  # неизвестная платформа — не блокируем
            continue

        try:
            page.goto(check_url, wait_until="domcontentloaded", timeout=20_000)
            current_url = page.url
            markers = _LOGIN_REDIRECT_MARKERS.get(platform, [])
            redirected = any(marker in current_url for marker in markers)
            results[platform] = not redirected
            logger.debug(
                "[session][%s] URL после перехода: %s → %s",
                platform,
                current_url,
                "залогинен" if results[platform] else "НЕ залогинен",
            )
        except Exception as exc:
            logger.warning("[session][%s] Не удалось проверить сессию: %s", platform, exc)
            results[platform] = False  # при ошибке считаем не залогиненным

    try:
        page.close()
    except Exception:
        pass

    return results


def launch_browser(account_cfg: dict, profile_dir: Path) -> tuple[Playwright, BrowserContext]:
    """
    Запускает persistent context для аккаунта с применением stealth.

    Перед запуском перебирает прокси (основной + резервные) и использует
    первый работающий. Если ни один прокси не работает — выбрасывает RuntimeError.
    Если прокси не настроен вообще — запускает без прокси.
    """
    active_proxy = resolve_working_proxy(account_cfg)

    # Есть кандидаты, но ни один не ответил
    has_proxy_cfg = bool(
        account_cfg.get("proxy", {}).get("host") or account_cfg.get("fallback_proxies")
    )
    if has_proxy_cfg and active_proxy is None:
        raise RuntimeError(
            "Все прокси аккаунта недоступны (основной + резервные). "
            "Запуск браузера отменён."
        )

    proxy_config = _build_proxy_config(active_proxy) if active_proxy else None
    user_agent = account_cfg.get(
        "user_agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36",
    )

    profile_dir.mkdir(parents=True, exist_ok=True)
    manual_login_needed = _is_profile_empty(profile_dir)

    pw = sync_playwright().start()

    launch_kwargs = {
        "user_data_dir": str(profile_dir),
        "headless": False,
        "user_agent": user_agent,
        "viewport": {"width": 1366, "height": 768},
        "locale": "en-US",
        "timezone_id": "America/New_York",
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
    }
    if proxy_config:
        launch_kwargs["proxy"] = proxy_config

    context = pw.chromium.launch_persistent_context(**launch_kwargs)

    # ИСПРАВЛЕНИЕ: Используем новый API playwright_stealth
    stealth = Stealth()

    # Применяем ко всем существующим страницам
    for page in context.pages:
        stealth.apply_stealth_sync(page)

    # Для будущих страниц
    def apply_stealth_to_page(page):
        stealth.apply_stealth_sync(page)

    context.on("page", apply_stealth_to_page)

    if manual_login_needed:
        platforms = account_cfg.get("platforms", ["youtube"])
        # FIX #9: убеждаемся, что передаём список, а не строку
        if isinstance(platforms, str):
            platforms = [platforms]
        _manual_login_flow(context, platforms)

    return pw, context


def _manual_login_flow(context: BrowserContext, platforms: List[str]) -> None:
    """Ожидает ручного входа пользователя for multiple platforms."""
    login_urls = {
        "youtube": "https://accounts.google.com/ServiceLogin",
        "tiktok": "https://www.tiktok.com/login",
        "instagram": "https://www.instagram.com/accounts/login/",
    }
    pages = []
    for platform in platforms:
        url = login_urls.get(platform, "https://www.google.com")
        page = context.new_page()
        page.goto(url)
        pages.append(page)

    logger.info(
        "\n" + "="*60 +
        f"\n  РУЧНОЙ ЛОГИН: войдите в аккаунты {', '.join(platforms).upper()} в открытых вкладках."
        "\n  Когда закончите — нажмите ENTER в терминале."
        "\n" + "="*60
    )
    input("  >>> Нажмите ENTER после завершения логина: ")
    for page in pages:
        page.close()
    logger.info("Сессия сохранена в папку профиля.")


def close_browser(pw: Playwright, context: BrowserContext) -> None:
    """Корректно закрывает контекст и Playwright."""
    try:
        context.close()
    except Exception:
        pass
    try:
        pw.stop()
    except Exception:
        pass