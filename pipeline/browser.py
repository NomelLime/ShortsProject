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


def _is_profile_empty(profile_dir: Path) -> bool:
    """Проверяет, пустая ли папка профиля."""
    if not profile_dir.exists():
        return True
    return not any(profile_dir.glob("**/Cookies"))


def launch_browser(account_cfg: dict, profile_dir: Path) -> tuple[Playwright, BrowserContext]:
    """
    Запускает persistent context для аккаунта с применением stealth.
    Перед запуском проверяет работоспособность прокси.
    Если прокси указан, но недоступен — выбрасывает RuntimeError.
    """
    proxy_raw = account_cfg.get("proxy", {})
    proxy_config = _build_proxy_config(proxy_raw)

    # Проверяем прокси ДО запуска браузера
    if proxy_raw and proxy_raw.get("host"):
        if not utils.check_proxy_health(proxy_raw):
            raise RuntimeError(
                f"Прокси {proxy_raw.get('host')}:{proxy_raw.get('port')} недоступен. "
                "Запуск браузера для этого аккаунта отменён."
            )
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