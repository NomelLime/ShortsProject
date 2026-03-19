# REBROWSER UPGRADE 2026
"""
browser.py – Инициализация браузера с поддержкой stealth.
"""

import logging
import time
import urllib.request
import json as _json
from pathlib import Path
from typing import List, Optional
from rebrowser_playwright.sync_api import sync_playwright, BrowserContext, Playwright
from playwright_stealth import Stealth

from pipeline import config as cfg
from pipeline import utils
from pipeline.fingerprint.generator import ensure_fingerprint

logger = logging.getLogger(__name__)

# Кэш GEO-проверок: "host:port" → "US" — не ходим в ip-api.com повторно
_geo_cache: dict = {}


def _get_proxy_country(proxy: dict, timeout: int = 8) -> Optional[str]:
    """
    Определяет страну прокси через ip-api.com.
    Возвращает двухбуквенный countryCode (напр. "US") или None при ошибке.
    Результат кэшируется в памяти на время сессии.
    """
    key = f"{proxy.get('host')}:{proxy.get('port')}"
    if key in _geo_cache:
        return _geo_cache[key]

    host = proxy.get("host", "")
    port = proxy.get("port", 8080)
    username = proxy.get("username", "")
    password = proxy.get("password", "")

    proxy_url = f"http://{host}:{port}"
    proxy_handler = urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})

    if username:
        password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(None, proxy_url, username, password)
        auth_handler = urllib.request.ProxyBasicAuthHandler(password_mgr)
        opener = urllib.request.build_opener(proxy_handler, auth_handler)
    else:
        opener = urllib.request.build_opener(proxy_handler)

    try:
        # Шаг 1: получаем внешний IP через прокси
        req_ip = urllib.request.Request(
            "http://httpbin.org/ip",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with opener.open(req_ip, timeout=timeout) as resp:
            ip_data = _json.loads(resp.read().decode())
            external_ip = ip_data.get("origin", "").split(",")[0].strip()

        if not external_ip:
            return None

        # Шаг 2: определяем страну по IP (ip-api.com — 1500 req/min, бесплатно)
        geo_req = urllib.request.Request(
            f"http://ip-api.com/json/{external_ip}?fields=countryCode",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urllib.request.urlopen(geo_req, timeout=timeout) as resp:
            geo_data = _json.loads(resp.read().decode())
            country = geo_data.get("countryCode", "").upper()

        if country:
            _geo_cache[key] = country
            logger.debug("[proxy-geo] %s → IP %s → %s", key, external_ip, country)
        return country or None

    except Exception as e:
        logger.debug("[proxy-geo] GEO-проверка не удалась для %s: %s", key, e)
        return None


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

    required_country = (account_cfg.get("country") or "").upper().strip()

    for proxy in candidates:
        label = f"{proxy.get('host')}:{proxy.get('port')}"
        logger.debug("[proxy] Проверяем %s...", label)
        if not utils.check_proxy_health(proxy):
            logger.warning("[proxy] Недоступен: %s — пробуем следующий...", label)
            continue

        # GEO-проверка: если для аккаунта задана страна — прокси должен совпадать
        if required_country:
            proxy_country = _get_proxy_country(proxy)
            if proxy_country and proxy_country != required_country:
                logger.warning(
                    "[proxy] GEO-несоответствие: прокси %s → %s, аккаунт требует %s — пропускаем",
                    label, proxy_country, required_country,
                )
                continue
            if proxy_country:
                logger.info("[proxy] GEO OK: %s → %s", label, proxy_country)
            else:
                logger.debug("[proxy] GEO не определён для %s — принимаем", label)

        logger.info("[proxy] Рабочий прокси: %s", label)
        account_cfg["_active_proxy"] = proxy
        return proxy

    logger.error(
        "[proxy] Нет подходящего прокси для аккаунта (страна: %s, кандидатов: %d).",
        required_country or "любая", len(candidates),
    )
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


# Реестр платформенных контекстов (lazy import для избегания circular)
def _get_platform_contexts():
    from pipeline.contexts.youtube   import YouTubeContext
    from pipeline.contexts.tiktok    import TikTokContext
    from pipeline.contexts.instagram import InstagramContext
    return {
        "youtube":   YouTubeContext(),
        "tiktok":    TikTokContext(),
        "instagram": InstagramContext(),
    }


def _save_account_config(acc_config: dict, profile_dir: Path) -> None:
    """Сохраняет обновлённый config.json аккаунта (с fingerprint) на диск."""
    import json as _json
    cfg_path = profile_dir.parent / "config.json"
    if cfg_path.exists():
        try:
            cfg_path.write_text(
                _json.dumps(acc_config, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.warning("[browser] Не удалось сохранить config.json: %s", exc)


def launch_browser(
    account_cfg: dict,
    profile_dir: Path,
    platform: str = "",
) -> tuple[Playwright, BrowserContext]:
    """
    Запускает persistent context с платформенно-адаптивным fingerprint.

    Определяет платформу → выбирает стратегию контекста → генерирует
    (или загружает) уникальный fingerprint → запускает с правильными
    мобильными/десктопными параметрами.

    ОБРАТНАЯ СОВМЕСТИМОСТЬ: вызовы launch_browser(acc_cfg, profile_dir)
    без platform работают как раньше (YouTube / десктоп).

    Args:
        account_cfg: dict конфига аккаунта (из config.json)
        profile_dir: путь к директории профиля браузера
        platform:    целевая платформа (опционально; если не задан —
                     берётся первая из account_cfg["platforms"])
    """
    # Определяем платформу
    if not platform:
        platforms = account_cfg.get("platforms", ["youtube"])
        if isinstance(platforms, str):
            platforms = [platforms]
        platform = platforms[0] if platforms else "youtube"

    platform = platform.lower()

    # Платформенная стратегия
    ctx_strategies = _get_platform_contexts()
    ctx_strategy = ctx_strategies.get(platform, ctx_strategies["youtube"])

    # Proxy (существующая логика — без изменений)
    active_proxy = resolve_working_proxy(account_cfg)
    has_proxy_cfg = bool(
        account_cfg.get("proxy", {}).get("host") or account_cfg.get("fallback_proxies")
    )
    if has_proxy_cfg and active_proxy is None:
        raise RuntimeError(
            "Все прокси аккаунта недоступны (основной + резервные). "
            "Запуск браузера отменён."
        )
    proxy_config = _build_proxy_config(active_proxy) if active_proxy else None

    # Fingerprint: генерируем или читаем из config
    country = (account_cfg.get("country") or "US").upper()
    fp = ensure_fingerprint(account_cfg, platform, country)

    # Сохраняем config с fingerprint (если изменился)
    _save_account_config(account_cfg, profile_dir)

    profile_dir.mkdir(parents=True, exist_ok=True)
    manual_login_needed = _is_profile_empty(profile_dir)

    pw = sync_playwright().start()

    # Платформенные kwargs
    launch_kwargs = ctx_strategy.build_launch_kwargs(profile_dir, fp, proxy_config)

    context = pw.chromium.launch_persistent_context(**launch_kwargs)

    # Post-launch: stealth + fingerprint инъекции
    ctx_strategy.post_launch(context, fp)

    if manual_login_needed:
        platforms_list = account_cfg.get("platforms", [platform])
        if isinstance(platforms_list, str):
            platforms_list = [platforms_list]
        _manual_login_flow(context, platforms_list)

    logger.info(
        "[browser] Запущен для %s (platform=%s, device=%s, fp=%s...)",
        profile_dir.name, platform,
        fp.get("device_name", "?"),
        fp.get("fp_seed", "?")[:8],
    )
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