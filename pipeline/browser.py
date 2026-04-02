# REBROWSER UPGRADE 2026
"""
browser.py – Инициализация браузера с поддержкой stealth.
"""

import logging
import os
import random
import sys
import threading
import time
from pathlib import Path
from typing import List, Optional
from rebrowser_playwright.sync_api import sync_playwright, BrowserContext, Playwright
from playwright_stealth import Stealth

from pipeline import config as cfg
from pipeline import utils
from pipeline.fingerprint.generator import ensure_fingerprint
from pipeline.mobileproxy_connection import fetch_mobileproxy_http_proxy
from pipeline.proxy_ip_registry import (
    account_id_from,
    ensure_exit_ip_for_account,
    proxy_ip_registry_enabled,
)

logger = logging.getLogger(__name__)

# Кэш GEO-проверок: "host:port" → "US" — не ходим в ip-api.com повторно
_geo_cache: dict = {}


def invalidate_proxy_geo_cache(proxy: dict) -> None:
    """Сбрасывает кэш GEO после смены exit-IP на том же host:port."""
    key = f"{proxy.get('host')}:{proxy.get('port')}"
    _geo_cache.pop(key, None)


def get_proxy_country(proxy: dict, timeout: int = 8) -> Optional[str]:
    """
    Определяет страну прокси через ip-api.com.
    Возвращает двухбуквенный countryCode (напр. "US") или None при ошибке.
    Результат кэшируется в памяти на время сессии.
    """
    key = f"{proxy.get('host')}:{proxy.get('port')}"
    if key in _geo_cache:
        return _geo_cache[key]

    try:
        # Шаг 1: получаем внешний IP через прокси (http/socks5)
        external_ip = utils.fetch_exit_ip_via_proxy(proxy, timeout=float(timeout))
        if not external_ip:
            return None

        # Шаг 2: определяем страну по IP.
        country = utils.fetch_country_for_ip(external_ip, timeout=float(timeout))

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
    scheme = (proxy.get("scheme") or "http").strip().lower()
    # Chromium/Playwright не понимает socks5h в proxy.server (ERR_NO_SUPPORTED_PROXIES).
    # Для браузера используем socks5, оставляя остальные схемы без изменений.
    if scheme == "socks5h":
        logger.info("[proxy] scheme socks5h -> socks5 для Playwright")
        scheme = "socks5"
    proxy_cfg = {
        "server": f"{scheme}://{proxy['host']}:{proxy['port']}",
    }
    if proxy.get("username"):
        proxy_cfg["username"] = proxy["username"]
        proxy_cfg["password"] = proxy.get("password", "")
    return proxy_cfg


def resolve_working_proxy(account_cfg: dict) -> dict | None:
    """
    Возвращает первый работающий прокси для аккаунта.

    Порядок проверки:
      1. Основной прокси: account_cfg["proxy"] (если задан host — ручной override)
      2. Иначе — HTTP-прокси из mobileproxy API (MOBILEPROXY_API_KEY + MOBILEPROXY_PROXY_ID),
         с кэшем в data/mobileproxy_http_cache.json
      3. Резервные прокси: account_cfg["fallback_proxies"] (список)

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

    # Явный override для всей инфраструктуры (http/socks5/socks5h)
    explicit_proxy_url = (os.getenv("PROXY") or "").strip()
    explicit_proxy_cfg = utils.proxy_url_to_cfg(explicit_proxy_url or "")
    if explicit_proxy_cfg and explicit_proxy_cfg.get("host"):
        candidates.append(explicit_proxy_cfg)

    primary = account_cfg.get("proxy", {})
    if primary and primary.get("host"):
        candidates.append(primary)
    else:
        mp = fetch_mobileproxy_http_proxy(force_refresh=False, use_cache_on_api_fail=True)
        if mp:
            candidates.append(mp)

    for fb in account_cfg.get("fallback_proxies", []):
        if fb and fb.get("host"):
            candidates.append(fb)

    if not candidates:
        return None  # прокси не настроен вообще

    required_country = (account_cfg.get("country") or "").upper().strip()

    for proxy in candidates:
        label = f"{proxy.get('host')}:{proxy.get('port')}"
        scheme = (proxy.get("scheme") or "http").strip().lower()
        has_auth = bool((proxy.get("username") or "").strip())
        if scheme in ("socks5", "socks5h") and has_auth:
            logger.warning(
                "[proxy] %s использует SOCKS с auth; Chromium/Playwright это не поддерживает — пропускаем",
                label,
            )
            continue
        logger.debug("[proxy] Проверяем %s...", label)
        if not utils.check_proxy_health(proxy):
            logger.warning("[proxy] Недоступен: %s — пробуем следующий...", label)
            continue

        # GEO-проверка: если для аккаунта задана страна — прокси должен совпадать
        if required_country:
            proxy_country = get_proxy_country(proxy)
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


def _soften_launch_kwargs_for_login(launch_kwargs: dict) -> dict:
    """
    Более «обычный» запуск Chrome для страниц авторизации.
    Убираем часть флагов, которые чаще триггерят антибот-эвристику.
    """
    out = dict(launch_kwargs)
    args = list(out.get("args") or [])
    drop_exact = {
        "--disable-blink-features=AutomationControlled",
        "--disable-extensions-except=",
    }
    out["args"] = [a for a in args if a not in drop_exact]
    return out


def check_session_valid(context: BrowserContext, platforms: list[str]) -> dict[str, bool]:
    """
    Проверяет, залогинен ли браузер на каждой из платформ.

    Открывает служебную страницу (требующую авторизации) и проверяет,
    не произошёл ли редирект на страницу логина.

    Возвращает словарь {platform: is_logged_in}.
    """
    results: dict[str, bool] = {}
    page = context.new_page()
    nav_timeout_ms = int(getattr(cfg, "SESSION_CHECK_NAV_TIMEOUT_MS", 12000))

    for platform in platforms:
        check_url = _SESSION_CHECK_URLS.get(platform)
        if not check_url:
            results[platform] = True  # неизвестная платформа — не блокируем
            continue

        try:
            logger.info("[session][%s] Проверка сессии: %s", platform, check_url)
            page.goto(check_url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
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
            logger.warning(
                "[session][%s] Не удалось проверить сессию за %sms: %s",
                platform,
                nav_timeout_ms,
                exc,
            )
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
            safe_cfg = dict(acc_config)
            # runtime-поле; не сохраняем, чтобы не тащить временный прокси между сессиями
            safe_cfg.pop("_active_proxy", None)
            cfg_path.write_text(
                _json.dumps(safe_cfg, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.warning("[browser] Не удалось сохранить config.json: %s", exc)


def launch_browser(
    account_cfg: dict,
    profile_dir: Path,
    platform: str = "",
    allow_direct_fallback: bool = False,
    use_ip_registry: bool = True,
    force_manual_login: bool = False,
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

    # Proxy обязателен для любого запуска браузера.
    active_proxy = resolve_working_proxy(account_cfg)
    if active_proxy is None:
        raise RuntimeError(
            "Прокси обязателен для запуска браузера. "
            "Настройте proxy/fallback_proxies в аккаунте и убедитесь, что прокси доступен."
        )
    if active_proxy and use_ip_registry and proxy_ip_registry_enabled(account_cfg):
        ensure_exit_ip_for_account(
            account_id_from(profile_dir, account_cfg),
            account_cfg,
            active_proxy,
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
    if force_manual_login:
        launch_kwargs = _soften_launch_kwargs_for_login(launch_kwargs)

    logger.info("[browser] launch_persistent_context start (platform=%s)", platform)
    try:
        context = pw.chromium.launch_persistent_context(**launch_kwargs)
    except Exception as exc:
        if allow_direct_fallback:
            logger.warning(
                "[browser] launch с прокси не удался (%s) — пробуем direct без прокси для ручного логина",
                exc,
            )
            try:
                pw.stop()
            except Exception:
                pass
            pw = sync_playwright().start()
            launch_kwargs_no_proxy = dict(launch_kwargs)
            launch_kwargs_no_proxy.pop("proxy", None)
            context = pw.chromium.launch_persistent_context(**launch_kwargs_no_proxy)
        else:
            raise

    # Post-launch: stealth + fingerprint инъекции
    ctx_strategy.post_launch(context, fp)

    platforms_list = account_cfg.get("platforms", [platform])
    if isinstance(platforms_list, str):
        platforms_list = [platforms_list]
    platforms_list = [str(p).lower() for p in platforms_list]

    # Ручной логин нужен не только для пустого профиля: сессии могли протухнуть,
    # а файл Cookies при этом уже существует.
    login_required = bool(manual_login_needed or force_manual_login)
    if not login_required:
        session_state = check_session_valid(context, platforms_list)
        invalid = [p for p, ok in session_state.items() if not ok]
        if invalid:
            logger.info(
                "[session] Обнаружены невалидные сессии: %s — запускаем ручной логин",
                ", ".join(invalid),
            )
            login_required = True

    if login_required:
        # В фоновых потоках scheduler нет безопасного interactive stdin:
        # там не блокируем выполнение input(), только логируем.
        can_prompt = bool(getattr(sys.stdin, "isatty", lambda: False)()) and (
            threading.current_thread() is threading.main_thread()
        )
        if can_prompt:
            _manual_login_flow(context, platforms_list)
        else:
            logger.warning(
                "[session] Требуется ручной логин (%s), но запуск неинтерактивный; "
                "выполните ручной запуск browser/login из основного терминала.",
                ", ".join(platforms_list),
            )

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
    warm_min = int(getattr(cfg, "LOGIN_POST_AUTH_WARMUP_MIN_SEC", 120))
    warm_max = int(getattr(cfg, "LOGIN_POST_AUTH_WARMUP_MAX_SEC", 300))
    if warm_max >= warm_min > 0:
        pause = int(random.randint(warm_min, warm_max))
        logger.info("[session] Пост-логин прогрев браузера: %ss", pause)
        time.sleep(pause)
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