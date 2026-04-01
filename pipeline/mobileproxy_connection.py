"""
HTTP-прокси mobileproxy.space из get_my_proxy: host/port/login без дублирования в каждом аккаунте.

Источник правды — API; в data/mobileproxy_http_cache.json хранится последний успешный снимок
на случай недоступности API при старте пайплайна.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, Optional

from pipeline import config

logger = logging.getLogger(__name__)

_memory_proxy: Optional[Dict[str, Any]] = None
_memory_ts: float = 0.0


def invalidate_mobileproxy_http_cache() -> None:
    """Сброс in-memory кэша строки подключения (после change_equipment и т.п.)."""
    global _memory_proxy, _memory_ts
    _memory_proxy = None
    _memory_ts = 0.0


def http_proxy_dict_from_my_proxy_row(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Собирает dict для pipeline/browser (host, port, username, password) из строки get_my_proxy.

    См. поля API: proxy_hostname, proxy_http_port, proxy_login, proxy_pass.
    """
    if not row or not isinstance(row, dict):
        return None
    host = (row.get("proxy_hostname") or row.get("proxy_host_ip") or "").strip()
    port_raw = row.get("proxy_http_port")
    if port_raw is None or port_raw == "":
        port_raw = row.get("proxy_independent_port")
    try:
        port = int(port_raw)
    except (TypeError, ValueError):
        return None
    if not host or port <= 0:
        return None
    login = (row.get("proxy_login") or "").strip()
    password = (row.get("proxy_pass") or "").strip()
    out: Dict[str, Any] = {"host": host, "port": port}
    if login:
        out["username"] = login
        out["password"] = password
    return out


def _cache_path() -> Path:
    return config.MOBILEPROXY_HTTP_CACHE_FILE


def _read_disk_cache() -> Optional[Dict[str, Any]]:
    path = _cache_path()
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        p = raw.get("proxy")
        if isinstance(p, dict) and p.get("host"):
            return p
    except Exception as exc:
        logger.debug("[mobileproxy_connection] disk cache read: %s", exc)
    return None


def _write_disk_cache(proxy: Dict[str, Any]) -> None:
    path = _cache_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {"updated_at": time.time(), "proxy": proxy},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.warning("[mobileproxy_connection] disk cache write: %s", exc)


def fetch_mobileproxy_http_proxy(
    *,
    force_refresh: bool = False,
    use_cache_on_api_fail: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Текущие параметры HTTP-прокси по MOBILEPROXY_API_KEY + MOBILEPROXY_PROXY_ID.

    Кэш в памяти (TTL — MOBILEPROXY_HTTP_MEMORY_TTL_SEC), затем API, затем диск при сбое API.
    """
    global _memory_proxy, _memory_ts

    if not config.MOBILEPROXY_API_KEY or not config.MOBILEPROXY_PROXY_ID:
        return None

    ttl = float(getattr(config, "MOBILEPROXY_HTTP_MEMORY_TTL_SEC", 300.0))
    now = time.monotonic()
    if (
        not force_refresh
        and _memory_proxy is not None
        and (now - _memory_ts) < ttl
    ):
        return dict(_memory_proxy)

    if force_refresh:
        invalidate_mobileproxy_http_cache()

    from pipeline.mobileproxy_api import get_my_proxy_row

    row = get_my_proxy_row(force_refresh=True)
    proxy = http_proxy_dict_from_my_proxy_row(row)
    if proxy:
        _memory_proxy = dict(proxy)
        _memory_ts = now
        _write_disk_cache(proxy)
        return proxy

    if use_cache_on_api_fail:
        disk = _read_disk_cache()
        if disk:
            logger.warning(
                "[mobileproxy_connection] API недоступен — используем последний снимок из %s",
                _cache_path(),
            )
            _memory_proxy = dict(disk)
            _memory_ts = now
            return disk

    return None


def mobileproxy_env_configured() -> bool:
    return bool(config.MOBILEPROXY_API_KEY and config.MOBILEPROXY_PROXY_ID)


def verify_mobileproxy_for_new_account(country_iso: str) -> bool:
    """
    Перед созданием аккаунта: смена линии под ISO, проверка доступности и GEO выхода.
    Возвращает True только при успехе.
    """
    iso = country_iso.strip().upper()
    if len(iso) != 2 or not iso.isalpha():
        print("  ❌ Некорректный код страны.")
        return False
    if not mobileproxy_env_configured():
        print(
            "  ❌ Задайте в .env переменные MOBILEPROXY_API_KEY и MOBILEPROXY_PROXY_ID "
            "(https://mobileproxy.space/user.html?api)."
        )
        return False

    from pipeline.mobileproxy_api import (
        ensure_equipment_country_for_iso,
        invalidate_my_proxy_cache,
        iso_supported_by_mobileproxy,
        swap_to_fresh_equipment_same_iso,
    )
    from pipeline.mobileproxy_equipment_freeze import freeze_current_proxy_line_from_api

    if not iso_supported_by_mobileproxy(iso):
        print(
            "  ❌ ISO «%s» не входит в список стран mobileproxy.space "
            "(command=get_id_country, см. %s). "
            "Задайте MOBILEPROXY_ISO_TO_ID_JSON или выберите код из ответа API."
            % (iso, config.MOBILEPROXY_API_DOCS_URL)
        )
        return False

    print("  … Выравнивание линии прокси под страну (change_equipment / API)…")
    try:
        ensure_equipment_country_for_iso(iso)
    except RuntimeError as exc:
        print(f"  ❌ {exc}")
        return False
    except Exception as exc:
        print(f"  ❌ Ошибка API mobileproxy: {exc}")
        return False

    invalidate_my_proxy_cache()
    proxy = fetch_mobileproxy_http_proxy(force_refresh=True, use_cache_on_api_fail=False)
    if not proxy:
        print("  ❌ Не удалось получить параметры прокси (get_my_proxy). Аккаунт не создан.")
        return False

    from pipeline.utils import check_proxy_health
    from pipeline.browser import get_proxy_country
    from pipeline.proxy_ip_registry import get_change_ip_url, rotate_exit_ip_preserving_country

    setup_timeout = float(getattr(config, "MOBILEPROXY_VERIFY_SETUP_TIMEOUT_SEC", 90.0))
    retry_pause = float(getattr(config, "MOBILEPROXY_VERIFY_SETUP_RETRY_PAUSE_SEC", 15.0))
    extra_pause = float(getattr(config, "MOBILEPROXY_VERIFY_SETUP_EXTRA_PAUSE_SEC", 10.0))
    max_rounds = max(1, int(getattr(config, "MOBILEPROXY_VERIFY_SETUP_ROTATE_ATTEMPTS", 4)))
    rotate_pause = float(getattr(config, "MOBILEPROXY_VERIFY_SETUP_ROTATE_PAUSE_SEC", 12.0))
    swap_max = max(1, int(getattr(config, "MOBILEPROXY_VERIFY_SETUP_EQUIPMENT_SWAP_ATTEMPTS", 4)))
    swap_pause = float(getattr(config, "MOBILEPROXY_VERIFY_SETUP_EQUIPMENT_SWAP_PAUSE_SEC", 15.0))

    can_rotate_ip = bool(get_change_ip_url())
    rounds = max_rounds if can_rotate_ip else 1
    account_cfg_min = {"country": iso}
    healthy = False

    for swap_i in range(swap_max):
        # Первая итерация: пауза после ensure_equipment; далее — новое оборудование
        # задаётся в конце предыдущей итерации (см. блок после внутреннего цикла).
        if swap_i == 0 and extra_pause > 0:
            print(
                f"  … Пауза {extra_pause:.0f}s после смены линии перед проверкой HTTP…"
            )
            time.sleep(extra_pause)

        for round_i in range(rounds):
            healthy = check_proxy_health(proxy, timeout=setup_timeout)
            if not healthy:
                print(
                    f"  … Повторная проверка прокси через {retry_pause:.0f}s "
                    "(мобильная линия после смены гео может отвечать дольше обычного)…"
                )
                time.sleep(retry_pause)
                invalidate_mobileproxy_http_cache()
                proxy = fetch_mobileproxy_http_proxy(force_refresh=True, use_cache_on_api_fail=False)
                if not proxy:
                    print(
                        "  ❌ Не удалось обновить параметры прокси после ожидания. "
                        "Аккаунт не создан."
                    )
                    return False
                healthy = check_proxy_health(proxy, timeout=setup_timeout)

            if healthy:
                break

            if round_i + 1 >= rounds:
                break

            print(
                "  … Ротация exit-IP в том же GEO (proxy_change_ip_url) — "
                f"попытка {round_i + 2}/{rounds}…"
            )
            if not rotate_exit_ip_preserving_country(account_cfg_min, proxy):
                print(
                    "  ⚠ Ротация exit-IP не удалась — переходим к смене оборудования "
                    "в том же GEO (если остались попытки)."
                )
                break
            invalidate_my_proxy_cache()
            invalidate_mobileproxy_http_cache()
            proxy = fetch_mobileproxy_http_proxy(force_refresh=True, use_cache_on_api_fail=False)
            if not proxy:
                print(
                    "  ⚠ После ротации не удалось получить параметры прокси — "
                    "пробуем смену оборудования."
                )
                break
            if rotate_pause > 0:
                time.sleep(rotate_pause)

        if healthy:
            break

        # Проверка/ротации IP исчерпаны — смена оборудования (тот же id_country), затем новый круг
        if swap_i + 1 >= swap_max:
            break

        print(
            "  … Смена оборудования в том же GEO (change_equipment + add_to_black_list, "
            "см. https://mobileproxy.space/user.html?api)…"
        )
        # Невалидная линия не выбирается из get_geo_list повторно 24 ч (см. MOBILEPROXY_INVALID_*)
        freeze_current_proxy_line_from_api()
        if not swap_to_fresh_equipment_same_iso(iso):
            print(
                "  ❌ Не удалось сменить оборудование в том же GEO (mobileproxy API). "
                "Аккаунт не создан."
            )
            return False
        invalidate_my_proxy_cache()
        invalidate_mobileproxy_http_cache()
        proxy = fetch_mobileproxy_http_proxy(force_refresh=True, use_cache_on_api_fail=False)
        if not proxy:
            print(
                "  ❌ После смены оборудования не удалось получить параметры прокси. "
                "Аккаунт не создан."
            )
            return False
        if swap_pause > 0:
            time.sleep(swap_pause)

    if not healthy:
        freeze_current_proxy_line_from_api()
        print(
            "  ❌ Прокси недоступен (проверка HTTP через MOBILEPROXY_PROXY_HEALTH_CHECK_URLS). "
            f"Увеличьте MOBILEPROXY_VERIFY_SETUP_TIMEOUT_SEC (сейчас {setup_timeout:.0f}s), "
            "MOBILEPROXY_VERIFY_CONNECT_TIMEOUT_SEC, MOBILEPROXY_VERIFY_SETUP_EQUIPMENT_SWAP_ATTEMPTS "
            f"(сейчас {swap_max}), MOBILEPROXY_VERIFY_SETUP_ROTATE_ATTEMPTS (сейчас {max_rounds}). "
            "Аккаунт не создан."
        )
        return False

    geo_timeout = int(max(setup_timeout, 15.0))
    gc = get_proxy_country(proxy, timeout=geo_timeout)
    if not gc:
        print(
            "  ❌ Не удалось определить страну выхода прокси (GEO). "
            "Проверьте линию и при необходимости MOBILEPROXY_ISO_TO_ID_JSON. Аккаунт не создан."
        )
        return False
    if gc != iso:
        freeze_current_proxy_line_from_api()
        print(
            f"  ❌ Страна выхода прокси ({gc}) не совпадает с выбранной ({iso}). Аккаунт не создан."
        )
        return False

    print(f"  ✓ Прокси mobileproxy OK, выход: {iso}")
    return True
