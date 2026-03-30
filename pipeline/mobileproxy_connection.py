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
        resolve_iso_to_id_country,
    )

    if resolve_iso_to_id_country(iso) is None:
        print(
            "  ❌ Не найден id_country для ISO «%s». Задайте MOBILEPROXY_ISO_TO_ID_JSON "
            "или проверьте get_id_country в API." % iso
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

    if not check_proxy_health(proxy):
        print("  ❌ Прокси недоступен (проверка httpbin через HTTP). Аккаунт не создан.")
        return False

    gc = get_proxy_country(proxy)
    if not gc:
        print(
            "  ❌ Не удалось определить страну выхода прокси (GEO). "
            "Проверьте линию и при необходимости MOBILEPROXY_ISO_TO_ID_JSON. Аккаунт не создан."
        )
        return False
    if gc != iso:
        print(
            f"  ❌ Страна выхода прокси ({gc}) не совпадает с выбранной ({iso}). Аккаунт не создан."
        )
        return False

    print(f"  ✓ Прокси mobileproxy OK, выход: {iso}")
    return True
