"""
Клиент mobileproxy.space для ShortsProject: get_my_proxy, get_id_country, change_equipment.

Используется для выравнивания страны линии (id_country) под config.json аккаунта
перед ротацией exit-IP. Лимиты API: см. MOBILEPROXY_API_MIN_INTERVAL_SEC.

Проверка спам-баз (IPGuardian): параметр check_spam=true у proxy_ip и change_equipment.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Dict, Optional

import requests

from pipeline import config

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_last_api_monotonic: float = 0.0

_my_proxy_row_cache: Optional[Dict[str, Any]] = None
_iso_to_id_cache: Optional[Dict[str, int]] = None


def _throttle_api() -> None:
    global _last_api_monotonic
    with _lock:
        gap = time.monotonic() - _last_api_monotonic
        need = config.MOBILEPROXY_API_MIN_INTERVAL_SEC
        if gap < need:
            time.sleep(need - gap)
        _last_api_monotonic = time.monotonic()


def _api_get(command: str, **params: Any) -> Optional[Dict[str, Any]]:
    if not config.MOBILEPROXY_API_KEY:
        return None
    _throttle_api()
    try:
        resp = requests.get(
            config.MOBILEPROXY_API_BASE,
            headers={"Authorization": f"Bearer {config.MOBILEPROXY_API_KEY}"},
            params={"command": command, **params},
            timeout=25,
        )
        if resp.status_code != 200:
            logger.warning("[mobileproxy_api] %s HTTP %s", command, resp.status_code)
            return None
        data = resp.json()
        if str(data.get("status", "")).lower() != "ok":
            logger.warning("[mobileproxy_api] %s: %s", command, str(data)[:200])
            return None
        return data
    except Exception as exc:
        logger.warning("[mobileproxy_api] %s: %s", command, exc)
        return None


def invalidate_my_proxy_cache() -> None:
    """Сброс кэша карточки прокси (после change_equipment)."""
    global _my_proxy_row_cache
    _my_proxy_row_cache = None


def get_my_proxy_row(force_refresh: bool = False) -> Optional[Dict[str, Any]]:
    """
    Одна запись прокси по MOBILEPROXY_PROXY_ID (get_my_proxy).
    """
    global _my_proxy_row_cache
    if _my_proxy_row_cache is not None and not force_refresh:
        return _my_proxy_row_cache
    if not config.MOBILEPROXY_PROXY_ID:
        return None
    data = _api_get("get_my_proxy", proxy_id=config.MOBILEPROXY_PROXY_ID)
    if not data:
        return None
    raw = data.get("proxy_id", data)
    if isinstance(raw, dict):
        lst = list(raw.values())
    elif isinstance(raw, list):
        lst = raw
    else:
        lst = []
    if not lst and isinstance(data, dict):
        lst = [data]
    want_id: Optional[int] = None
    try:
        want_id = int(config.MOBILEPROXY_PROXY_ID)
    except ValueError:
        want_id = None
    row: Optional[Dict[str, Any]] = None
    for item in lst:
        if not isinstance(item, dict):
            continue
        if want_id is not None and int(item.get("proxy_id", 0) or 0) != want_id:
            continue
        row = item
        break
    if row is None and lst:
        cand = lst[0]
        row = cand if isinstance(cand, dict) else None
    if row:
        _my_proxy_row_cache = row
    return row


def _manual_iso_map() -> Dict[str, int]:
    raw = config.MOBILEPROXY_ISO_TO_ID_JSON or ""
    if not str(raw).strip():
        return {}
    try:
        d = json.loads(raw)
        return {str(k).upper(): int(v) for k, v in d.items()}
    except Exception as exc:
        logger.warning("[mobileproxy_api] MOBILEPROXY_ISO_TO_ID_JSON: %s", exc)
        return {}


def load_iso_to_id_country_map() -> Dict[str, int]:
    """ISO2 → id_country (кэш + get_id_country + ручной JSON)."""
    global _iso_to_id_cache
    if _iso_to_id_cache is not None:
        return _iso_to_id_cache
    manual = _manual_iso_map()
    if manual:
        _iso_to_id_cache = manual
        return _iso_to_id_cache
    data = _api_get("get_id_country", only_avaliable="1")
    if not data:
        _iso_to_id_cache = {}
        return _iso_to_id_cache
    out: Dict[str, int] = {}
    raw = data.get("id_country") or {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            if isinstance(v, dict):
                iso = (v.get("iso") or v.get("country_code") or "").strip().upper()
                if len(iso) == 2:
                    try:
                        cid = int(v.get("id_country", v.get("id", k)))
                        out[iso] = cid
                    except (TypeError, ValueError):
                        pass
            else:
                try:
                    kid = int(k)
                    if isinstance(v, str) and len(v.strip()) == 2:
                        out[v.strip().upper()] = kid
                except (TypeError, ValueError):
                    pass
    _iso_to_id_cache = out
    return _iso_to_id_cache


def resolve_iso_to_id_country(iso2: str) -> Optional[int]:
    m = load_iso_to_id_country_map()
    return m.get(iso2.strip().upper())


def mobileproxy_geo_enabled(account_cfg: Optional[dict] = None) -> bool:
    """Смена страны оборудования по API (не путать с реестром IP)."""
    if not config.MOBILEPROXY_CHANGE_GEO:
        return False
    if not config.MOBILEPROXY_API_KEY or not config.MOBILEPROXY_PROXY_ID:
        return False
    if account_cfg is not None:
        c = (account_cfg.get("country") or "").strip()
        if not c:
            return False
    return True


def _check_spam_param() -> Dict[str, str]:
    """Параметры для API, если включена проверка IPGuardian."""
    if not getattr(config, "MOBILEPROXY_CHECK_SPAM", True):
        return {}
    return {"check_spam": "true"}


def fetch_proxy_ip_with_spam_check(proxy_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """
    command=proxy_ip с check_spam=true — текущий exit-IP и данные IPGuardian.net.
    """
    if not config.MOBILEPROXY_CHECK_SPAM:
        return None
    if not config.MOBILEPROXY_API_KEY or not config.MOBILEPROXY_PROXY_ID:
        return None
    pid = proxy_id
    if pid is None:
        try:
            pid = int(config.MOBILEPROXY_PROXY_ID)
        except ValueError:
            return None
    if not pid:
        return None
    params: Dict[str, Any] = {"proxy_id": str(pid), "check_spam": "true"}
    return _api_get("proxy_ip", **params)


def spam_check_requires_rotation(data: Optional[Dict[str, Any]]) -> bool:
    """
    True — IP отмечен в спам-базе, нужна смена IP.
    При ошибке API или отсутствии блока ipguardian — False (не блокируем пайплайн).
    """
    if not data or str(data.get("status", "")).lower() != "ok":
        return False
    ig = data.get("ipguardian.net") or data.get("ipguardian")
    if ig is None:
        return False
    if isinstance(ig, dict):
        for k in ("spam", "is_spam", "listed", "blacklisted", "in_blacklist", "is_blacklisted"):
            v = ig.get(k)
            if v in (True, 1, "1", "yes", "true", "True"):
                return True
        st = str(ig.get("status", "") or "").lower()
        if st in ("spam", "listed", "bad", "blacklist", "blacklisted"):
            return True
        score = ig.get("spam_score") or ig.get("risk") or ig.get("score")
        try:
            if score is not None and float(score) >= 80.0:
                return True
        except (TypeError, ValueError):
            pass
    return False


def change_equipment_to_country(id_country: int, proxy_id: Optional[int] = None) -> bool:
    """change_equipment: переключение на другую страну (id_country)."""
    pid = proxy_id
    if pid is None:
        try:
            pid = int(config.MOBILEPROXY_PROXY_ID)
        except ValueError:
            return False
    if not pid:
        return False
    params: Dict[str, Any] = {
        "proxy_id": str(pid),
        "id_country": str(id_country),
        "check_after_change": "true",
    }
    params.update(_check_spam_param())
    data = _api_get("change_equipment", **params)
    return bool(data)


def ensure_equipment_country_for_iso(iso2: str) -> None:
    """
    Если текущая линия прокси (id_country) не совпадает с целевой ISO2 — change_equipment.
    Без ключа API / proxy_id — no-op. Неизвестный ISO в справочнике — предупреждение, no-op.
    """
    if not config.MOBILEPROXY_API_KEY or not config.MOBILEPROXY_PROXY_ID:
        return
    target_iso = iso2.strip().upper()
    if len(target_iso) != 2:
        return
    target_id = resolve_iso_to_id_country(target_iso)
    if target_id is None:
        logger.warning(
            "[mobileproxy_api] Не найден id_country для ISO %s — задайте MOBILEPROXY_ISO_TO_ID_JSON "
            "или проверьте get_id_country",
            target_iso,
        )
        return
    row = get_my_proxy_row()
    if not row:
        logger.warning("[mobileproxy_api] get_my_proxy пуст — пропуск смены страны")
        return
    try:
        current = int(row.get("id_country", 0) or 0)
    except (TypeError, ValueError):
        current = 0
    if current == target_id:
        logger.debug("[mobileproxy_api] Линия уже id_country=%s (%s)", target_id, target_iso)
        return
    logger.info(
        "[mobileproxy_api] Смена оборудования: id_country %s → %s (%s)",
        current,
        target_id,
        target_iso,
    )
    ok = change_equipment_to_country(target_id)
    invalidate_my_proxy_cache()
    if not ok:
        raise RuntimeError(
            f"mobileproxy change_equipment не удалась (цель id_country={target_id}, ISO={target_iso})"
        )
    pause = float(getattr(config, "MOBILEPROXY_POST_GEO_PAUSE_SEC", 8.0))
    if pause > 0:
        time.sleep(pause)
