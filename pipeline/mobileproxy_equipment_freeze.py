"""
Локальная «заморозка» линий mobileproxy (geoid/eid/operator), которые не прошли проверку setup.

Хранится в JSON с TTL — не выбираются из get_geo_list повторно 24 ч (см. config),
чтобы не зацикливаться на двух плохих EQUIPMENT при смене оборудования.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, Optional

import portalocker

from pipeline import config

logger = logging.getLogger(__name__)


def _freeze_path() -> Path:
    return getattr(
        config,
        "MOBILEPROXY_INVALID_EQUIPMENT_FREEZE_FILE",
        config.BASE_DIR / "data" / "mobileproxy_invalid_equipment_freeze.json",
    )


def _freeze_hours() -> float:
    return float(getattr(config, "MOBILEPROXY_INVALID_EQUIPMENT_FREEZE_HOURS", 24.0))


def equipment_freeze_key(proxy_id: int, d: Dict[str, Any]) -> Optional[str]:
    """
    Стабильный ключ линии: proxy_id + geoid + eid + operator.
    Без geoid идентифицировать оборудование нельзя — заморозку не делаем.
    """
    geoid = d.get("geoid")
    if geoid is None:
        return None
    try:
        gid = int(geoid)
    except (TypeError, ValueError):
        return None
    eid_raw = d.get("eid")
    try:
        eid_i = int(eid_raw) if eid_raw is not None and str(eid_raw).strip() != "" else 0
    except (TypeError, ValueError):
        eid_i = 0
    op = (
        d.get("proxy_operator")
        or d.get("operator")
        or d.get("operator_name")
        or ""
    )
    op_n = str(op).strip().lower()
    return f"{int(proxy_id)}:{gid}:{eid_i}:{op_n}"


def _load_raw() -> Dict[str, Any]:
    path = _freeze_path()
    if not path.exists():
        return {"frozen": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {"frozen": {}}
        data.setdefault("frozen", {})
        if not isinstance(data["frozen"], dict):
            data["frozen"] = {}
        return data
    except Exception as exc:
        logger.warning("[equipment_freeze] не удалось прочитать %s: %s", path, exc)
        return {"frozen": {}}


def _prune(frozen: Dict[str, float]) -> None:
    now = time.time()
    dead = [k for k, exp in frozen.items() if exp <= now]
    for k in dead:
        del frozen[k]


def is_equipment_frozen(proxy_id: int, d: Dict[str, Any]) -> bool:
    key = equipment_freeze_key(proxy_id, d)
    if not key:
        return False
    data = _load_raw()
    frozen = data.get("frozen", {})
    if not isinstance(frozen, dict):
        return False
    _prune(frozen)
    exp = frozen.get(key)
    if exp is None:
        return False
    try:
        return float(exp) > time.time()
    except (TypeError, ValueError):
        return False


def freeze_invalid_equipment(proxy_id: int, row: Dict[str, Any]) -> None:
    """Помечает текущую линию как невалидную на MOBILEPROXY_INVALID_EQUIPMENT_FREEZE_HOURS часов."""
    key = equipment_freeze_key(proxy_id, row)
    if not key:
        logger.debug("[equipment_freeze] нет geoid в строке прокси — заморозку пропускаем")
        return
    path = _freeze_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    ttl = max(1.0, _freeze_hours() * 3600.0)
    expiry = time.time() + ttl
    lock_path = path.with_suffix(path.suffix + ".lock")
    with portalocker.Lock(str(lock_path), timeout=30):
        data = _load_raw()
        frozen = data.setdefault("frozen", {})
        if not isinstance(frozen, dict):
            data["frozen"] = {}
            frozen = data["frozen"]
        _prune(frozen)
        frozen[key] = expiry
        tmp = path.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(path)
    logger.info(
        "[equipment_freeze] линия заморожена на %.0f ч: %s",
        ttl / 3600.0,
        key,
    )


def freeze_current_proxy_line_from_api() -> None:
    """
    Снимок get_my_proxy и заморозка — для вызова перед сменой оборудования
    или после окончательного провала проверки (HTTP/GEO).
    """
    if not getattr(config, "MOBILEPROXY_PROXY_ID", None):
        return
    try:
        pid = int(config.MOBILEPROXY_PROXY_ID)
    except (TypeError, ValueError):
        return
    if not pid:
        return
    from pipeline.mobileproxy_api import get_my_proxy_row

    row = get_my_proxy_row(force_refresh=True)
    if row:
        freeze_invalid_equipment(pid, row)
