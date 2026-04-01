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
from typing import Any, Dict, List, Optional

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
    timeout = float(getattr(config, "MOBILEPROXY_API_TIMEOUT_SEC", 90.0))
    max_try = int(getattr(config, "MOBILEPROXY_API_MAX_RETRIES", 2))
    retry_pause = float(getattr(config, "MOBILEPROXY_API_RETRY_ON_429_SEC", 5.0))

    for attempt in range(max_try):
        _throttle_api()
        try:
            resp = requests.get(
                config.MOBILEPROXY_API_BASE,
                headers={"Authorization": f"Bearer {config.MOBILEPROXY_API_KEY}"},
                params={"command": command, **params},
                timeout=timeout,
            )
        except Exception as exc:
            logger.warning("[mobileproxy_api] %s: %s", command, exc)
            return None

        if resp.status_code == 429:
            logger.warning(
                "[mobileproxy_api] %s HTTP 429 (лимит API: см. MOBILEPROXY_API_MIN_INTERVAL_SEC), "
                "пауза %.1fs, попытка %s/%s",
                command,
                retry_pause,
                attempt + 1,
                max_try,
            )
            if attempt + 1 < max_try:
                time.sleep(retry_pause)
                continue
            return None

        if resp.status_code != 200:
            logger.warning("[mobileproxy_api] %s HTTP %s", command, resp.status_code)
            return None
        try:
            data = resp.json()
        except Exception as exc:
            logger.warning("[mobileproxy_api] %s JSON: %s", command, exc)
            return None
        # Некоторые команды (например get_my_proxy) могут вернуть массив вместо объекта.
        # Нормализуем ответ к dict, чтобы код ниже работал единообразно.
        if isinstance(data, list):
            if command == "get_my_proxy":
                return {"status": "ok", "proxy_id": data}
            if command == "get_geo_list":
                return {"status": "ok", "_geo_list_array": data}
            logger.warning("[mobileproxy_api] %s unexpected list response", command)
            return None
        if not isinstance(data, dict):
            logger.warning(
                "[mobileproxy_api] %s unexpected response type: %s",
                command,
                type(data).__name__,
            )
            return None
        if "status" in data and str(data.get("status", "")).lower() != "ok":
            err_txt = str(data)[:300].lower()
            if (
                attempt + 1 < max_try
                and (
                    "too many" in err_txt
                    or "timeout 5" in err_txt
                    or "lonely" in err_txt
                )
            ):
                logger.warning(
                    "[mobileproxy_api] %s: ответ не ok (%s), пауза %.1fs — повтор",
                    command,
                    str(data)[:120],
                    retry_pause,
                )
                time.sleep(retry_pause)
                continue
            logger.warning("[mobileproxy_api] %s: %s", command, str(data)[:200])
            return None
        return data

    return None


def invalidate_my_proxy_cache() -> None:
    """Сброс кэша карточки прокси (после change_equipment)."""
    global _my_proxy_row_cache
    _my_proxy_row_cache = None
    try:
        from pipeline.mobileproxy_connection import invalidate_mobileproxy_http_cache

        invalidate_mobileproxy_http_cache()
    except Exception:
        pass


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


def _iso2_from_country_row(row: Dict[str, Any]) -> str:
    """mobileproxy отдаёт ISO в верхнем регистре ключа «ISO»; json чувствителен к регистру."""
    raw = (row.get("iso") or row.get("ISO") or row.get("country_code") or "").strip()
    u = raw.upper()
    return u if len(u) == 2 and u.isalpha() else ""


def _iso_map_from_get_id_country(data: Dict[str, Any]) -> Dict[str, int]:
    """Разбор поля id_country из ответа command=get_id_country."""
    out: Dict[str, int] = {}
    raw = data.get("id_country")
    if raw is None:
        raw = {}
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            iso = _iso2_from_country_row(item)
            if not iso:
                continue
            try:
                cid = int(item.get("id_country", item.get("id", 0)))
                if cid:
                    out[iso] = cid
            except (TypeError, ValueError):
                pass
        return out
    if isinstance(raw, dict):
        for k, v in raw.items():
            if isinstance(v, dict):
                iso = _iso2_from_country_row(v)
                if iso:
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
    return out


def load_iso_to_id_country_map() -> Dict[str, int]:
    """
    ISO2 → id_country (get_id_country + MOBILEPROXY_ISO_TO_ID_JSON).

    Важно: параметр API only_avaliable=1 означает «только страны со свободным
    оборудованием для аренды» — при нехватке слотов ответ может быть пустым.
    Для справочника ISO→id сначала запрашиваем get_id_country без фильтра;
    если распарсить нечего — повтор с only_avaliable=1 (на случай отличий API).
    """
    global _iso_to_id_cache
    if _iso_to_id_cache is not None:
        return _iso_to_id_cache
    manual = _manual_iso_map()
    out: Dict[str, int] = {}
    data = _api_get("get_id_country")
    if data:
        out = _iso_map_from_get_id_country(data)
    if not out:
        data2 = _api_get("get_id_country", only_avaliable="1")
        if data2:
            out = _iso_map_from_get_id_country(data2)
    if not out and not manual:
        logger.warning(
            "[mobileproxy_api] get_id_country: пустой маппинг ISO→id_country "
            "(проверьте ключ API и формат ответа; см. MOBILEPROXY_ISO_TO_ID_JSON)"
        )
    out.update(manual)
    _iso_to_id_cache = out
    return _iso_to_id_cache


def resolve_iso_to_id_country(iso2: str) -> Optional[int]:
    m = load_iso_to_id_country_map()
    return m.get(iso2.strip().upper())


def invalidate_iso_to_id_country_cache() -> None:
    """Сброс кэша ISO→id_country (тесты / смена MOBILEPROXY_ISO_TO_ID_JSON в рантайме)."""
    global _iso_to_id_cache
    _iso_to_id_cache = None


def list_supported_iso2_codes() -> list[str]:
    """
    Отсортированные ISO2 из get_id_country (полный справочник без only_avaliable,
    иначе fallback) + MOBILEPROXY_ISO_TO_ID_JSON.
    См. https://mobileproxy.space/user.html?api — command=get_id_country.
    """
    return sorted(load_iso_to_id_country_map().keys())


def iso_supported_by_mobileproxy(iso2: str) -> bool:
    """True, если для ISO есть id_country в маппинге MOBILEPROXY (API + ручной JSON)."""
    return resolve_iso_to_id_country(iso2) is not None


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
            if score is not None:
                min_s = float(getattr(config, "MOBILEPROXY_SPAM_SCORE_ROTATE_MIN", 80.0))
                if float(score) >= min_s:
                    return True
        except (TypeError, ValueError):
            pass
    return False


def change_equipment_to_country(
    id_country: int,
    proxy_id: Optional[int] = None,
    *,
    extra_params: Optional[Dict[str, Any]] = None,
    add_to_black_list: bool = False,
) -> bool:
    """
    change_equipment: переключение на другую страну (id_country).

    extra_params — опционально operator, geoid, id_city, eid (как в get_my_proxy / get_geo_list).
    add_to_black_list — убрать текущее оборудование из пула (см. доку mobileproxy).
    """
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
    if extra_params:
        for key in ("operator", "geoid", "id_city", "eid"):
            val = extra_params.get(key)
            if val is not None and str(val).strip() != "":
                params[key] = str(int(val)) if key != "operator" else str(val).strip()
    if add_to_black_list:
        params["add_to_black_list"] = "1"
    params.update(_check_spam_param())
    data = _api_get("change_equipment", **params)
    return bool(data)


def _extra_params_from_proxy_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Поля из строки get_my_proxy для полного change_equipment."""
    out: Dict[str, Any] = {}
    op = row.get("proxy_operator") or row.get("operator")
    if op:
        out["operator"] = str(op).strip()
    for fld, key in (("geoid", "geoid"), ("eid", "eid"), ("id_city", "id_city")):
        v = row.get(fld)
        if v is not None and str(v).strip() != "":
            try:
                out[key] = int(v)
            except (TypeError, ValueError):
                pass
    return out


def _geo_items_from_response(data: Optional[Dict[str, Any]]) -> List[dict]:
    if not data or not isinstance(data, dict):
        return []
    raw = data.get("_geo_list_array")
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    for key in ("geo_list", "geo_operator_list"):
        gl = data.get(key)
        if isinstance(gl, list):
            return [x for x in gl if isinstance(x, dict)]
        if isinstance(gl, dict):
            return [x for x in gl.values() if isinstance(x, dict)]
    return []


def _try_change_equipment_via_geo_list(
    target_iso: str,
    id_country: int,
    proxy_id: int,
    *,
    skip_frozen: bool = False,
) -> bool:
    """
    Перебор GEO из get_geo_list с фильтром по ISO (тот же id_country).

    skip_frozen: пропускать линии из mobileproxy_equipment_freeze (невалидные при setup).
    """
    from pipeline.mobileproxy_equipment_freeze import is_equipment_frozen

    data = _api_get("get_geo_list", proxy_id=str(proxy_id))
    items = _geo_items_from_response(data)
    if not items:
        logger.warning("[mobileproxy_api] get_geo_list не вернул записей для proxy_id=%s", proxy_id)
        return False
    want = target_iso.strip().upper()
    matching = [it for it in items if str(it.get("iso") or it.get("ISO") or "").strip().upper() == want]
    if not matching:
        logger.warning(
            "[mobileproxy_api] get_geo_list: нет GEO с ISO=%s среди %s записей",
            want,
            len(items),
        )
        return False
    use_list = matching
    if skip_frozen:
        use_list = [it for it in matching if not is_equipment_frozen(proxy_id, it)]
        if matching and not use_list:
            hrs = float(getattr(config, "MOBILEPROXY_INVALID_EQUIPMENT_FREEZE_HOURS", 24.0))
            logger.warning(
                "[mobileproxy_api] get_geo_list ISO=%s: все %s вариант(ов) в заморозке "
                "(%.0f ч после невалидной проверки) — других линий нет",
                want,
                len(matching),
                hrs,
            )
            return False
    geo_max = int(getattr(config, "MOBILEPROXY_CHANGE_EQUIPMENT_GEO_LIST_MAX", 12))
    pause = float(getattr(config, "MOBILEPROXY_CHANGE_EQUIPMENT_RETRY_PAUSE_SEC", 12.0))
    n = min(len(use_list), geo_max)
    for i, item in enumerate(use_list[:geo_max]):
        geoid = item.get("geoid")
        if geoid is None:
            continue
        try:
            gid = int(geoid)
        except (TypeError, ValueError):
            continue
        extra: Dict[str, Any] = {"geoid": gid}
        op = item.get("operator") or item.get("operator_name")
        if op:
            extra["operator"] = str(op).strip()
        ic = item.get("id_city")
        if ic is not None:
            try:
                extra["id_city"] = int(ic)
            except (TypeError, ValueError):
                pass
        logger.info(
            "[mobileproxy_api] change_equipment через get_geo_list: geoid=%s ISO=%s (%s/%s)",
            gid,
            want,
            i + 1,
            n,
        )
        if change_equipment_to_country(
            id_country,
            proxy_id=proxy_id,
            extra_params=extra,
            add_to_black_list=(i > 0),
        ):
            return True
        if pause > 0:
            time.sleep(pause)
    return False


def swap_to_fresh_equipment_same_iso(iso2: str) -> bool:
    """
    То же id_country (целевой ISO), другое оборудование: change_equipment с add_to_black_list=1
    (см. mobileproxy API). Перебор get_geo_list сначала с пропуском «замороженных» линий
    (невалидных при setup), затем типовые вызовы change_equipment.

    Вызывать, когда линия уже на нужной стране, но текущее оборудование «битое»
    (не проходит HTTP/GEO после ротаций IP).
    """
    if not config.MOBILEPROXY_API_KEY or not config.MOBILEPROXY_PROXY_ID:
        return False
    target_iso = iso2.strip().upper()
    if len(target_iso) != 2:
        return False
    target_id = resolve_iso_to_id_country(target_iso)
    if target_id is None:
        return False
    try:
        pid = int(config.MOBILEPROXY_PROXY_ID)
    except ValueError:
        return False

    invalidate_my_proxy_cache()
    row = get_my_proxy_row(force_refresh=True)
    extra: Optional[Dict[str, Any]] = None
    if row:
        extra = _extra_params_from_proxy_row(row) or None

    logger.info(
        "[mobileproxy_api] Смена оборудования в том же GEO: id_country=%s ISO=%s (add_to_black_list)",
        target_id,
        target_iso,
    )
    # 0) Сначала явный перебор get_geo_list без замороженных линий (не зацикливаться A↔B)
    if _try_change_equipment_via_geo_list(
        target_iso, target_id, pid, skip_frozen=True
    ):
        invalidate_my_proxy_cache()
        pause = float(getattr(config, "MOBILEPROXY_POST_GEO_PAUSE_SEC", 8.0))
        if pause > 0:
            time.sleep(pause)
        return True

    # 1) С контекстом текущей линии (geoid/eid/operator)
    ok = change_equipment_to_country(
        target_id,
        proxy_id=pid,
        extra_params=extra,
        add_to_black_list=True,
    )
    invalidate_my_proxy_cache()
    if ok:
        pause = float(getattr(config, "MOBILEPROXY_POST_GEO_PAUSE_SEC", 8.0))
        if pause > 0:
            time.sleep(pause)
        return True

    # 2) Без geoid/eid — иначе API может не переключить другое оборудование в том же id_country
    logger.info(
        "[mobileproxy_api] Повтор смены оборудования: только id_country + add_to_black_list (без geoid/eid)"
    )
    ok = change_equipment_to_country(
        target_id,
        proxy_id=pid,
        extra_params=None,
        add_to_black_list=True,
    )
    invalidate_my_proxy_cache()
    if ok:
        pause = float(getattr(config, "MOBILEPROXY_POST_GEO_PAUSE_SEC", 8.0))
        if pause > 0:
            time.sleep(pause)
        return True

    logger.warning(
        "[mobileproxy_api] swap same ISO: смена оборудования не удалась "
        "(get_geo_list без заморозки + change_equipment с blacklist)"
    )
    return False


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
    try:
        pid = int(config.MOBILEPROXY_PROXY_ID)
    except ValueError:
        raise RuntimeError("MOBILEPROXY_PROXY_ID не число") from None

    max_att = int(getattr(config, "MOBILEPROXY_CHANGE_EQUIPMENT_MAX_ATTEMPTS", 6))
    retry_pause = float(getattr(config, "MOBILEPROXY_CHANGE_EQUIPMENT_RETRY_PAUSE_SEC", 12.0))

    logger.info(
        "[mobileproxy_api] Смена оборудования: id_country %s → %s (%s), до %s попыток",
        current,
        target_id,
        target_iso,
        max_att,
    )

    for attempt in range(max_att):
        invalidate_my_proxy_cache()
        row = get_my_proxy_row(force_refresh=True)
        extra: Optional[Dict[str, Any]] = None
        if attempt >= 1 and row:
            extra = _extra_params_from_proxy_row(row) or None
        add_bl = attempt >= 2

        ok = change_equipment_to_country(
            target_id,
            proxy_id=pid,
            extra_params=extra,
            add_to_black_list=add_bl,
        )
        invalidate_my_proxy_cache()
        if ok:
            pause = float(getattr(config, "MOBILEPROXY_POST_GEO_PAUSE_SEC", 8.0))
            if pause > 0:
                time.sleep(pause)
            return

        logger.warning(
            "[mobileproxy_api] change_equipment не удалась (попытка %s/%s), ISO=%s",
            attempt + 1,
            max_att,
            target_iso,
        )
        if retry_pause > 0 and attempt + 1 < max_att:
            time.sleep(retry_pause)

    if _try_change_equipment_via_geo_list(target_iso, target_id, pid):
        invalidate_my_proxy_cache()
        pause = float(getattr(config, "MOBILEPROXY_POST_GEO_PAUSE_SEC", 8.0))
        if pause > 0:
            time.sleep(pause)
        return

    raise RuntimeError(
        f"mobileproxy change_equipment не удалась после {max_att} попыток и перебора GEO "
        f"(цель id_country={target_id}, ISO={target_iso})"
    )
