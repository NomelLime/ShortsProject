"""
Реестр exit-IP на аккаунт + сериализация ротации (один mobileproxy).

Политика (см. обсуждение):
  — помнить IP для аккаунта «навсегда»; предпочитать тот же exit;
  — если недоступен — любой «чистый» (не привязанный к другому аккаунту в реестре);
  — в крайнем случае — текущий IP, даже если занят другим аккаунтом;
  — ротация до успеха по стране аккаунта;
  — глобальная блокировка (файл + очередь ожидания ОС) на смену IP.

Включение: SHORTS_PROXY_IP_REGISTRY=1 или MOBILEPROXY_CHANGE_IP_URL,
  либо MOBILEPROXY_API_KEY + MOBILEPROXY_PROXY_ID.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, Optional

import portalocker
import requests

from pipeline import config
from pipeline import utils
from pipeline.mobileproxy_api import mobileproxy_geo_enabled

logger = logging.getLogger(__name__)


def proxy_ip_registry_enabled(account_cfg: Optional[dict] = None) -> bool:
    """Включён ли реестр для данного аккаунта."""
    if account_cfg and account_cfg.get("proxy_ip_registry") is False:
        return False
    if account_cfg and account_cfg.get("proxy_ip_registry") is True:
        return True
    return bool(
        config.SHORTS_PROXY_IP_REGISTRY
        or config.MOBILEPROXY_CHANGE_IP_URL
        or (config.MOBILEPROXY_API_KEY and config.MOBILEPROXY_PROXY_ID)
    )


def _registry_path() -> Path:
    return config.PROXY_IP_REGISTRY_FILE


def _load_registry() -> Dict[str, Any]:
    path = _registry_path()
    if not path.exists():
        return {"accounts": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {"accounts": {}}
        data.setdefault("accounts", {})
        if not isinstance(data["accounts"], dict):
            data["accounts"] = {}
        return data
    except Exception as exc:
        logger.warning("[proxy_ip_registry] Не удалось прочитать %s: %s", path, exc)
        return {"accounts": {}}


def _save_registry(data: Dict[str, Any]) -> None:
    path = _registry_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def account_id_from(profile_dir: Path, account_cfg: dict) -> str:
    """Стабильный id: account_id / name / имя папки профиля."""
    for key in ("account_id", "name", "id"):
        v = account_cfg.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return profile_dir.name


def _is_ip_clean_for_account(ip: Optional[str], account_id: str, reg: Dict[str, Any]) -> bool:
    """Чистый = не записан ни за каким другим аккаунтом (свой IP разрешён)."""
    if not ip:
        return False
    for aid, row in reg.get("accounts", {}).items():
        if aid == account_id:
            continue
        if isinstance(row, dict) and row.get("ip") == ip:
            return False
    return True


def get_change_ip_url() -> Optional[str]:
    """
    URL смены IP: env MOBILEPROXY_CHANGE_IP_URL или get_my_proxy (mobileproxy_api).
    """
    if config.MOBILEPROXY_CHANGE_IP_URL:
        return config.MOBILEPROXY_CHANGE_IP_URL.strip() or None
    if not config.MOBILEPROXY_API_KEY or not config.MOBILEPROXY_PROXY_ID:
        return None
    from pipeline.mobileproxy_api import get_my_proxy_row

    row = get_my_proxy_row()
    if not row:
        return None
    return (row.get("proxy_change_ip_url") or "").strip() or None


def clear_proxy_session_caches() -> None:
    """После change_equipment / смены линии — сброс кэша карточки прокси."""
    from pipeline.mobileproxy_api import invalidate_my_proxy_cache

    invalidate_my_proxy_cache()


def _rotate_once(change_url: str) -> tuple[bool, Optional[str]]:
    try:
        resp = requests.get(
            change_url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
            },
            params={"format": "json"},
            timeout=25,
        )
        data = resp.json()
        ok = data.get("code") == 200 or str(data.get("status", "")).lower() == "ok"
        new_ip = data.get("new_ip")
        nip = new_ip if isinstance(new_ip, str) else None
        return ok, nip
    except Exception as exc:
        logger.warning("[proxy_ip_registry] rotate: %s", exc)
        return False, None


def _country_matches(account_cfg: dict, exit_ip: Optional[str]) -> bool:
    req = (account_cfg.get("country") or "").upper().strip()
    if not req:
        return True
    if not exit_ip:
        return False
    cc = utils.fetch_country_for_ip(exit_ip)
    return bool(cc and cc == req)


def _invalidate_browser_geo_cache(proxy_cfg: dict) -> None:
    from pipeline import browser as browser_mod

    browser_mod.invalidate_proxy_geo_cache(proxy_cfg)


def ensure_exit_ip_for_account(
    account_id: str,
    account_cfg: dict,
    active_proxy: dict,
) -> None:
    """
    Подбирает exit-IP по политике реестра; держит глобальную блокировку на всё время.

    Вызывать после resolve_working_proxy, до launch_persistent_context.
    """
    if not proxy_ip_registry_enabled(account_cfg):
        return
    if not active_proxy or not active_proxy.get("host"):
        return

    change_url = get_change_ip_url()
    if not change_url:
        logger.warning(
            "[proxy_ip_registry] Нет URL смены IP: задайте MOBILEPROXY_CHANGE_IP_URL "
            "или MOBILEPROXY_API_KEY + MOBILEPROXY_PROXY_ID"
        )
        return

    lock_path = config.PROXY_IP_ROTATION_LOCK_FILE
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    with open(lock_path, "a+", encoding="utf-8") as lock_fh:
        portalocker.lock(lock_fh, portalocker.LOCK_EX)
        try:
            if mobileproxy_geo_enabled(account_cfg):
                iso = (account_cfg.get("country") or "").strip().upper()
                if len(iso) == 2:
                    from pipeline.mobileproxy_api import ensure_equipment_country_for_iso

                    ensure_equipment_country_for_iso(iso)
                    clear_proxy_session_caches()
                    _invalidate_browser_geo_cache(active_proxy)
            _ensure_under_lock(account_id, account_cfg, active_proxy, change_url)
        finally:
            portalocker.unlock(lock_fh)
            _invalidate_browser_geo_cache(active_proxy)


def _do_rotate(change_url: str, active_proxy: dict) -> None:
    ok_rot, _ = _rotate_once(change_url)
    if ok_rot:
        time.sleep(config.PROXY_IP_POST_ROTATE_PAUSE_SEC)
    _invalidate_browser_geo_cache(active_proxy)


def _ensure_under_lock(
    account_id: str,
    account_cfg: dict,
    active_proxy: dict,
    change_url: str,
) -> None:
    reg = _load_registry()
    accounts = reg.setdefault("accounts", {})
    remembered: Optional[str] = None
    row = accounts.get(account_id)
    if isinstance(row, dict):
        remembered = row.get("ip")
        if isinstance(remembered, str):
            remembered = remembered.strip() or None

    required_country = (account_cfg.get("country") or "").upper().strip()
    max_total = config.PROXY_IP_MAX_ROTATIONS
    max_sticky = config.PROXY_IP_MAX_STICKY_ATTEMPTS
    total_rotations = 0
    sticky_tries = 0
    dirty_ok_passes = 0

    def snapshot() -> tuple[Optional[str], bool]:
        ip = utils.fetch_exit_ip_via_proxy(active_proxy)
        return ip, _country_matches(account_cfg, ip)

    def maybe_rotate_if_spam() -> bool:
        """IPGuardian (proxy_ip&check_spam=true): при спаме — ротация и True."""
        nonlocal total_rotations
        if not getattr(config, "MOBILEPROXY_CHECK_SPAM", True):
            return False
        if not config.MOBILEPROXY_API_KEY or not config.MOBILEPROXY_PROXY_ID:
            return False
        from pipeline.mobileproxy_api import fetch_proxy_ip_with_spam_check, spam_check_requires_rotation

        data = fetch_proxy_ip_with_spam_check()
        if not spam_check_requires_rotation(data):
            return False
        logger.warning(
            "[proxy_ip_registry] %s: IP в спам-базе IPGuardian — ротация",
            account_id,
        )
        _do_rotate(change_url, active_proxy)
        total_rotations += 1
        return True

    while total_rotations < max_total:
        cur_ip, country_ok = snapshot()

        # 1) Запомненный exit и гео совпадают — готово
        if remembered and cur_ip == remembered and country_ok:
            if maybe_rotate_if_spam():
                continue
            logger.info(
                "[proxy_ip_registry] %s: активен запомненный IP %s",
                account_id,
                cur_ip,
            )
            return

        # 2) Первая привязка: нет истории — фиксируем любой чистый при корректной стране
        if remembered is None and cur_ip and country_ok:
            if _is_ip_clean_for_account(cur_ip, account_id, reg):
                if maybe_rotate_if_spam():
                    continue
                accounts[account_id] = {"ip": cur_ip}
                _save_registry(reg)
                logger.info(
                    "[proxy_ip_registry] %s: первый чистый IP %s",
                    account_id,
                    cur_ip,
                )
                return
            dirty_ok_passes += 1
            if dirty_ok_passes > max_sticky:
                logger.warning(
                    "[proxy_ip_registry] %s: крайний случай — первый IP %s не чистый, принимаем",
                    account_id,
                    cur_ip,
                )
                if maybe_rotate_if_spam():
                    continue
                accounts[account_id] = {"ip": cur_ip}
                _save_registry(reg)
                return
            _do_rotate(change_url, active_proxy)
            total_rotations += 1
            continue

        # 3) Страна не совпала — ротация до успеха (политика пользователя)
        if required_country and cur_ip and not country_ok:
            _do_rotate(change_url, active_proxy)
            total_rotations += 1
            continue

        # 4) Фаза sticky: вернуться к запомненному IP
        if remembered and sticky_tries < max_sticky and cur_ip != remembered:
            _do_rotate(change_url, active_proxy)
            total_rotations += 1
            sticky_tries += 1
            continue

        # 5) Страна ок — ищем чистый IP (или общий после многих попыток)
        if cur_ip and country_ok:
            if _is_ip_clean_for_account(cur_ip, account_id, reg):
                if maybe_rotate_if_spam():
                    continue
                accounts[account_id] = {"ip": cur_ip}
                _save_registry(reg)
                logger.info("[proxy_ip_registry] %s: закреплён чистый IP %s", account_id, cur_ip)
                return
            dirty_ok_passes += 1
            if dirty_ok_passes > max_sticky:
                logger.warning(
                    "[proxy_ip_registry] %s: крайний случай — IP %s занят другим аккаунтом, принимаем",
                    account_id,
                    cur_ip,
                )
                if maybe_rotate_if_spam():
                    continue
                accounts[account_id] = {"ip": cur_ip}
                _save_registry(reg)
                return
            _do_rotate(change_url, active_proxy)
            total_rotations += 1
            continue

        # 6) Нет IP или прочие случаи — крутим дальше
        _do_rotate(change_url, active_proxy)
        total_rotations += 1

    raise RuntimeError(
        f"[proxy_ip_registry] Исчерпан лимит ротаций ({max_total}) для аккаунта {account_id}"
    )
