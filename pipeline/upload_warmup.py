"""
Прогрев аккаунта: после первой успешной верификации сессии на платформе
загрузка видео блокируется на 3–5 дней (настраивается). Остальной пайплайн
(активность по расписанию, скачивание и т.д.) не затрагиется.

Состояние: accounts/<name>/upload_warmup.json
Отключение: в config.json аккаунта — "skip_upload_warmup": true

Интеграция: distributor не кладёт файлы в очередь прогревающихся слотов;
удаление из OUTPUT и архивирование в finalize учитывают платформы, где
все аккаунты в прогреве (см. all_accounts_warmup_for_platform).
"""

from __future__ import annotations

import json
import logging
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Set, Tuple

from pipeline import config

logger = logging.getLogger(__name__)

WARMUP_FILENAME = "upload_warmup.json"


def load_account_config(acc_dir: Path) -> Dict[str, Any]:
    """Читает config.json аккаунта; при ошибке — пустой dict."""
    path = acc_dir / "config.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _warmup_path(acc_dir: Path) -> Path:
    return acc_dir / WARMUP_FILENAME


def _load_warmup(acc_dir: Path) -> Dict[str, Any]:
    path = _warmup_path(acc_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_warmup(acc_dir: Path, data: Dict[str, Any]) -> None:
    path = _warmup_path(acc_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def warmup_scope_for_account(acc_cfg: Dict[str, Any]) -> str:
    """
    «platform» — отдельное окно прогрева на каждую сеть.
    «account» — одно окно на все платформы аккаунта (первый валидный логин).
    """
    s = acc_cfg.get("upload_warmup_scope")
    if s is not None:
        v = str(s).strip().lower()
        if v in ("account", "platform"):
            return v
    g = getattr(config, "UPLOAD_WARMUP_DEFAULT_SCOPE", "platform")
    return g if g in ("account", "platform") else "platform"


def ensure_warmup_started(acc_dir: Path, platform: str, acc_cfg: Dict[str, Any]) -> None:
    """
    При первом valid=True задаёт дату начала заливки.
    Режим scope: см. warmup_scope_for_account() и UPLOAD_WARMUP_DEFAULT_SCOPE.
    """
    if not getattr(config, "UPLOAD_WARMUP_ENABLED", True):
        return
    if acc_cfg.get("skip_upload_warmup") is True:
        return

    min_d = max(1, int(getattr(config, "UPLOAD_WARMUP_MIN_DAYS", 3)))
    max_d = max(min_d, int(getattr(config, "UPLOAD_WARMUP_MAX_DAYS", 5)))
    scope = warmup_scope_for_account(acc_cfg)

    data = _load_warmup(acc_dir)
    platforms_map = data.setdefault("platforms", {})

    if scope == "account":
        if any(info.get("upload_allowed_after") for info in platforms_map.values()):
            return
        pl_list = acc_cfg.get("platforms", [platform])
        if isinstance(pl_list, str):
            pl_list = [pl_list]
        if not pl_list:
            pl_list = [platform]
        days = random.randint(min_d, max_d)
        now = datetime.now(timezone.utc)
        until = now + timedelta(days=days)
        until_s = until.isoformat(timespec="seconds")
        entry = {
            "first_session_ok_at": now.isoformat(timespec="seconds"),
            "upload_allowed_after": until_s,
            "warmup_days": days,
            "scope": "account",
        }
        for p in pl_list:
            platforms_map[p] = dict(entry)
        _save_warmup(acc_dir, data)
        logger.info(
            "[upload_warmup] %s (account scope): прогрев %d д. для [%s] — заливка с %s (UTC)",
            acc_dir.name,
            days,
            ", ".join(pl_list),
            until.strftime("%Y-%m-%d %H:%M"),
        )
        try:
            from pipeline.warmup_notify import notify_warmup_started

            notify_warmup_started(acc_dir.name, until_s, days, list(pl_list))
        except Exception as exc:
            logger.debug("[upload_warmup] notify: %s", exc)
        return

    # --- platform scope ---
    if platform in platforms_map and platforms_map[platform].get("upload_allowed_after"):
        return

    days = random.randint(min_d, max_d)
    now = datetime.now(timezone.utc)
    until = now + timedelta(days=days)
    until_s = until.isoformat(timespec="seconds")
    platforms_map[platform] = {
        "first_session_ok_at": now.isoformat(timespec="seconds"),
        "upload_allowed_after": until_s,
        "warmup_days": days,
        "scope": "platform",
    }
    _save_warmup(acc_dir, data)
    logger.info(
        "[upload_warmup] %s/%s: старт прогрева на %d д. — заливка с %s (UTC)",
        acc_dir.name,
        platform,
        days,
        until.strftime("%Y-%m-%d %H:%M"),
    )
    try:
        from pipeline.warmup_notify import notify_warmup_started

        notify_warmup_started(acc_dir.name, until_s, days, [platform])
    except Exception as exc:
        logger.debug("[upload_warmup] notify: %s", exc)


def is_upload_warmup_active(acc_dir: Path, platform: str, acc_cfg: Dict[str, Any]) -> Tuple[bool, str]:
    """
    True если заливка по этой платформе ещё в «морозилке».
    """
    if not getattr(config, "UPLOAD_WARMUP_ENABLED", True):
        return False, ""
    if acc_cfg.get("skip_upload_warmup") is True:
        return False, ""

    plat = _load_warmup(acc_dir).get("platforms", {}).get(platform)
    if not plat:
        return False, ""

    until_raw = plat.get("upload_allowed_after")
    if not until_raw:
        return False, ""

    try:
        until = datetime.fromisoformat(until_raw)
        if until.tzinfo is None:
            until = until.replace(tzinfo=timezone.utc)
    except Exception:
        return False, ""

    now = datetime.now(timezone.utc)
    if now < until:
        return True, f"прогрев до {until_raw} (UTC)"
    return False, ""


def is_upload_blocked(account_name: str, platform: str) -> Tuple[bool, str]:
    """Проверка по имени аккаунта (для Publisher / Guardian без acc_dir)."""
    acc_dir = Path(config.ACCOUNTS_ROOT) / account_name
    if not acc_dir.is_dir():
        return False, ""
    cfg = load_account_config(acc_dir)
    return is_upload_warmup_active(acc_dir, platform, cfg)


def tracking_stem_ready_for_archive(platforms_map: Dict[str, bool], required: Set[str]) -> bool:
    """
    Достаточно ли трекинга для архивирования исходника:
    хотя бы одна успешная заливка (True в map) и по каждой обязательной
    платформе — либо залито, либо все аккаунты с платформой в прогреве.
    """
    if not platforms_map or not any(platforms_map.values()):
        return False
    for p in required:
        if platforms_map.get(p, False):
            continue
        if all_accounts_warmup_for_platform(p):
            continue
        return False
    return True


def all_accounts_warmup_for_platform(platform: str) -> bool:
    """
    True, если платформа указана хотя бы у одного аккаунта и все такие аккаунты
    сейчас в прогреве заливки. Тогда дистрибьютор может убрать ролик из OUTPUT
    без копии в очередь на эту платформу, а finalize — заархивировать исходник
    без фактической заливки на неё.

    Если прогрев глобально выключен или ни у кого нет этой платформы — False.
    """
    if not getattr(config, "UPLOAD_WARMUP_ENABLED", True):
        return False

    accounts_root = Path(config.ACCOUNTS_ROOT)
    if not accounts_root.exists():
        return False

    found = False
    for acc_dir in accounts_root.iterdir():
        if not acc_dir.is_dir():
            continue
        cfg_path = acc_dir / "config.json"
        if not cfg_path.exists():
            continue
        acc_cfg = load_account_config(acc_dir)
        platforms = acc_cfg.get("platforms", [acc_cfg.get("platform", "youtube")])
        if isinstance(platforms, str):
            platforms = [platforms]
        if platform not in platforms:
            continue
        found = True
        active, _ = is_upload_warmup_active(acc_dir, platform, acc_cfg)
        if not active:
            return False
    return found
