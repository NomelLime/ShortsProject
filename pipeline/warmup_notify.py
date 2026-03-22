"""
Telegram при старте прогрева заливки и напоминание за N часов до окончания.

Состояние напоминаний: data/warmup_reminder_state.json (чтобы не спамить).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from pipeline import config

logger = logging.getLogger(__name__)

_REMINDER_STATE_FILE = config.BASE_DIR / "data" / "warmup_reminder_state.json"


def _load_reminder_state() -> Dict[str, Any]:
    if not _REMINDER_STATE_FILE.exists():
        return {"reminders_sent": {}}
    try:
        data = json.loads(_REMINDER_STATE_FILE.read_text(encoding="utf-8"))
        if "reminders_sent" not in data:
            data["reminders_sent"] = {}
        return data
    except Exception:
        return {"reminders_sent": {}}


def _save_reminder_state(data: Dict[str, Any]) -> None:
    try:
        _REMINDER_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _REMINDER_STATE_FILE.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.warning("[warmup_notify] не удалось сохранить state: %s", exc)


def notify_warmup_started(
    account_name: str,
    until_iso: str,
    days: int,
    platforms: List[str],
) -> None:
    """Один Telegram при старте окна прогрева."""
    try:
        from pipeline.notifications import send_telegram
    except Exception as exc:
        logger.debug("[warmup_notify] telegram import: %s", exc)
        return

    pl = ", ".join(platforms) if platforms else "—"
    msg = (
        f"🧊 <b>Прогрев заливки</b> <code>{account_name}</code>\n"
        f"Платформы: {pl}\n"
        f"Дней: {days}\n"
        f"Заливка с: <code>{until_iso}</code> (UTC)"
    )
    try:
        send_telegram(msg)
    except Exception as exc:
        logger.debug("[warmup_notify] send: %s", exc)


def scan_warmup_end_reminders() -> None:
    """
    Для активных прогревов: если до конца осталось ≤ UPLOAD_WARMUP_REMINDER_HOURS ч —
    отправить одно напоминание на пару (аккаунт, платформа, until).
    """
    hrs = getattr(config, "UPLOAD_WARMUP_REMINDER_HOURS", 24.0)
    if hrs <= 0:
        return

    try:
        from pipeline.notifications import send_telegram
    except Exception:
        return

    from pipeline.upload_warmup import load_account_config

    accounts_root = Path(config.ACCOUNTS_ROOT)
    if not accounts_root.exists():
        return

    state = _load_reminder_state()
    sent: Dict[str, str] = state.setdefault("reminders_sent", {})
    now = datetime.now(timezone.utc)
    changed = False
    warmup_json = "upload_warmup.json"

    # Удаляем устаревшие ключи (until уже в прошлом)
    keys_to_drop = []
    for key in list(sent.keys()):
        try:
            acc, plat, until_s = key.split("|", 2)
            until = datetime.fromisoformat(until_s)
            if until.tzinfo is None:
                until = until.replace(tzinfo=timezone.utc)
            if now >= until:
                keys_to_drop.append(key)
        except Exception:
            keys_to_drop.append(key)
    for k in keys_to_drop:
        del sent[k]
        changed = True

    for acc_dir in sorted(accounts_root.iterdir()):
        if not acc_dir.is_dir():
            continue
        wpath = acc_dir / warmup_json
        if not wpath.exists():
            continue
        acc_cfg = load_account_config(acc_dir)
        if acc_cfg.get("skip_upload_warmup") is True:
            continue
        try:
            wdata = json.loads(wpath.read_text(encoding="utf-8"))
        except Exception:
            continue
        for plat, info in (wdata.get("platforms") or {}).items():
            until_s = info.get("upload_allowed_after")
            if not until_s:
                continue
            try:
                until = datetime.fromisoformat(until_s)
                if until.tzinfo is None:
                    until = until.replace(tzinfo=timezone.utc)
            except Exception:
                continue
            if now >= until:
                continue
            hours_left = (until - now).total_seconds() / 3600.0
            if hours_left > hrs:
                continue
            key = f"{acc_dir.name}|{plat}|{until_s}"
            if key in sent:
                continue
            msg = (
                f"⏳ <b>Прогрев скоро закончится</b> <code>{acc_dir.name}</code> / {plat}\n"
                f"Заливка с: <code>{until_s}</code> UTC (~{hours_left:.1f} ч)"
            )
            try:
                send_telegram(msg)
                sent[key] = now.isoformat(timespec="seconds")
                changed = True
            except Exception as exc:
                logger.debug("[warmup_notify] reminder send: %s", exc)

    if changed:
        _save_reminder_state(state)
