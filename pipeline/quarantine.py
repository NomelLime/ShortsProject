"""
pipeline/quarantine.py — Карантин аккаунтов.

Если аккаунт получает QUARANTINE_ERROR_THRESHOLD ошибок подряд —
он автоматически ставится на паузу на QUARANTINE_DURATION_HOURS часов.
Telegram-уведомление отправляется при входе в карантин и при выходе.

Структура data/quarantine.json:
  {
    "acc_name": {
      "youtube": {
        "errors":     3,
        "until":      "2024-01-15T16:00:00",   # null если не в карантине
        "reason":     "upload_failed × 3",
        "total_quarantines": 2
      }
    }
  }

Использование в uploader.py / upload_scheduler.py:
    from pipeline.quarantine import is_quarantined, mark_error, mark_success

    if is_quarantined(acc_name, platform):
        continue   # пропускаем аккаунт

    success = upload_video(...)
    if success:
        mark_success(acc_name, platform)
    else:
        mark_error(acc_name, platform, reason="upload_failed")
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Dict, Optional

from pipeline import config
from pipeline.notifications import send_telegram

logger = logging.getLogger(__name__)

_QUARANTINE_FILE = config.BASE_DIR / "data" / "quarantine.json"
_lock = Lock()


# ─────────────────────────────────────────────────────────────────────────────
# Хранилище
# ─────────────────────────────────────────────────────────────────────────────

def _load() -> Dict:
    if not _QUARANTINE_FILE.exists():
        return {}
    try:
        return json.loads(_QUARANTINE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save(data: Dict) -> None:
    with _lock:
        _QUARANTINE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _QUARANTINE_FILE.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )


def _entry(data: Dict, acc_name: str, platform: str) -> Dict:
    """Возвращает запись (создаёт если нет)."""
    data.setdefault(acc_name, {})
    data[acc_name].setdefault(platform, {
        "errors":            0,
        "until":             None,
        "reason":            "",
        "total_quarantines": 0,
    })
    return data[acc_name][platform]


# ─────────────────────────────────────────────────────────────────────────────
# Публичный API
# ─────────────────────────────────────────────────────────────────────────────

def is_quarantined(acc_name: str, platform: str) -> bool:
    """
    Возвращает True если аккаунт сейчас в карантине.
    Автоматически снимает карантин по истечении времени.
    """
    data  = _load()
    entry = data.get(acc_name, {}).get(platform)
    if not entry or not entry.get("until"):
        return False

    try:
        until = datetime.fromisoformat(entry["until"])
    except Exception:
        return False

    if datetime.now() >= until:
        # Карантин истёк — снимаем автоматически
        _lift(data, acc_name, platform, auto=True)
        return False

    remaining = until - datetime.now()
    logger.info(
        "[quarantine] [%s][%s] В карантине ещё %.0f мин (причина: %s)",
        acc_name, platform, remaining.total_seconds() / 60, entry.get("reason", "?"),
    )
    return True


def mark_error(acc_name: str, platform: str, reason: str = "upload_failed") -> None:
    """
    Фиксирует ошибку для аккаунта.
    После QUARANTINE_ERROR_THRESHOLD ошибок подряд — вводит карантин.
    """
    data  = _load()
    entry = _entry(data, acc_name, platform)

    entry["errors"] += 1
    entry["reason"]  = f"{reason} × {entry['errors']}"

    threshold = config.QUARANTINE_ERROR_THRESHOLD
    logger.warning(
        "[quarantine] [%s][%s] Ошибка %d/%d: %s",
        acc_name, platform, entry["errors"], threshold, reason,
    )

    if entry["errors"] >= threshold:
        hours = config.QUARANTINE_DURATION_HOURS
        until = datetime.now() + timedelta(hours=hours)
        entry["until"]             = until.isoformat(timespec="seconds")
        entry["total_quarantines"] = entry.get("total_quarantines", 0) + 1
        entry["errors"]            = 0  # сбрасываем счётчик

        _save(data)
        logger.error(
            "[quarantine] [%s][%s] Введён карантин на %d ч (до %s). Причина: %s",
            acc_name, platform, hours, until.strftime("%H:%M"), reason,
        )
        send_telegram(
            f"🚫 <b>Карантин аккаунта</b>\n"
            f"  Аккаунт: <b>{acc_name}</b> | Платформа: <b>{platform}</b>\n"
            f"  Причина: {reason}\n"
            f"  Пауза: <b>{hours} ч</b> (до {until.strftime('%d.%m %H:%M')})\n"
            f"  Всего карантинов: {entry['total_quarantines']}"
        )
    else:
        _save(data)


def mark_success(acc_name: str, platform: str) -> None:
    """Сбрасывает счётчик ошибок после успешной загрузки."""
    data  = _load()
    entry = _entry(data, acc_name, platform)
    if entry["errors"] > 0:
        logger.debug("[quarantine] [%s][%s] Ошибки сброшены после успеха.", acc_name, platform)
        entry["errors"] = 0
        _save(data)


def lift_quarantine(acc_name: str, platform: str) -> None:
    """Ручное снятие карантина."""
    data = _load()
    _lift(data, acc_name, platform, auto=False)


def _lift(data: Dict, acc_name: str, platform: str, auto: bool) -> None:
    entry = data.get(acc_name, {}).get(platform)
    if not entry:
        return
    entry["until"]  = None
    entry["errors"] = 0
    _save(data)
    tag = "автоматически" if auto else "вручную"
    logger.info("[quarantine] [%s][%s] Карантин снят (%s).", acc_name, platform, tag)
    if not auto:
        send_telegram(f"✅ [{acc_name}][{platform}] Карантин снят вручную.")


def get_status() -> Dict:
    """Возвращает текущее состояние карантина всех аккаунтов."""
    return _load()
