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
import os
import tempfile
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
# Хранилище — все операции чтение+изменение+запись под _lock
# ─────────────────────────────────────────────────────────────────────────────

def _load_unsafe() -> Dict:
    """Читает файл с диска БЕЗ блокировки. Вызывать только внутри with _lock."""
    if not _QUARANTINE_FILE.exists():
        return {}
    try:
        return json.loads(_QUARANTINE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_unsafe(data: Dict) -> None:
    """Атомично записывает данные на диск БЕЗ блокировки. Вызывать только внутри with _lock."""
    _QUARANTINE_FILE.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(data, ensure_ascii=False, indent=2)
    # Атомичная запись через temp-файл — защита от коррупции при OOM/Ctrl+C
    fd, tmp = tempfile.mkstemp(dir=_QUARANTINE_FILE.parent, suffix=".tmp")
    try:
        os.write(fd, text.encode("utf-8"))
        os.close(fd)
        os.replace(tmp, str(_QUARANTINE_FILE))
    except Exception:
        try:
            os.close(fd)
        except Exception:
            pass
        try:
            os.unlink(tmp)
        except Exception:
            pass
        raise


def _entry(data: Dict, acc_name: str, platform: str) -> Dict:
    """Возвращает запись (создаёт если нет). Вызывать внутри with _lock."""
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
    with _lock:
        data  = _load_unsafe()
        entry = data.get(acc_name, {}).get(platform)
        if not entry or not entry.get("until"):
            return False

        try:
            until = datetime.fromisoformat(entry["until"])
        except Exception:
            return False

        if datetime.now() >= until:
            # Карантин истёк — снимаем автоматически
            entry["until"]  = None
            entry["errors"] = 0
            _save_unsafe(data)
            logger.info("[quarantine] [%s][%s] Карантин истёк, снят автоматически.", acc_name, platform)
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
    notify_quarantine: Optional[tuple] = None

    with _lock:
        data  = _load_unsafe()
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

            _save_unsafe(data)
            logger.error(
                "[quarantine] [%s][%s] Введён карантин на %d ч (до %s). Причина: %s",
                acc_name, platform, hours, until.strftime("%H:%M"), reason,
            )
            notify_quarantine = (acc_name, platform, reason, hours, until, entry["total_quarantines"])
        else:
            _save_unsafe(data)

    # Telegram-уведомление вне lock (может блокировать I/O)
    if notify_quarantine:
        acc, plat, rsn, hrs, until, total = notify_quarantine
        send_telegram(
            f"🚫 <b>Карантин аккаунта</b>\n"
            f"  Аккаунт: <b>{acc}</b> | Платформа: <b>{plat}</b>\n"
            f"  Причина: {rsn}\n"
            f"  Пауза: <b>{hrs} ч</b> (до {until.strftime('%d.%m %H:%M')})\n"
            f"  Всего карантинов: {total}"
        )


def mark_success(acc_name: str, platform: str) -> None:
    """Сбрасывает счётчик ошибок после успешной загрузки."""
    with _lock:
        data  = _load_unsafe()
        entry = _entry(data, acc_name, platform)
        if entry["errors"] > 0:
            logger.debug("[quarantine] [%s][%s] Ошибки сброшены после успеха.", acc_name, platform)
            entry["errors"] = 0
            _save_unsafe(data)


def lift_quarantine(acc_name: str, platform: str) -> None:
    """Ручное снятие карантина."""
    with _lock:
        data  = _load_unsafe()
        entry = data.get(acc_name, {}).get(platform)
        if not entry:
            return
        entry["until"]  = None
        entry["errors"] = 0
        _save_unsafe(data)

    logger.info("[quarantine] [%s][%s] Карантин снят вручную.", acc_name, platform)
    send_telegram(f"✅ [{acc_name}][{platform}] Карантин снят вручную.")


def get_status() -> Dict:
    """Возвращает текущее состояние карантина всех аккаунтов."""
    with _lock:
        return _load_unsafe()
