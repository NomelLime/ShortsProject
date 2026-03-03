"""
notifications.py – Telegram-уведомления и обработка CAPTCHA / 2FA.
"""

import time
import logging
import requests
from rebrowser_playwright.sync_api import Page

from pipeline.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, CAPTCHA_WAIT_TIMEOUT_SEC

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# Telegram
# ──────────────────────────────────────────────────────────────

def send_telegram(message: str, parse_mode: str = "HTML") -> bool:
    """Отправляет сообщение в Telegram. Возвращает True при успехе."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram не настроен (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID пустые).")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": parse_mode},
            timeout=10,
        )
        ok = resp.status_code == 200
        if not ok:
            logger.warning(f"Telegram вернул {resp.status_code}: {resp.text[:200]}")
        return ok
    except Exception as e:
        logger.error(f"Ошибка отправки в Telegram: {e}")
        return False


def send_telegram_alert(message: str, parse_mode: str = "HTML") -> bool:
    """Отправляет предупреждение (аналог send_telegram, для совместимости)."""
    return send_telegram(message, parse_mode)


# ──────────────────────────────────────────────────────────────
# Детектор CAPTCHA / 2FA
# ──────────────────────────────────────────────────────────────

# ... (original remains)

# New: Used in uploader and finalize for expanded notifications