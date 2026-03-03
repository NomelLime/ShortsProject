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

_CAPTCHA_INDICATORS = [
    "iframe[src*='recaptcha']",
    "iframe[src*='hcaptcha']",
    "iframe[src*='captcha']",
    "[data-sitekey]",
    "#captcha",
    ".g-recaptcha",
]

_2FA_INDICATORS = [
    "input[name='totpPin']",
    "input[autocomplete='one-time-code']",
    "#challenge",
    "[data-e2e='tiktok-verify']",
    "input[name='verificationCode']",
]

_POLL_INTERVAL_SEC = 5   # интервал проверки «решена ли задача»


def _is_captcha_visible(page: Page) -> bool:
    for sel in _CAPTCHA_INDICATORS:
        try:
            if page.locator(sel).count() > 0:
                return True
        except Exception:
            pass
    return False


def _is_2fa_visible(page: Page) -> bool:
    for sel in _2FA_INDICATORS:
        try:
            if page.locator(sel).count() > 0:
                return True
        except Exception:
            pass
    return False


def check_and_handle_captcha(
    page: Page,
    platform: str,
    account_name: str = "",
) -> None:
    """
    Проверяет наличие CAPTCHA или 2FA на странице.
    Если обнаружено — отправляет Telegram-уведомление и ждёт ручного решения.
    При превышении таймаута бросает TimeoutError.
    """
    has_captcha = _is_captcha_visible(page)
    has_2fa = _is_2fa_visible(page)

    if not has_captcha and not has_2fa:
        return

    challenge_type = "CAPTCHA" if has_captcha else "2FA / верификация"
    timeout_min = CAPTCHA_WAIT_TIMEOUT_SEC // 60

    msg = (
        f"🚨 <b>Требуется вмешательство!</b>\n"
        f"Аккаунт: <code>{account_name or 'неизвестен'}</code>\n"
        f"Платформа: <b>{platform.upper()}</b>\n"
        f"Тип: <b>{challenge_type}</b>\n\n"
        f"Откройте браузер и решите задачу вручную.\n"
        f"⏳ Ожидание: до <b>{timeout_min} мин.</b>"
    )
    logger.warning(
        f"[{platform}] Обнаружен {challenge_type}! "
        f"Ожидаю решения (макс. {timeout_min} мин.)…"
    )
    send_telegram(msg)

    deadline = time.time() + CAPTCHA_WAIT_TIMEOUT_SEC

    while time.time() < deadline:
        time.sleep(_POLL_INTERVAL_SEC)
        if not _is_captcha_visible(page) and not _is_2fa_visible(page):
            logger.info(f"[{platform}] {challenge_type} решена, продолжаю.")
            send_telegram(
                f"✅ <b>{challenge_type} решена</b> — "
                f"аккаунт <code>{account_name or platform}</code>, продолжаю работу."
            )
            return

    # Таймаут истёк
    send_telegram(
        f"⏰ <b>Таймаут!</b>\n"
        f"Аккаунт: <code>{account_name or platform}</code>\n"
        f"{challenge_type} не решена за {timeout_min} мин. — аккаунт пропущен."
    )
    raise TimeoutError(
        f"[{platform}] {challenge_type} не решена за {timeout_min} мин. "
        f"Аккаунт {account_name!r} пропущен."
    )