"""
notifications.py – Telegram-уведомления и обработка CAPTCHA / 2FA.
"""

import hashlib
import threading
import time
import logging
import requests
from rebrowser_playwright.sync_api import Page

from pipeline.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, CAPTCHA_WAIT_TIMEOUT_SEC

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# Rate limiter: защита от спама при массовых ошибках
# ──────────────────────────────────────────────────────────────
_tg_lock            = threading.Lock()
_tg_last_send_ts    = 0.0          # время последней отправки
_tg_min_interval    = 2.0          # минимум 2 секунды между сообщениями
_tg_dedup_cache: dict = {}         # hash → timestamp для дедупликации
_tg_dedup_window    = 300          # одно и то же сообщение не чаще 5 мин


# ──────────────────────────────────────────────────────────────
# Telegram
# ──────────────────────────────────────────────────────────────

def send_telegram(message: str, parse_mode: str = "HTML") -> bool:
    """
    Отправляет сообщение в Telegram с rate limiting.
    - Не чаще 1 сообщения в 2 сек (лимит Telegram API ~30/сек для ботов)
    - Дедупликация: одно и то же сообщение не чаще раза в 5 мин
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram не настроен (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID пустые).")
        return False

    with _tg_lock:
        global _tg_last_send_ts

        # Дедупликация по первым 200 символам (ключевой смысл)
        msg_hash = hashlib.md5(message[:200].encode()).hexdigest()
        now = time.monotonic()
        last_for_msg = _tg_dedup_cache.get(msg_hash, 0.0)
        if now - last_for_msg < _tg_dedup_window:
            logger.debug("Telegram: дубль сообщения пропущен (cooldown %ds)", _tg_dedup_window)
            return True

        # Rate limit: ждём если отправляли недавно
        wait = _tg_min_interval - (now - _tg_last_send_ts)
        if wait > 0:
            time.sleep(wait)

        _tg_last_send_ts = time.monotonic()
        _tg_dedup_cache[msg_hash] = _tg_last_send_ts

        # Чистим старые записи из кеша (старше 10 мин)
        cutoff = _tg_last_send_ts - 600
        expired = [k for k, v in _tg_dedup_cache.items() if v < cutoff]
        for k in expired:
            del _tg_dedup_cache[k]

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": parse_mode},
            timeout=10,
        )
        ok = resp.status_code == 200
        if not ok:
            logger.warning("Telegram вернул %d: %s", resp.status_code, resp.text[:200])
        return ok
    except Exception as e:
        logger.error("Ошибка отправки в Telegram: %s", e)
        return False


def send_telegram_alert(message: str, parse_mode: str = "HTML") -> bool:
    """Отправляет предупреждение (аналог send_telegram, для совместимости)."""
    return send_telegram(message, parse_mode)


# ──────────────────────────────────────────────────────────────
# Детектор CAPTCHA / 2FA
# ──────────────────────────────────────────────────────────────

def check_and_handle_captcha(page: Page, platform: str) -> bool:
    """
    Проверяет наличие CAPTCHA или 2FA на странице.
    При обнаружении отправляет уведомление в Telegram и ждёт ручного решения.
    Возвращает True если CAPTCHA была обнаружена и (предположительно) решена.
    """
    captcha_selectors = {
        "youtube":   ["#captcha-form", "iframe[src*='recaptcha']"],
        "tiktok":    ["#captcha_container", "div[class*='captcha']"],
        "instagram": ["form[id*='captcha']", "div[class*='captcha']"],
    }
    selectors = captcha_selectors.get(platform, [])

    for sel in selectors:
        try:
            if page.locator(sel).first.is_visible(timeout=2_000):
                msg = (
                    f"⚠️ CAPTCHA обнаружена!\n"
                    f"Платформа: {platform}\n"
                    f"URL: {page.url}\n"
                    f"Ожидаю ручного решения (макс. {CAPTCHA_WAIT_TIMEOUT_SEC // 60} мин)..."
                )
                logger.warning("[%s] CAPTCHA обнаружена на %s", platform, page.url)
                send_telegram_alert(msg)

                deadline = time.time() + CAPTCHA_WAIT_TIMEOUT_SEC
                while time.time() < deadline:
                    time.sleep(5)
                    try:
                        if not page.locator(sel).first.is_visible(timeout=1_000):
                            logger.info("[%s] CAPTCHA пройдена.", platform)
                            return True
                    except Exception:
                        return True  # элемент пропал — считаем что решена

                logger.error("[%s] CAPTCHA не решена за %d сек.", platform, CAPTCHA_WAIT_TIMEOUT_SEC)
                return True
        except Exception:
            continue

    return False