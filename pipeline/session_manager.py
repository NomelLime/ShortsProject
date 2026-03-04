"""
pipeline/session_manager.py — Управление сроком жизни сессий и авто-обновление cookies.

Проблема: cookies браузерного профиля протухают через ~24–72 ч в зависимости от
платформы. Если сессия истекла в момент загрузки — упадёт весь аккаунт.

Решение:
  1. session_health.json хранит метку последней успешной проверки сессии
     для каждой пары (account, platform).
  2. Перед каждой загрузкой (и в фоновом планировщике) вызывается
     ensure_session_fresh() — если сессия «старая» (> SESSION_MAX_AGE_HOURS),
     запускается принудительная проверка через check_session_valid().
  3. Если сессия невалидна — отправляется Telegram-уведомление и открывается
     страница логина для ручного обновления (с таймаутом).
  4. После успешной проверки/обновления метка времени сбрасывается.

Структура session_health.json:
  {
    "acc_name": {
      "youtube":   {"last_verified": "2024-01-15T10:30:00", "valid": true},
      "tiktok":    {"last_verified": "2024-01-14T08:00:00", "valid": false},
      ...
    }
  }
"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

from pipeline import config
from pipeline.notifications import send_telegram

logger = logging.getLogger(__name__)

_health_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# Хранилище session_health.json
# ─────────────────────────────────────────────────────────────────────────────

def _load_health() -> Dict:
    path = config.SESSION_HEALTH_FILE
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_health(data: Dict) -> None:
    with _health_lock:
        try:
            config.SESSION_HEALTH_FILE.parent.mkdir(parents=True, exist_ok=True)
            config.SESSION_HEALTH_FILE.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.warning("Не удалось сохранить session_health.json: %s", exc)


def mark_session_verified(account_name: str, platform: str, valid: bool = True) -> None:
    """Записывает факт успешной проверки сессии с текущей меткой времени."""
    health = _load_health()
    health.setdefault(account_name, {})
    health[account_name][platform] = {
        "last_verified": datetime.now().isoformat(timespec="seconds"),
        "valid": valid,
    }
    _save_health(health)
    logger.debug(
        "[session_manager] [%s][%s] Метка обновлена, valid=%s",
        account_name, platform, valid,
    )


def get_session_age_hours(account_name: str, platform: str) -> Optional[float]:
    """
    Возвращает возраст последней проверки сессии в часах.
    None — если запись отсутствует (сессия никогда не проверялась).
    """
    health = _load_health()
    entry = health.get(account_name, {}).get(platform)
    if not entry or "last_verified" not in entry:
        return None
    try:
        last = datetime.fromisoformat(entry["last_verified"])
        delta = datetime.now() - last
        return delta.total_seconds() / 3600
    except Exception:
        return None


def is_session_stale(account_name: str, platform: str) -> bool:
    """
    Возвращает True если сессия не проверялась дольше SESSION_MAX_AGE_HOURS
    или если запись отсутствует.
    """
    age = get_session_age_hours(account_name, platform)
    if age is None:
        return True  # никогда не проверялась — считаем устаревшей
    return age >= config.SESSION_MAX_AGE_HOURS


# ─────────────────────────────────────────────────────────────────────────────
# Основная логика проверки и обновления
# ─────────────────────────────────────────────────────────────────────────────

def ensure_session_fresh(
    context,          # BrowserContext из Playwright
    account_name: str,
    platform: str,
    force_check: bool = False,
) -> bool:
    """
    Проверяет свежесть сессии и при необходимости запускает обновление.

    Алгоритм:
      1. Если сессия свежая (< SESSION_MAX_AGE_HOURS) и force_check=False — пропускаем.
      2. Иначе — проверяем через check_session_valid().
      3. Если сессия невалидна — отправляем Telegram + открываем страницу логина
         и ждём ручного ввода (с таймаутом CAPTCHA_WAIT_TIMEOUT_SEC).
      4. Повторно проверяем сессию и обновляем health-файл.

    Возвращает True если сессия валидна, False если обновить не удалось.
    """
    # Импортируем здесь чтобы избежать циклического импорта (browser -> session_manager -> browser)
    from pipeline.browser import check_session_valid

    age = get_session_age_hours(account_name, platform)

    # Предупреждение если сессия приближается к истечению
    if age is not None and age >= config.SESSION_REFRESH_WARN_HOURS and not is_session_stale(account_name, platform):
        logger.warning(
            "[session_manager] [%s][%s] Сессия приближается к истечению (возраст: %.1f ч / лимит: %d ч).",
            account_name, platform, age, config.SESSION_MAX_AGE_HOURS,
        )
        send_telegram(
            f"⏰ [{account_name}][{platform}] Сессия скоро истечёт "
            f"(возраст: {age:.1f} ч). Следующая загрузка выполнит проверку автоматически."
        )

    if not is_session_stale(account_name, platform) and not force_check:
        logger.debug(
            "[session_manager] [%s][%s] Сессия свежая (%.1f ч) — проверка не нужна.",
            account_name, platform, age,
        )
        return True

    logger.info(
        "[session_manager] [%s][%s] Проверяем сессию (возраст: %s ч)...",
        account_name, platform,
        f"{age:.1f}" if age is not None else "неизвестен",
    )

    session_status = check_session_valid(context, [platform])
    is_valid = session_status.get(platform, False)

    if is_valid:
        mark_session_verified(account_name, platform, valid=True)
        logger.info("[session_manager] [%s][%s] Сессия валидна.", account_name, platform)
        return True

    # Сессия невалидна — запускаем процедуру обновления
    logger.warning("[session_manager] [%s][%s] Сессия истекла — требуется повторный вход.", account_name, platform)
    send_telegram(
        f"🔐 [{account_name}][{platform}] Сессия истекла. "
        f"Открываю страницу входа — войдите и нажмите ENTER в терминале.\n"
        f"Таймаут: {config.CAPTCHA_WAIT_TIMEOUT_SEC // 60} мин."
    )

    login_urls = {
        "youtube":   "https://accounts.google.com/ServiceLogin",
        "tiktok":    "https://www.tiktok.com/login",
        "instagram": "https://www.instagram.com/accounts/login/",
    }

    try:
        login_page = context.new_page()
        login_page.goto(login_urls.get(platform, "https://google.com"))
    except Exception as exc:
        logger.error("[session_manager] Не удалось открыть страницу логина: %s", exc)
        mark_session_verified(account_name, platform, valid=False)
        return False

    logged_in_event = threading.Event()

    def _wait_input() -> None:
        try:
            input(f"\n  >>> [{account_name}][{platform}] Войдите в аккаунт и нажмите ENTER: ")
        except (EOFError, KeyboardInterrupt):
            pass
        logged_in_event.set()

    t = threading.Thread(target=_wait_input, daemon=True)
    t.start()
    t.join(timeout=config.CAPTCHA_WAIT_TIMEOUT_SEC)

    try:
        login_page.close()
    except Exception:
        pass

    # Финальная проверка после ручного входа
    session_status = check_session_valid(context, [platform])
    is_valid = session_status.get(platform, False)
    mark_session_verified(account_name, platform, valid=is_valid)

    if is_valid:
        logger.info("[session_manager] [%s][%s] Сессия успешно обновлена.", account_name, platform)
        send_telegram(f"✅ [{account_name}][{platform}] Сессия обновлена, загрузка продолжается.")
    else:
        logger.error("[session_manager] [%s][%s] Сессию обновить не удалось.", account_name, platform)
        send_telegram(f"❌ [{account_name}][{platform}] Не удалось обновить сессию — аккаунт пропущен.")

    return is_valid


# ─────────────────────────────────────────────────────────────────────────────
# Фоновая профилактическая проверка
# ─────────────────────────────────────────────────────────────────────────────

class SessionHealthMonitor:
    """
    Фоновый монитор здоровья сессий.

    Периодически проверяет все аккаунты и отправляет предупреждение в Telegram,
    если сессия приближается к истечению (> SESSION_REFRESH_WARN_HOURS).
    Реальное обновление через браузер выполняется при следующем запуске загрузки.

    Используется как контекстный менеджер или явно:
        monitor = SessionHealthMonitor()
        monitor.start()
        ...
        monitor.stop()
    """

    CHECK_INTERVAL_SEC = 3600  # проверяем раз в час

    def __init__(self) -> None:
        self._timer: Optional[threading.Timer] = None
        self._running = False

    def start(self) -> None:
        self._running = True
        self._schedule()
        logger.info("[session_monitor] Запущен (интервал: %d мин).", self.CHECK_INTERVAL_SEC // 60)

    def stop(self) -> None:
        self._running = False
        if self._timer:
            self._timer.cancel()
        logger.info("[session_monitor] Остановлен.")

    def __enter__(self) -> "SessionHealthMonitor":
        self.start()
        return self

    def __exit__(self, *_) -> None:
        self.stop()

    def _schedule(self) -> None:
        if not self._running:
            return
        self._timer = threading.Timer(self.CHECK_INTERVAL_SEC, self._run)
        self._timer.daemon = True
        self._timer.start()

    def _run(self) -> None:
        self._check_all_accounts()
        self._schedule()

    def _check_all_accounts(self) -> None:
        from pipeline import utils
        accounts = utils.get_all_accounts()
        health = _load_health()

        warnings: list[str] = []

        for account in accounts:
            acc_name  = account["name"]
            platforms = account["platforms"]
            for platform in platforms:
                entry = health.get(acc_name, {}).get(platform)
                if not entry:
                    continue  # не логинились ни разу — нет данных для предупреждения
                age = get_session_age_hours(acc_name, platform)
                if age is None:
                    continue
                if age >= config.SESSION_REFRESH_WARN_HOURS:
                    warnings.append(
                        f"  • [{acc_name}][{platform}] — возраст сессии: {age:.1f} ч"
                    )

        if warnings:
            msg = (
                "⏰ <b>Сессии требуют обновления:</b>\n"
                + "\n".join(warnings)
                + f"\n\nОбновление произойдёт автоматически при следующей загрузке."
            )
            logger.warning("[session_monitor] Устаревшие сессии:\n%s", "\n".join(warnings))
            send_telegram(msg)
