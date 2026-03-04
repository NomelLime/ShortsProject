"""
pipeline/upload_scheduler.py — Планировщик публикаций по расписанию.

Позволяет задать время загрузки для каждого аккаунта в config.json:

  {
    "platforms": ["youtube", "tiktok"],
    "upload_schedule": {
      "youtube":   ["09:00", "19:00"],
      "tiktok":    ["10:00", "20:00"],
      "instagram": ["11:00", "21:00"]
    }
  }

Если upload_schedule не задан — используется глобальное расписание
из переменной окружения UPLOAD_TIMES (формат "09:00,19:00")
или дефолтное из DEFAULT_UPLOAD_TIMES в config.

Запуск (вместо run_pipeline.py для непрерывной работы):

    python run_scheduled.py

Этот файл только отвечает за тайминг. Когда наступает нужное время —
запускает полный пайплайн (или только этап загрузки).

Интеграция с фоновыми процессами:
  - ActivityScheduler (scheduler.py) — независимый, управляет прогревом
  - SessionHealthMonitor (session_manager.py) — независимый, следит за cookies
  - UploadScheduler (этот файл) — управляет временем публикации
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from pipeline import config, utils
from pipeline.notifications import send_telegram

logger = logging.getLogger(__name__)

# Дефолтные времена публикации если не заданы в конфиге аккаунта
# Можно переопределить через env: UPLOAD_TIMES="09:00,12:00,19:00"
_DEFAULT_UPLOAD_TIMES_STR = os.getenv("UPLOAD_TIMES", "09:00,19:00")
DEFAULT_UPLOAD_TIMES: List[str] = [
    t.strip() for t in _DEFAULT_UPLOAD_TIMES_STR.split(",") if t.strip()
]


# ─────────────────────────────────────────────────────────────────────────────
# Утилиты
# ─────────────────────────────────────────────────────────────────────────────

def _parse_time(time_str: str) -> Optional[Tuple[int, int]]:
    """Парсит строку "HH:MM" → (hour, minute) или None при ошибке."""
    try:
        parts = time_str.strip().split(":")
        return int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        logger.warning("[upload_scheduler] Невалидное время: %r", time_str)
        return None


def _seconds_until(hour: int, minute: int) -> float:
    """Возвращает секунды до следующего наступления HH:MM (сегодня или завтра)."""
    now = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def _next_upload_delay(times: List[str], jitter_sec: int = 120) -> Tuple[float, str]:
    """
    Вычисляет задержку до ближайшего времени из списка.
    Добавляет случайный jitter ±jitter_sec для натуральности.
    Возвращает (delay_seconds, time_str).
    """
    parsed = [_parse_time(t) for t in times]
    parsed = [(h, m, t) for (h, m), t in zip(  # type: ignore[misc]
        [p for p in parsed if p], times
    ) if _parse_time(t)]

    if not parsed:
        return 3600.0, "??:??"  # fallback — раз в час

    best_delay = None
    best_time  = None

    for h, m, t in parsed:
        delay = _seconds_until(h, m)
        if best_delay is None or delay < best_delay:
            best_delay = delay
            best_time  = t

    jitter = random.uniform(-jitter_sec, jitter_sec)
    return max(60.0, best_delay + jitter), best_time


def get_account_upload_times(account_cfg: Dict, platform: str) -> List[str]:
    """
    Возвращает список времён загрузки для аккаунта и платформы.
    Приоритет: account_cfg["upload_schedule"][platform] > DEFAULT_UPLOAD_TIMES
    """
    schedule = account_cfg.get("upload_schedule", {})
    if isinstance(schedule, dict) and platform in schedule:
        times = schedule[platform]
        if isinstance(times, list) and times:
            return times
    # Глобальное расписание из аккаунта (для всех платформ)
    if isinstance(schedule, list) and schedule:
        return schedule
    return DEFAULT_UPLOAD_TIMES


# ─────────────────────────────────────────────────────────────────────────────
# Job на один аккаунт × платформа
# ─────────────────────────────────────────────────────────────────────────────

class _UploadJob:
    """
    Запускает загрузку для одного аккаунта / платформы в нужное время.
    После выполнения сам себя перепланирует на следующий слот.
    """

    def __init__(self, account: Dict, platform: str) -> None:
        self._account  = account
        self._platform = platform
        self._timer: Optional[threading.Timer] = None
        self._running  = False
        self._lock     = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._running = True
        self._schedule_next()

    def stop(self) -> None:
        with self._lock:
            self._running = False
            if self._timer:
                self._timer.cancel()
                self._timer = None

    def _schedule_next(self) -> None:
        times = get_account_upload_times(self._account["config"], self._platform)
        delay, time_str = _next_upload_delay(times)

        logger.info(
            "[upload_scheduler] [%s][%s] Следующая загрузка в ~%s (через %.0f мин)",
            self._account["name"], self._platform, time_str, delay / 60,
        )

        with self._lock:
            if not self._running:
                return
            self._timer = threading.Timer(delay, self._run)
            self._timer.daemon = True
            self._timer.start()

    def _run(self) -> None:
        acc_name  = self._account["name"]
        platform  = self._platform

        logger.info(
            "[upload_scheduler] ⏰ Время публикации! [%s][%s]",
            acc_name, platform,
        )
        send_telegram(f"⏰ <b>[{acc_name}][{platform}]</b> Запуск загрузки по расписанию.")

        try:
            # Запускаем только этап загрузки для конкретного аккаунта и платформы
            _run_upload_for(self._account, platform)
        except Exception as exc:
            logger.error(
                "[upload_scheduler] [%s][%s] Ошибка при загрузке: %s",
                acc_name, platform, exc, exc_info=True,
            )
            send_telegram(
                f"❌ [{acc_name}][{platform}] Ошибка загрузки по расписанию: {str(exc)[:200]}"
            )

        # Перепланируем на следующий слот
        with self._lock:
            if self._running:
                self._schedule_next()


def _run_upload_for(account: Dict, platform: str) -> None:
    """
    Запускает загрузку очереди для конкретного аккаунта и платформы.
    Использует существующую логику из uploader.py без запуска полного пайплайна.
    """
    # Ленивый импорт чтобы избежать циклических зависимостей
    from pipeline.browser import launch_browser, close_browser
    from pipeline.uploader import upload_video, clean_video_metadata
    from pipeline.activity import run_activity
    from pipeline.session_manager import ensure_session_fresh, mark_session_verified
    from pipeline.analytics import register_upload
    from pathlib import Path

    acc_name    = account["name"]
    acc_dir     = account["dir"]
    acc_cfg     = account["config"]
    daily_limit = config.PLATFORM_DAILY_LIMITS.get(platform, config.DAILY_UPLOAD_LIMIT)
    queue       = utils.get_upload_queue(acc_dir, platform)

    if not queue:
        logger.info("[upload_scheduler] [%s][%s] Очередь пуста — пропуск.", acc_name, platform)
        return

    if utils.get_uploads_today(acc_dir) >= daily_limit:
        logger.info(
            "[upload_scheduler] [%s][%s] Дневной лимит исчерпан.", acc_name, platform
        )
        return

    profile_dir = acc_dir / "browser_profile"

    try:
        pw, context = launch_browser(acc_cfg, profile_dir)
    except RuntimeError as exc:
        logger.error("[upload_scheduler] [%s] Прокси недоступен: %s", acc_name, exc)
        send_telegram(f"⚠️ [{acc_name}][{platform}] Прокси недоступен — загрузка пропущена.")
        return

    try:
        if not ensure_session_fresh(context, acc_name, platform):
            logger.error("[upload_scheduler] [%s][%s] Сессия невалидна.", acc_name, platform)
            return

        mark_session_verified(acc_name, platform, valid=True)
        run_activity(context, platform, queue[0].get("meta", {}))

        for item in queue:
            if utils.get_uploads_today(acc_dir) >= daily_limit:
                break

            video_path = item["video_path"]
            meta       = item["meta"]
            clean_path = clean_video_metadata(video_path)

            success = upload_video(
                context, platform, clean_path, meta,
                account_name=acc_name, account_cfg=acc_cfg,
            )

            if success:
                utils.mark_uploaded(item)
                utils.increment_upload_count(acc_dir)
                register_upload(
                    video_stem=Path(video_path).stem,
                    platform=platform,
                    video_url="",
                    meta=meta,
                )
                logger.info(
                    "[upload_scheduler] ✅ [%s][%s] Загружено: %s",
                    acc_name, platform, video_path.name,
                )
    finally:
        close_browser(pw, context)


# ─────────────────────────────────────────────────────────────────────────────
# Публичный планировщик
# ─────────────────────────────────────────────────────────────────────────────

class UploadScheduler:
    """
    Планировщик загрузок по расписанию для всех аккаунтов.

    Создаёт по одному _UploadJob на каждую пару (аккаунт, платформа).
    Каждый job самостоятельно вычисляет время до следующего слота
    из config.json аккаунта или из DEFAULT_UPLOAD_TIMES.

    Использование как контекстный менеджер (в run_scheduled.py):
        with UploadScheduler():
            # держим процесс живым
            while True:
                time.sleep(60)

    Или явно:
        s = UploadScheduler()
        s.start()
        ...
        s.stop()
    """

    def __init__(self) -> None:
        self._jobs: List[_UploadJob] = []
        self._started = False

    def start(self) -> None:
        if self._started:
            return

        accounts = utils.get_all_accounts()
        if not accounts:
            logger.warning("[upload_scheduler] Аккаунты не найдены — планировщик не запущен.")
            return

        for account in accounts:
            for platform in account["platforms"]:
                job = _UploadJob(account=account, platform=platform)
                self._jobs.append(job)
                job.start()

        self._started = True
        logger.info(
            "[upload_scheduler] Запущено %d job(s) для %d аккаунтов. "
            "Дефолтные времена: %s",
            len(self._jobs), len(accounts), ", ".join(DEFAULT_UPLOAD_TIMES),
        )
        send_telegram(
            f"🗓 Планировщик загрузок запущен.\n"
            f"Аккаунтов: {len(accounts)} | Job-ов: {len(self._jobs)}\n"
            f"Дефолтные слоты: {', '.join(DEFAULT_UPLOAD_TIMES)}"
        )

    def stop(self) -> None:
        for job in self._jobs:
            job.stop()
        self._jobs.clear()
        self._started = False
        logger.info("[upload_scheduler] Остановлен.")

    def __enter__(self) -> "UploadScheduler":
        self.start()
        return self

    def __exit__(self, *_) -> None:
        self.stop()
