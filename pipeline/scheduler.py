"""
pipeline/scheduler.py — Фоновый планировщик активности аккаунтов.

Запускает симуляцию активности (activity.py) для каждого аккаунта
независимо от цикла загрузки видео. Это решает проблему масштабирования:
при 10 аккаунтах × 3 платформы старая схема тратила 2–7 часов на прогрев
перед каждой загрузкой.

Схема работы:
  - Каждый аккаунт получает свой job в APScheduler с интервалом
    ACTIVITY_SCHEDULER_INTERVAL_MIN ± jitter (по умолчанию 90 мин ± 5 мин).
  - Активность запускается в отдельном потоке, не блокируя основной пайплайн.
  - Планировщик запускается один раз при старте run_pipeline.py и работает
    фоном всё время выполнения пайплайна.

Интеграция в run_pipeline.py:
    from pipeline.scheduler import ActivityScheduler
    scheduler = ActivityScheduler()
    scheduler.start()
    # ... основной пайплайн ...
    scheduler.stop()

Или как контекстный менеджер:
    with ActivityScheduler() as scheduler:
        run_pipeline()
"""

from __future__ import annotations

import logging
import random
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from pipeline import config, utils
from pipeline.browser import launch_browser, close_browser
from pipeline.activity_vl import run_activity_vl

logger = logging.getLogger(__name__)

# Семафор: ограничивает число одновременных VL-сессий
# При превышении — job переносится на +10 мин вместо блокировки потока
_vl_semaphore = threading.Semaphore(config.ACTIVITY_VL_CONCURRENCY)

# Задержка переноса при занятом GPU-семафоре (сек)
_GPU_BUSY_RESCHEDULE_SEC = 600  # 10 мин


def _in_activity_window() -> bool:
    """
    Проверяет, попадает ли текущее время в разрешённое окно активности.
    Использует локальное время машины (аккаунты привязаны к той же ТЗ что и сервер).
    """
    hour = datetime.now().hour
    return config.ACTIVITY_HOURS_START <= hour < config.ACTIVITY_HOURS_END


# ─────────────────────────────────────────────────────────────────────────────
# Внутренний простой планировщик на threading.Timer
# (не требует apscheduler — работает на стандартной библиотеке)
# ─────────────────────────────────────────────────────────────────────────────

class _AccountActivityJob:
    """
    Периодически запускает активность для одного аккаунта на одной платформе.
    Использует threading.Timer для перезапуска с интервалом + jitter.
    """

    def __init__(
        self,
        account: Dict,
        platform: str,
        interval_sec: int,
        jitter_sec: int,
    ) -> None:
        self._account      = account
        self._platform     = platform
        self._interval_sec = interval_sec
        self._jitter_sec   = jitter_sec
        self._timer: threading.Timer | None = None
        self._running      = False
        self._lock         = threading.Lock()

    def _next_delay(self) -> float:
        jitter = random.uniform(-self._jitter_sec, self._jitter_sec)
        base = max(60.0, self._interval_sec + jitter)
        try:
            from pathlib import Path

            from pipeline.upload_warmup import is_upload_warmup_active

            acc_dir = Path(self._account["dir"])
            warm, _ = is_upload_warmup_active(
                acc_dir, self._platform, self._account.get("config", {}),
            )
            imult = float(getattr(config, "ACTIVITY_WARMUP_INTERVAL_MULT", 1.0) or 1.0)
            if warm and imult > 1.0:
                base *= imult
        except Exception:
            pass
        return base

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._running = True
        # Первый запуск — со случайным сдвигом, чтобы аккаунты не стартовали одновременно
        initial_delay = random.uniform(10, min(300, self._interval_sec // 2))
        self._schedule(initial_delay)
        logger.info(
            "[scheduler] Активность для [%s][%s] запланирована через %.0f сек.",
            self._account["name"], self._platform, initial_delay,
        )

    def stop(self) -> None:
        with self._lock:
            self._running = False
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None

    def _schedule(self, delay: float) -> None:
        with self._lock:
            if not self._running:
                return
            self._timer = threading.Timer(delay, self._run)
            self._timer.daemon = True
            self._timer.start()

    def _run(self) -> None:
        acc_name    = self._account["name"]
        platform    = self._platform
        acc_cfg     = self._account["config"]
        acc_dir     = self._account["dir"]
        profile_dir = acc_dir / "browser_profile"

        # Проверка временно́го окна: не работаем ночью
        if not _in_activity_window():
            next_hour = config.ACTIVITY_HOURS_START
            now = datetime.now()
            # Задержка до начала следующего активного окна
            minutes_to_window = ((next_hour - now.hour) % 24) * 60 - now.minute
            delay = max(60, minutes_to_window * 60)
            logger.info(
                "[scheduler] [%s][%s] Вне окна активности (%02d:00–%02d:00) — "
                "перенос на %.0f мин",
                acc_name, platform,
                config.ACTIVITY_HOURS_START, config.ACTIVITY_HOURS_END,
                delay / 60,
            )
            self._schedule(delay)
            return

        # Проверка VL-семафора: не блокируем поток, переносим если все слоты заняты
        acquired = _vl_semaphore.acquire(blocking=False)
        if not acquired:
            logger.info(
                "[scheduler] [%s][%s] VL слоты заняты (%d конкурентных сессий) — "
                "перенос на %d мин",
                acc_name, platform,
                config.ACTIVITY_VL_CONCURRENCY, _GPU_BUSY_RESCHEDULE_SEC // 60,
            )
            self._schedule(_GPU_BUSY_RESCHEDULE_SEC)
            return

        logger.info("[scheduler] Запуск активности: [%s][%s]", acc_name, platform)
        try:
            pw, context = launch_browser(acc_cfg, profile_dir)
            try:
                run_activity_vl(context, platform, self._account)
            finally:
                close_browser(pw, context)
            logger.info("[scheduler] Активность завершена: [%s][%s]", acc_name, platform)
        except RuntimeError as proxy_err:
            logger.warning(
                "[scheduler] Прокси недоступен для [%s] — активность пропущена: %s",
                acc_name, proxy_err,
            )
        except Exception as exc:
            logger.error(
                "[scheduler] Ошибка активности [%s][%s]: %s",
                acc_name, platform, exc,
            )
        finally:
            _vl_semaphore.release()

        # Планируем следующий запуск
        next_delay = self._next_delay()
        logger.debug(
            "[scheduler] Следующая активность [%s][%s] через %.0f сек.",
            acc_name, platform, next_delay,
        )
        self._schedule(next_delay)


# ─────────────────────────────────────────────────────────────────────────────
# Публичный планировщик
# ─────────────────────────────────────────────────────────────────────────────

class ActivityScheduler:
    """
    Фоновый планировщик активности для всех аккаунтов.

    Создаёт по одному job на каждую пару (аккаунт, платформа).
    Поддерживает использование как контекстного менеджера.

    Пример:
        with ActivityScheduler() as s:
            run_pipeline()   # активность идёт параллельно

    Или явно:
        s = ActivityScheduler()
        s.start()
        run_pipeline()
        s.stop()
    """

    def __init__(self) -> None:
        self._jobs: List[_AccountActivityJob] = []
        self._started = False

    def start(self) -> None:
        if not config.ACTIVITY_SCHEDULER_ENABLED:
            logger.info("[scheduler] Планировщик активности отключён (ACTIVITY_SCHEDULER_ENABLED=False).")
            return

        if self._started:
            return

        accounts = utils.get_all_accounts()
        if not accounts:
            logger.warning("[scheduler] Аккаунты не найдены — планировщик не запущен.")
            return

        interval_sec = config.ACTIVITY_SCHEDULER_INTERVAL_MIN * 60
        jitter_sec   = config.ACTIVITY_SCHEDULER_JITTER_SEC

        for account in accounts:
            for platform in account["platforms"]:
                job = _AccountActivityJob(
                    account      = account,
                    platform     = platform,
                    interval_sec = interval_sec,
                    jitter_sec   = jitter_sec,
                )
                self._jobs.append(job)
                job.start()

        self._started = True
        logger.info(
            "[scheduler] Запущено %d job(s) для %d аккаунтов. "
            "Интервал: %d мин ± %d сек.",
            len(self._jobs),
            len(accounts),
            config.ACTIVITY_SCHEDULER_INTERVAL_MIN,
            jitter_sec,
        )

    def stop(self) -> None:
        for job in self._jobs:
            job.stop()
        self._jobs.clear()
        self._started = False
        logger.info("[scheduler] Планировщик активности остановлен.")

    def __enter__(self) -> "ActivityScheduler":
        self.start()
        return self

    def __exit__(self, *_) -> None:
        self.stop()
