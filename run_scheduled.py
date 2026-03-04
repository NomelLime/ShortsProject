#!/usr/bin/env python3
"""
run_scheduled.py — Непрерывный режим с публикациями по расписанию.

Запускает все три фоновых компонента и держит процесс живым:
  1. ActivityScheduler     — прогрев аккаунтов каждые ~90 мин
  2. SessionHealthMonitor  — мониторинг срока жизни cookies (раз в час)
  3. UploadScheduler       — загрузка в нужное время из config.json аккаунта

Разово при старте выполняет все подготовительные этапы пайплайна:
  поиск трендов → скачивание → обработка → распределение

После этого загрузка происходит автоматически по расписанию.
Аналитика собирается в фоне через 24–48 ч после публикаций.

Запуск:
    python run_scheduled.py

Остановка: Ctrl+C

Переменные окружения:
    UPLOAD_TIMES=09:00,19:00        — дефолтные времена публикации
    SKIP_PIPELINE_STAGES=1          — пропустить этапы подготовки при старте
    ANALYTICS_INTERVAL_MIN=60       — интервал проверки аналитики (мин)
"""

import os
import sys
import time
import threading
import signal
import argparse

from pipeline.logging_setup import setup_logger
logger = setup_logger("run_scheduled")

from pipeline import config
from pipeline.utils import ensure_dirs, validate_config
from pipeline.scheduler import ActivityScheduler
from pipeline.session_manager import SessionHealthMonitor
from pipeline.upload_scheduler import UploadScheduler
from pipeline.analytics import collect_pending_analytics, queue_reposts, compare_ab_results
from pipeline import downloader, download, main_processing, distributor


# ─────────────────────────────────────────────────────────────────────────────
# Фоновый сборщик аналитики
# ─────────────────────────────────────────────────────────────────────────────

class _AnalyticsCollectorThread(threading.Thread):
    """Периодически вызывает collect_pending_analytics() в фоновом потоке."""

    def __init__(self, interval_min: int) -> None:
        super().__init__(daemon=True, name="analytics-collector")
        self._interval_sec = interval_min * 60
        self._stop_event   = threading.Event()

    def run(self) -> None:
        logger.info("[analytics_thread] Запущен (интервал: %d мин).", self._interval_sec // 60)
        # Первый запуск — через 30 мин после старта
        self._stop_event.wait(timeout=1800)
        while not self._stop_event.is_set():
            try:
                count = collect_pending_analytics()
                if count:
                    logger.info("[analytics_thread] Собрано записей: %d", count)

                # Сравниваем A/B результаты
                ab_results = compare_ab_results()
                if ab_results:
                    logger.info("[analytics_thread] A/B сравнено: %d видео", len(ab_results))

                # Ставим в очередь слабые видео на репост
                reposted = queue_reposts()
                if reposted:
                    logger.info("[analytics_thread] В очередь репоста: %d видео", reposted)

            except Exception as exc:
                logger.error("[analytics_thread] Ошибка: %s", exc, exc_info=True)
            self._stop_event.wait(timeout=self._interval_sec)

    def stop(self) -> None:
        self._stop_event.set()


# ─────────────────────────────────────────────────────────────────────────────
# Подготовительные этапы (поиск + скачивание + обработка + распределение)
# ─────────────────────────────────────────────────────────────────────────────

def _run_preparation_stages() -> None:
    """Запускает все подготовительные этапы один раз при старте."""

    def _stage(fn, name, **kwargs):
        logger.info("=" * 55)
        logger.info("ЭТАП: %s", name)
        logger.info("=" * 55)
        try:
            fn(**kwargs)
            logger.info("Этап %s завершён.", name)
        except Exception as exc:
            logger.error("Ошибка на этапе %s: %s", name, exc, exc_info=True)

    _stage(downloader.search_and_save, "Поиск трендов")
    _stage(download.download_all,       "Скачивание")
    _stage(main_processing.run_processing, "Обработка")
    _stage(distributor.distribute_shorts,  "Распределение")


# ─────────────────────────────────────────────────────────────────────────────
# Основной процесс
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ShortsProject — непрерывный режим по расписанию")
    p.add_argument(
        "--skip-preparation", action="store_true",
        help="Пропустить поиск/скачивание/обработку/распределение при старте",
    )
    p.add_argument(
        "--analytics-interval", type=int, default=60,
        metavar="MIN",
        help="Интервал сбора аналитики в минутах (по умолчанию: 60)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    logger.info("=" * 60)
    logger.info("ShortsProject — Непрерывный режим с расписанием")
    logger.info("=" * 60)

    validate_config()
    ensure_dirs()

    # Обработка Ctrl+C
    stop_event = threading.Event()

    def _handle_signal(*_) -> None:
        logger.info("Получен сигнал остановки — завершение работы...")
        stop_event.set()

    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # 1. Разовая подготовка при старте
    if not args.skip_preparation and not os.getenv("SKIP_PIPELINE_STAGES"):
        logger.info("Запуск подготовительных этапов...")
        _run_preparation_stages()
        logger.info("Подготовительные этапы завершены.")
    else:
        logger.info("Подготовительные этапы пропущены (--skip-preparation).")

    # 2. Запускаем все фоновые компоненты
    analytics_thread = _AnalyticsCollectorThread(interval_min=args.analytics_interval)

    with ActivityScheduler(), SessionHealthMonitor(), UploadScheduler():
        analytics_thread.start()

        logger.info("Все компоненты запущены. Ожидаем событий... (Ctrl+C для остановки)")

        while not stop_event.is_set():
            stop_event.wait(timeout=60)

        analytics_thread.stop()

    logger.info("=" * 60)
    logger.info("ShortsProject остановлен.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
