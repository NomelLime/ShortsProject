#!/usr/bin/env python3
"""
Главный оркестратор, запускающий все этапы пайплайна последовательно.
Каждый этап реализован как функция, возвращающая bool/статус.
При ошибке этапа процесс не прерывается, ошибка логируется, и можно перейти к следующему этапу (если разрешено).
"""

import argparse
import sys
from pathlib import Path

# Настройка логирования (должна быть первой)
from pipeline.logging_setup import setup_logger
logger = setup_logger("orchestrator")

# Импорты этапов
from pipeline import (
    downloader,
    download,
    main_processing,   # бывший main.py, переделанный в функцию
    distributor,
    uploader,          # содержит функцию upload_all()
    finalize,
)
from pipeline.utils import ensure_dirs, validate_config
from pipeline import config
from pipeline.scheduler import ActivityScheduler

def parse_args():
    parser = argparse.ArgumentParser(description="Запуск полного пайплайна обработки и загрузки видео")
    parser.add_argument("--skip-search", action="store_true", help="Пропустить поиск трендов (downloader)")
    parser.add_argument("--skip-download", action="store_true", help="Пропустить скачивание (download)")
    parser.add_argument("--skip-processing", action="store_true", help="Пропустить обработку (нарезка, AI, постобработка, клоны)")
    parser.add_argument("--skip-distribute", action="store_true", help="Пропустить распределение (distributor)")
    parser.add_argument("--skip-upload", action="store_true", help="Пропустить загрузку (uploader)")
    parser.add_argument("--skip-finalize", action="store_true", help="Пропустить финализацию (finalize)")
    parser.add_argument("--dry-run", action="store_true", help="Пробный запуск без реальных изменений")
    return parser.parse_args()

def run_stage(stage_func, stage_name, *args, **kwargs):
    """Обёртка для запуска этапа с логированием и обработкой ошибок."""
    logger.info("=" * 60)
    logger.info("ЗАПУСК ЭТАПА: %s", stage_name)
    logger.info("=" * 60)
    try:
        result = stage_func(*args, **kwargs)
        logger.info("Этап %s завершён успешно", stage_name)
        return result
    except Exception as e:
        logger.error("Критическая ошибка на этапе %s: %s", stage_name, e, exc_info=True)
        return False

def main():
    args = parse_args()

    # Ранняя проверка конфигурации
    validate_config()

    # Создаём все необходимые директории
    ensure_dirs()

    try:
        from pipeline.warmup_report import log_warmup_dashboard

        log_warmup_dashboard(logger)
    except Exception:
        pass

    # Запускаем фоновый планировщик активности аккаунтов
    # Активность идёт параллельно с пайплайном, независимо от загрузки
    with ActivityScheduler():

        # ---- Этап 0: Поиск трендов ----
        if not args.skip_search:
            run_stage(downloader.search_and_save, "downloader")
        else:
            logger.info("Пропуск этапа поиска (--skip-search)")

        # ---- Этап 1: Скачивание ----
        if not args.skip_download:
            run_stage(download.download_all, "download")
        else:
            logger.info("Пропуск этапа скачивания (--skip-download)")

        # ---- Этап 2: Обработка (нарезка, AI, постобработка, клонирование) ----
        if not args.skip_processing:
            run_stage(main_processing.run_processing, "processing", dry_run=args.dry_run)
        else:
            logger.info("Пропуск этапа обработки (--skip-processing)")

        # ---- Этап 3: Распределение по аккаунтам ----
        if not args.skip_distribute:
            run_stage(distributor.distribute_shorts, "distributor", dry_run=args.dry_run)
        else:
            logger.info("Пропуск этапа распределения (--skip-distribute)")

        # ---- Этап 4: Загрузка на платформы ----
        if not args.skip_upload:
            upload_results = run_stage(uploader.upload_all, "uploader", dry_run=args.dry_run)
            if not isinstance(upload_results, list):
                logger.warning("Загрузка не вернула список результатов — финализация получит пустой список.")
                upload_results = []
        else:
            upload_results = []
            logger.info("Пропуск этапа загрузки (--skip-upload)")

        # ---- Этап 5: Финализация (архивирование, отчёт) ----
        if not args.skip_finalize:
            run_stage(finalize.finalize_and_report, "finalize", upload_results, dry_run=args.dry_run)
        else:
            logger.info("Пропуск этапа финализации (--skip-finalize)")

    logger.info("=" * 60)
    logger.info("ПАЙПЛАЙН ЗАВЕРШЁН")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
