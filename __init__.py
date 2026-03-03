"""
Unified Video Shorts Pipeline — пакет для обработки и загрузки видео.
Экспортирует основные функции и классы для внешнего использования.
"""

__version__ = "1.0.0"

# Импортируем модули для доступа через pipeline.<module>
from . import config
from . import utils
from . import logging_setup

# Основные функции этапов
from .downloader import search_and_save
from .download import download_all
from .main_processing import run_processing
from .distributor import distribute_shorts
from .uploader import upload_all
from .finalize import finalize_and_report

# Утилиты для работы с аккаунтами и очередью
from .utils import (
    get_all_accounts,
    get_upload_queue,
    mark_uploaded,
    get_uploads_today,
    increment_upload_count,
    is_daily_limit_reached,
    create_sample_account,
)

# Человекоподобные задержки и ввод текста
from .utils import human_sleep, type_humanlike

# Функции для работы с видео
from .utils import probe_video, check_video_integrity, detect_encoder, get_random_asset

__all__ = [
    "config",
    "utils",
    "logging_setup",
    "search_and_save",
    "download_all",
    "run_processing",
    "distribute_shorts",
    "upload_all",
    "finalize_and_report",
    "get_all_accounts",
    "get_upload_queue",
    "mark_uploaded",
    "get_uploads_today",
    "increment_upload_count",
    "is_daily_limit_reached",
    "create_sample_account",
    "human_sleep",
    "type_humanlike",
    "probe_video",
    "check_video_integrity",
    "detect_encoder",
    "get_random_asset",
]