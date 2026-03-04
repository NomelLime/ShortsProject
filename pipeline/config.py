"""
Единый конфигурационный файл для всего проекта.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent.parent

# ----------------------------------------------------------------------
# Пути к папкам
# ----------------------------------------------------------------------
ASSETS_DIR    = BASE_DIR / 'assets'
PREPARING_DIR = BASE_DIR / 'preparing_shorts'
ARCHIVE_DIR   = BASE_DIR / 'archive'
BG_DIR        = ASSETS_DIR / 'backgrounds'
TEMP_DIR      = BASE_DIR / 'temp' / 'clips'
OUTPUT_DIR    = BASE_DIR / 'Ready-made_shorts_with_description'
BANNER_DIR    = ASSETS_DIR / 'banner'
MUSIC_DIR     = ASSETS_DIR / 'music'
HASHTAGS_FILE = ASSETS_DIR / 'trending_hashtags.txt'

# ----------------------------------------------------------------------
# Пути для загрузчика
# ----------------------------------------------------------------------
KEYWORDS_FILE     = BASE_DIR / "data" / "keywords.txt"
URLS_FILE         = BASE_DIR / "data" / "urls.txt"
FAILED_URLS_FILE  = BASE_DIR / "data" / "failed_urls.txt"
DAILY_LIMIT_FILE  = BASE_DIR / "data" / "daily_limit.json"
UPLOAD_TRACKING_FILE = BASE_DIR / "data" / "upload_tracking.json"
CONFIG_JSON       = BASE_DIR / "config.json"
ACCOUNTS_ROOT     = os.getenv("ACCOUNTS_ROOT", "accounts")
LOG_FILE          = BASE_DIR / "data" / "pipeline.log"

# Куки-файлы
COOKIES = {
    "youtube":   BASE_DIR / "cookies_youtube.txt",
    "tiktok":    BASE_DIR / "cookies_tiktok.txt",
    "instagram": BASE_DIR / "cookies_instagram.txt",
}

# ----------------------------------------------------------------------
# Параметры нарезки (slicer)
# ----------------------------------------------------------------------
CLIP_MIN_LEN          = 15.0
CLIP_MAX_LEN          = 35.0
SHORT_VIDEO_THRESHOLD = 15.0
SILENCE_THRESHOLD     = -30.0
SILENCE_MIN_DUR       = 0.5

# ----------------------------------------------------------------------
# Постобработка (postprocessor)
# ----------------------------------------------------------------------
OUTPUT_W               = 1080
OUTPUT_H               = 1920
OUTPUT_FPS             = 30
CIRCLE_RATIO_LANDSCAPE = 0.70
CIRCLE_RATIO_PORTRAIT  = 0.82
CIRCLE_VARIATION       = 0.05

BANNER_HEIGHT_PCT  = 0.12
CIRCLE_OFFSET_PCT  = 0.05

# ----------------------------------------------------------------------
# Текстовые оверлеи (postprocessor + ai)
# ----------------------------------------------------------------------
FONT_PATH = ASSETS_DIR / 'fonts' / 'Roboto-Bold.ttf'

HOOK_TEXT_DURATION   = 3
HOOK_TEXT_POSITION   = "center"
LOOP_PROMPT_DURATION = 2
OVERLAY_DEFAULT_DURATION = 2
OVERLAY_POSITION     = "x=(w-text_w)/2:y=h*0.8"

# ----------------------------------------------------------------------
# Клонирование (cloner)
# Баннер/лого в клонере УБРАН — баннер уже накладывается в postprocessor
# ----------------------------------------------------------------------
CLONES_PER_VIDEO     = 20
SPEED_RANGE          = (0.97, 1.03)
ZOOM_RANGE           = (1.02, 1.06)
BRIGHTNESS_RANGE     = (-0.03, 0.03)
CONTRAST_RANGE       = (0.95, 1.05)
SATURATION_RANGE     = (0.85, 1.15)
HUE_RANGE            = (-8.0, 8.0)
VIGNETTE_RANGE       = (0.1, 0.5)
NOISE_STRENGTH_RANGE = (3, 8)

# ----------------------------------------------------------------------
# Аудио
# ----------------------------------------------------------------------
AUDIO_BITRATE  = '192k'
MUSIC_VOLUME   = 0.15
MUSIC_FADE_DUR = 0.5

# ----------------------------------------------------------------------
# GPU / CPU
# ----------------------------------------------------------------------
GPU_SLOTS   = 2
MAX_WORKERS = os.cpu_count() or 2

# ----------------------------------------------------------------------
# AI
# ----------------------------------------------------------------------
OLLAMA_MODEL    = 'qwen2.5-vl:7b'
YOLO_MODEL_PT   = 'yolo11x.pt'
AI_ENABLED      = True
OLLAMA_TIMEOUT  = 60
AI_NUM_FRAMES   = 6      # увеличено: кадры для YOLO + метаданных + точек нарезки

# ----------------------------------------------------------------------
# Дедупликация видео (perceptual hash)
# Кадры берутся равномерно с интервалом DEDUP_FRAME_INTERVAL_SEC.
# Например: видео 60 сек → кадр каждые 3 сек → 20 кадров.
# ----------------------------------------------------------------------
DEDUP_FRAME_INTERVAL_SEC = float(os.getenv("DEDUP_FRAME_INTERVAL_SEC", "3.0"))
AI_NUM_VARIANTS = 3

# Автозапуск Ollama если не запущен
OLLAMA_AUTOSTART = True
OLLAMA_AUTOSTART_WAIT_SEC = 5

# ----------------------------------------------------------------------
# Расширения видео
# ----------------------------------------------------------------------
VIDEO_EXT = ('.mp4', '.mov', '.avi', '.mkv', '.webm')

# ----------------------------------------------------------------------
# Параметры поиска (downloader)
# MAX_DURATION_SEC = 90 — ищем именно шортсы (до 90 сек)
# ----------------------------------------------------------------------
MAX_RESULTS_PER_QUERY = 50
MAX_DURATION_SEC      = 90      # было 60, увеличено для захвата 60–90 сек роликов
MIN_VIEWS             = 100_000

# Браузерный поиск (симуляция живого человека)
BROWSER_SEARCH_ENABLED      = True   # включить Playwright-поиск поверх yt-dlp
BROWSER_SEARCH_HEADLESS     = False  # False = видимый браузер для стелс-режима
BROWSER_SEARCH_KEYWORDS_MAX = 3      # сколько keywords обрабатывать через браузер

# AI-расширение ключевых слов
AI_KEYWORD_EXPANSION        = True   # расширять keywords через Ollama перед поиском
AI_KEYWORD_EXPANSION_COUNT  = 5      # сколько новых запросов генерировать на 1 keyword

# ----------------------------------------------------------------------
# Параметры скачивания (download)
# ----------------------------------------------------------------------
FRAGMENT_THREADS     = 10
DOWNLOAD_MAX_WORKERS = 12
RETRIES              = 5
FFPROBE_TIMEOUT      = 30
SOCKET_TIMEOUT       = 30
SLEEP_MIN            = 10
SLEEP_MAX            = 30

# ----------------------------------------------------------------------
# Платформы для поиска
# ----------------------------------------------------------------------
from dataclasses import dataclass

@dataclass(frozen=True)
class Platform:
    name: str
    search_suffixes: tuple[str, ...]
    prefixes: tuple[str, ...]

    def build_queries(self, keyword: str, n: int) -> list[str]:
        queries = []
        for suffix in self.search_suffixes:
            query_text = f"{keyword} {suffix}".strip()
            for prefix_tpl in self.prefixes:
                queries.append(f"{prefix_tpl.format(n=n)}{query_text}")
        return queries

PLATFORMS = [
    Platform(
        name="YouTube Shorts",
        search_suffixes=("#shorts",),
        prefixes=("ytsearch{n}:", "ytsearchdate{n}:"),
    ),
    Platform(
        name="TikTok",
        search_suffixes=("tiktok",),
        prefixes=("ytsearch{n}:",),
    ),
    Platform(
        name="Instagram Reels",
        search_suffixes=("instagram reels",),
        prefixes=("ytsearch{n}:",),
    ),
]

# ----------------------------------------------------------------------
# HTTP-заголовки
# ----------------------------------------------------------------------
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ----------------------------------------------------------------------
# URL платформ для загрузки
# ----------------------------------------------------------------------
PLATFORM_URLS = {
    "youtube": {
        "home":   "https://www.youtube.com",
        "shorts": "https://www.youtube.com/shorts",
        "search": "https://www.youtube.com/results?search_query=",
        "upload": "https://studio.youtube.com",
    },
    "tiktok": {
        "home":   "https://www.tiktok.com",
        "feed":   "https://www.tiktok.com/foryou",
        "search": "https://www.tiktok.com/search?q=",
        "upload": "https://www.tiktok.com/upload",
    },
    "instagram": {
        "home":   "https://www.instagram.com",
        "reels":  "https://www.instagram.com/reels/",
        "search": "https://www.instagram.com/explore/",
        "upload": "https://www.instagram.com/",
    },
}

# Поисковые URL для браузерного поиска
BROWSER_SEARCH_URLS = {
    "youtube":   "https://www.youtube.com/results?search_query={query}&sp=EgIYAQ%253D%253D",  # фильтр: Short
    "tiktok":    "https://www.tiktok.com/search?q={query}",
    "instagram": "https://www.instagram.com/explore/search/keyword/?q={query}",
}

# ----------------------------------------------------------------------
# Тайминги (секунды)
# ----------------------------------------------------------------------
ACTIVITY_DURATION_MIN_SEC = 5 * 60
ACTIVITY_DURATION_MAX_SEC = 15 * 60
WATCH_TIME_MIN_SEC        = 10
WATCH_TIME_MAX_SEC        = 40
CLICK_DELAY_MIN_SEC       = 3
CLICK_DELAY_MAX_SEC       = 10
UPLOAD_TIMEOUT_MS         = 300_000
CAPTCHA_WAIT_TIMEOUT_SEC  = int(os.getenv("CAPTCHA_WAIT_TIMEOUT_SEC", str(30 * 60)))

# ----------------------------------------------------------------------
# Лимиты загрузок — ПО ПЛАТФОРМАМ
# Каждый аккаунт имеет одну платформу; лимит берётся из этой таблицы
# ----------------------------------------------------------------------
PLATFORM_DAILY_LIMITS = {
    "youtube":   int(os.getenv("DAILY_LIMIT_YOUTUBE",   "5")),
    "tiktok":    int(os.getenv("DAILY_LIMIT_TIKTOK",    "5")),
    "instagram": int(os.getenv("DAILY_LIMIT_INSTAGRAM", "5")),
}
# Общий fallback если платформа не распознана
DAILY_UPLOAD_LIMIT = int(os.getenv("DAILY_UPLOAD_LIMIT", "5"))

# Все платформы, которые должны получить видео до архивирования исходника
ALL_PLATFORMS = {"youtube", "tiktok", "instagram"}

# ----------------------------------------------------------------------
# Telegram уведомления
# ----------------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
