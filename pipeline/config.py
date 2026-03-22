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
KEYWORDS_FILE        = BASE_DIR / "data" / "keywords.txt"
URLS_FILE            = BASE_DIR / "data" / "urls.txt"
FAILED_URLS_FILE     = BASE_DIR / "data" / "failed_urls.txt"
UPLOAD_RETRY_QUEUE   = BASE_DIR / "data" / "upload_retry_queue.json"
DOWNLOAD_CHECKPOINT  = BASE_DIR / "data" / "download_checkpoint.json"   # чекпоинт скачивания
DAILY_LIMIT_FILE     = BASE_DIR / "data" / "daily_limit.json"
UPLOAD_TRACKING_FILE  = BASE_DIR / "data" / "upload_tracking.json"
ANALYTICS_FILE        = BASE_DIR / "data" / "analytics.json"
SESSION_HEALTH_FILE   = BASE_DIR / "data" / "session_health.json"
CONFIG_JSON          = BASE_DIR / "config.json"
ACCOUNTS_ROOT        = os.getenv("ACCOUNTS_ROOT", "accounts")
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
OLLAMA_MODEL    = 'qwen2.5vl:7b'   # VL-модель: видит реальные кадры видео
AI_ENABLED      = True
OLLAMA_TIMEOUT  = 60
AI_NUM_FRAMES   = 6      # кадры для VL-анализа метаданных и точек нарезки

# VL-фильтрация контента (CURATOR + SCOUT)
# CURATOR: проверяет качество видео перед обработкой (только новые — кеш по sha256)
# SCOUT:   оценивает thumbnail YouTube-видео до скачивания
CURATOR_VL_QUALITY_CHECK  = os.getenv("CURATOR_VL_CHECK",   "1") == "1"
SCOUT_VL_THUMBNAIL_FILTER = os.getenv("SCOUT_VL_FILTER",    "1") == "1"
SCOUT_VL_MIN_SCORE        = int(os.getenv("SCOUT_VL_MIN_SCORE",  "7"))   # 1-10
SCOUT_VL_MAX_PER_CYCLE    = int(os.getenv("SCOUT_VL_MAX_CYCLE",  "20"))  # макс. проверок за цикл
VL_CACHE_FILE             = BASE_DIR / "data" / "vl_cache.json"

# ----------------------------------------------------------------------
# Дедупликация видео (perceptual hash + Hamming distance)
# DEDUP_HAMMING_THRESHOLD — макс. расстояние Хэмминга (из 64 бит):
#   0 = только точные копии | 10 = похожие клоны | 20 = агрессивно
# ----------------------------------------------------------------------
DEDUP_FRAME_INTERVAL_SEC  = float(os.getenv("DEDUP_FRAME_INTERVAL_SEC", "3.0"))
DEDUP_HAMMING_THRESHOLD   = int(os.getenv("DEDUP_HAMMING_THRESHOLD",    "10"))

# ----------------------------------------------------------------------
# Карантин аккаунтов
# ----------------------------------------------------------------------
QUARANTINE_ERROR_THRESHOLD = int(os.getenv("QUARANTINE_ERROR_THRESHOLD", "3"))
QUARANTINE_DURATION_HOURS  = int(os.getenv("QUARANTINE_DURATION_HOURS",  "6"))

# ----------------------------------------------------------------------
# A/B тестирование заголовков
# ----------------------------------------------------------------------
AB_TEST_ENABLED         = os.getenv("AB_TEST_ENABLED", "1") != "0"
AB_TEST_COMPARE_AFTER_H = int(os.getenv("AB_TEST_COMPARE_AFTER_H", "24"))

# ----------------------------------------------------------------------
# Авто-репост слабых видео
# ----------------------------------------------------------------------
REPOST_ENABLED      = os.getenv("REPOST_ENABLED", "1") != "0"
REPOST_MIN_VIEWS    = int(os.getenv("REPOST_MIN_VIEWS",    "500"))
REPOST_AFTER_HOURS  = int(os.getenv("REPOST_AFTER_HOURS",  "48"))
REPOST_MAX_ATTEMPTS = int(os.getenv("REPOST_MAX_ATTEMPTS", "2"))

# ----------------------------------------------------------------------
# Умное расписание (на основе аналитики)
# ----------------------------------------------------------------------
SMART_SCHEDULE_ENABLED     = os.getenv("SMART_SCHEDULE_ENABLED", "1") != "0"
SMART_SCHEDULE_MIN_SAMPLES = int(os.getenv("SMART_SCHEDULE_MIN_SAMPLES", "10"))

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

# ----------------------------------------------------------------------
# Планировщик фоновой активности (scheduler.py)
# Активность запускается независимо от цикла загрузки
# ----------------------------------------------------------------------
ACTIVITY_SCHEDULER_ENABLED      = True
ACTIVITY_SCHEDULER_INTERVAL_MIN = int(os.getenv("ACTIVITY_INTERVAL_MIN", "90"))   # раз в 90 мин на аккаунт
ACTIVITY_SCHEDULER_JITTER_SEC   = int(os.getenv("ACTIVITY_JITTER_SEC",   "300"))  # ±5 мин разброс

# Окно активности (местное время): job-ы не стартуют вне этого диапазона
# Учитывает таймзону аккаунтов — платформы не засчитывают мёртвые ночные часы
ACTIVITY_HOURS_START    = int(os.getenv("ACTIVITY_HOURS_START", "8"))   # с 08:00
ACTIVITY_HOURS_END      = int(os.getenv("ACTIVITY_HOURS_END",   "23"))  # до 23:00

# Максимум одновременных VL-сессий активности (ограничивает нагрузку на GPU)
# При превышении — job переносится на +10 мин вместо блокировки потока
ACTIVITY_VL_CONCURRENCY = int(os.getenv("ACTIVITY_VL_CONCURRENCY", "2"))

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
    # Instagram Reels намеренно исключён из yt-dlp поиска:
    # ytsearch не достаёт Reels напрямую. Instagram ищется через
    # браузерный поиск (_search_browser в downloader.py).
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
# Сессии / авто-обновление cookies
# SESSION_MAX_AGE_HOURS — максимальный возраст сессии без проверки.
# По умолчанию 20 ч: проверка/обновление до истечения типичного срока cookies.
# SESSION_REFRESH_WARN_HOURS — порог для предупреждения в Telegram.
# ----------------------------------------------------------------------
SESSION_MAX_AGE_HOURS      = int(os.getenv("SESSION_MAX_AGE_HOURS",      "20"))
SESSION_REFRESH_WARN_HOURS = int(os.getenv("SESSION_REFRESH_WARN_HOURS", "18"))

# ----------------------------------------------------------------------
# Аналитика
# ANALYTICS_COLLECT_AFTER_HOURS — через сколько часов после загрузки
# собирать статистику (просмотры / лайки / комментарии).
# ANALYTICS_COLLECT_MAX_HOURS   — не собирать если видео старше этого порога.
# ----------------------------------------------------------------------
ANALYTICS_COLLECT_AFTER_HOURS = int(os.getenv("ANALYTICS_COLLECT_AFTER_HOURS", "24"))
ANALYTICS_COLLECT_MAX_HOURS   = int(os.getenv("ANALYTICS_COLLECT_MAX_HOURS",   "72"))

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
# Прогрев после первой валидной сессии: заливка откладывается на N дней
# (см. pipeline/upload_warmup.py, accounts/<name>/upload_warmup.json).
# В config.json аккаунта: "skip_upload_warmup": true — отключить для старых аккаунтов.
# ----------------------------------------------------------------------
def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() not in ("0", "false", "no", "off")


UPLOAD_WARMUP_ENABLED = _env_bool("UPLOAD_WARMUP_ENABLED", True)
UPLOAD_WARMUP_MIN_DAYS = max(1, int(os.getenv("UPLOAD_WARMUP_MIN_DAYS", "3")))
UPLOAD_WARMUP_MAX_DAYS = max(
    UPLOAD_WARMUP_MIN_DAYS,
    int(os.getenv("UPLOAD_WARMUP_MAX_DAYS", "5")),
)
# platform — отдельный прогрев на каждую сеть; account — одно окно на весь аккаунт
UPLOAD_WARMUP_DEFAULT_SCOPE = os.getenv("UPLOAD_WARMUP_DEFAULT_SCOPE", "platform").strip().lower()
if UPLOAD_WARMUP_DEFAULT_SCOPE not in ("platform", "account"):
    UPLOAD_WARMUP_DEFAULT_SCOPE = "platform"
# Напоминание в Telegram за N часов до конца прогрева (0 = отключить)
UPLOAD_WARMUP_REMINDER_HOURS = float(os.getenv("UPLOAD_WARMUP_REMINDER_HOURS", "24"))
# VL-активность в прогреве: доля длительности сессии (1.0 = как обычно)
ACTIVITY_WARMUP_DURATION_MULT = float(os.getenv("ACTIVITY_WARMUP_DURATION_MULT", "0.45"))
# Множитель интервала между сессиями активности (1.0 = без изменений)
ACTIVITY_WARMUP_INTERVAL_MULT = float(os.getenv("ACTIVITY_WARMUP_INTERVAL_MULT", "1.75"))

# ----------------------------------------------------------------------
# TTS — Kokoro-82M (локальный, бесплатный, MIT лицензия)
# Файлы модели: assets/tts/kokoro-v1.9.onnx + voices-v1.0.bin
# Скачать: https://github.com/thewh1teagle/kokoro-onnx/releases
# ----------------------------------------------------------------------
TTS_ENABLED        = os.getenv("TTS_ENABLED", "true").lower() == "true"
TTS_DIR            = ASSETS_DIR / "tts"
TTS_MODEL_FILE     = TTS_DIR / "kokoro-v1.9.onnx"
TTS_VOICES_FILE    = TTS_DIR / "voices-v1.0.bin"
TTS_DEFAULT_LANG   = os.getenv("TTS_DEFAULT_LANG", "en")   # en | ru | en-gb
TTS_SPEED          = float(os.getenv("TTS_SPEED", "1.0"))  # 0.5–2.0
TTS_VOLUME         = float(os.getenv("TTS_VOLUME", "1.0")) # громкость голоса (0.1–2.0)
TTS_VOICE_OVER_MIX = float(os.getenv("TTS_VOICE_OVER_MIX", "0.85"))  # доля голоса в миксе
# Голос применяется к hook_text из метаданных видео
TTS_USE_HOOK_TEXT  = os.getenv("TTS_USE_HOOK_TEXT", "true").lower() == "true"
# Временная папка для .wav файлов до микширования
TTS_TEMP_DIR       = BASE_DIR / "data" / "tts_temp"

# ----------------------------------------------------------------------
# Telegram уведомления
# ----------------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# ----------------------------------------------------------------------
# TrendScout — Агент мониторинга трендов (Этап 6)
# ----------------------------------------------------------------------
TREND_SCOUT_ENABLED    = os.getenv("TREND_SCOUT_ENABLED",  "1") == "1"
TREND_SCOUT_INTERVAL_H = int(os.getenv("TREND_SCOUT_INTERVAL_H", "2"))   # часов
TREND_SCOUT_THRESHOLD  = int(os.getenv("TREND_SCOUT_THRESHOLD",  "2"))   # min score
TREND_SCOUT_TOP_N      = int(os.getenv("TREND_SCOUT_TOP_N",      "30"))  # топ N
TREND_SCOUT_GEO        = os.getenv("TREND_SCOUT_GEO", "")                # "" = глобально
TREND_SCOUT_SOURCES    = os.getenv("TREND_SCOUT_SOURCES", "google,yt,tiktok")

# ----------------------------------------------------------------------
# Субтитры + перевод (Этап 8)
# ----------------------------------------------------------------------
SUBTITLE_ENABLED       = os.getenv("SUBTITLE_ENABLED",    "0") == "1"
SUBTITLE_LANGUAGES     = os.getenv("SUBTITLE_LANGUAGES",  "ru")          # ru,en,es,pt
WHISPER_MODEL_SIZE     = os.getenv("WHISPER_MODEL_SIZE",  "base")        # tiny/base/small/medium/large
SUBTITLE_STYLE         = os.getenv("SUBTITLE_STYLE",      "bottom_white") # bottom_white | top_yellow

# ----------------------------------------------------------------------
# Voice cloning — OpenVoice v2 (Этап 14)
# ----------------------------------------------------------------------
VOICE_CLONE_ENABLED    = os.getenv("VOICE_CLONE_ENABLED", "0") == "1"
VOICE_CLONE_MODEL      = os.getenv("VOICE_CLONE_MODEL",   "openvoice")   # openvoice | rvc
VOICE_CLONE_REF_AUDIO  = os.getenv("VOICE_CLONE_REF_AUDIO", "")          # путь к ref audio файлу

# ----------------------------------------------------------------------
# A/B тестирование миниатюр (Этап 7)
# ----------------------------------------------------------------------
THUMBNAIL_AB_ENABLED   = os.getenv("THUMBNAIL_AB_ENABLED",  "0") == "1"
THUMBNAIL_AB_VARIANTS  = int(os.getenv("THUMBNAIL_AB_VARIANTS", "2"))    # 2 или 3

# ----------------------------------------------------------------------
# Серийный контент (Этап 15)
# ----------------------------------------------------------------------
SERIAL_ENABLED     = os.getenv("SERIAL_ENABLED",     "0") == "1"
SERIAL_MIN_VIEWS   = int(os.getenv("SERIAL_MIN_VIEWS",   "500"))
SERIAL_MIN_HISTORY = int(os.getenv("SERIAL_MIN_HISTORY", "30"))   # минимум видео для анализа
SERIAL_TOP_PCT     = int(os.getenv("SERIAL_TOP_PCT",     "25"))   # топ N% по engagement_rate

# ----------------------------------------------------------------------
# Размытый фон для вертикального формата (Сессия 11 — ФИЧА 1)
# Если нет фонового видео (bg_path=None) — заполнять размытым видео вместо чёрных полос.
# Порядок приоритетов: bg_path (видео-фон) > BLURRED_BG > чёрные полосы
# ----------------------------------------------------------------------
BLURRED_BG_ENABLED  = os.getenv("BLURRED_BG_ENABLED", "true").lower() == "true"
BLURRED_BG_SIGMA    = int(os.getenv("BLURRED_BG_SIGMA", "40"))      # сила размытия (boxblur)
BLURRED_BG_DARKEN   = float(os.getenv("BLURRED_BG_DARKEN", "0.6"))  # затемнение 0.0–1.0 (1.0 = без затемнения)

# ----------------------------------------------------------------------
# Библиотека видеофильтров (Сессия 11 — ФИЧА 3)
# Визуальный фильтр применяется после масштабирования, перед баннером.
# Значение "none" = без фильтра. Список: см. pipeline/video_filters.py
# ----------------------------------------------------------------------
VIDEO_FILTER_ENABLED = os.getenv("VIDEO_FILTER_ENABLED", "false").lower() == "true"
VIDEO_FILTER_DEFAULT = os.getenv("VIDEO_FILTER_DEFAULT", "none")   # фильтр по умолчанию
VIDEO_FILTER_RANDOM  = os.getenv("VIDEO_FILTER_RANDOM", "false").lower() == "true"  # случайный для каждого видео

# ----------------------------------------------------------------------
# Hook-зум в первые N секунд (Сессия 11 — ФИЧА 5)
# Плавное Ken Burns zoom-in в начале клипа для повышения retention.
# ----------------------------------------------------------------------
HOOK_ZOOM_ENABLED   = os.getenv("HOOK_ZOOM_ENABLED", "false").lower() == "true"
HOOK_ZOOM_DURATION  = float(os.getenv("HOOK_ZOOM_DURATION", "2.0"))   # секунды
HOOK_ZOOM_START     = float(os.getenv("HOOK_ZOOM_START", "1.0"))       # начальный зум (1.0 = нет)
HOOK_ZOOM_END       = float(os.getenv("HOOK_ZOOM_END", "1.15"))        # конечный зум (1.15 = +15%)

# ----------------------------------------------------------------------
# Whisper-транскрипция для AI-метаданных (Сессия 11 — ФИЧА 2)
# Транскрипт речи из видео включается в LLM-промпт → лучший title/tags.
# Использует faster-whisper (уже в requirements.txt для subtitler.py).
# ----------------------------------------------------------------------
META_WHISPER_ENABLED  = os.getenv("META_WHISPER_ENABLED", "false").lower() == "true"
META_WHISPER_MODEL    = os.getenv("META_WHISPER_MODEL", "base")   # tiny/base/small/medium
META_WHISPER_MAX_SEC  = int(os.getenv("META_WHISPER_MAX_SEC", "120"))  # макс. длина для транскрипции
META_WHISPER_LANGUAGE = os.getenv("META_WHISPER_LANGUAGE", "")    # "" = автодетект
