"""
pipeline/utils.py – Единый набор утилит для всего проекта.
"""

import json
import logging
import random
import shutil
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import ffmpeg
from playwright.sync_api import Page

from pipeline import config
from pipeline.logging_setup import setup_logger

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Логгер
# ----------------------------------------------------------------------

def get_logger(name: str) -> logging.Logger:
    """Возвращает логгер с указанным именем, используя единую настройку."""
    return setup_logger(name)


# ----------------------------------------------------------------------
# Проверка внешних инструментов
# ----------------------------------------------------------------------

def check_ffmpeg() -> None:
    """Проверяет наличие ffmpeg в системе, иначе завершает работу."""
    if not shutil.which('ffmpeg'):
        logger.error("❌ FFmpeg не найден. Установите: https://ffmpeg.org")
        sys.exit(1)


# ----------------------------------------------------------------------
# Работа с видео через ffmpeg/ffprobe
# ----------------------------------------------------------------------

def probe_video(path: Union[str, Path]) -> Dict:
    """
    Анализирует видео и возвращает словарь с информацией.

    Returns:
        Словарь с ключами: width, height, fps, duration, has_audio, sample_rate.
    """
    try:
        info = ffmpeg.probe(str(path))
    except ffmpeg.Error as e:
        raise ValueError(f"Не удалось прочитать {path}: {e.stderr.decode()}")

    v = next((s for s in info['streams'] if s['codec_type'] == 'video'), None)
    a = next((s for s in info['streams'] if s['codec_type'] == 'audio'), None)
    if not v:
        raise ValueError(f"Нет видео-потока в {path}")

    fps_raw = v.get('r_frame_rate', '30/1').split('/')
    fps = float(fps_raw[0]) / float(fps_raw[1]) if len(fps_raw) == 2 else 30.0

    return {
        'width':       int(v['width']),
        'height':      int(v['height']),
        'fps':         fps,
        'duration':    float(info['format']['duration']),
        'has_audio':   a is not None,
        'sample_rate': int(a['sample_rate']) if a else 44100,
    }


def check_video_integrity(video_path: Path) -> bool:
    """
    Проверяет целостность видеофайла через ffprobe.
    Возвращает True, если видео корректно.
    """
    if not video_path.exists():
        logger.warning("Файл не найден: %s", video_path)
        return False

    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=codec_type",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(video_path),
            ],
            capture_output=True,
            text=True,
            timeout=config.FFPROBE_TIMEOUT,
        )
    except FileNotFoundError:
        logger.error("ffprobe не найден – установите FFmpeg. Проверка пропущена.")
        return True  # не блокируем пайплайн
    except subprocess.TimeoutExpired:
        logger.warning("ffprobe: таймаут (%d с) для %s", config.FFPROBE_TIMEOUT, video_path.name)
        return False

    if result.returncode == 0 and "video" in result.stdout.lower():
        return True

    logger.warning("ffprobe: повреждённый файл [%s] – %s",
                   video_path.name, result.stderr.strip()[:200] or "нет деталей")
    return False


def detect_encoder() -> Tuple[str, Dict]:
    """Определяет доступный видеокодек (GPU h264_nvenc или CPU libx264)."""
    test = [
        'ffmpeg', '-y', '-f', 'lavfi',
        '-i', 'color=c=black:size=256x256:duration=0.1:rate=30',
        '-vcodec', 'h264_nvenc', '-f', 'null', '-',
    ]
    try:
        r = subprocess.run(test, capture_output=True, timeout=15)
        if r.returncode == 0:
            logger.info("🚀 GPU обнаружен – h264_nvenc")
            return 'h264_nvenc', {'preset': 'p4', 'rc': 'vbr', 'cq': 21}
        err = r.stderr.decode('utf-8', errors='replace')
        bad = [
            line.strip() for line in err.splitlines()
            if any(k in line for k in ('nvenc', 'NVENC', 'No CUDA', 'driver'))
        ]
        if bad:
            logger.warning("⚠️ h264_nvenc недоступен:")
            for line in bad[:3]:
                logger.warning("   %s", line)
    except Exception as e:
        logger.warning("⚠️ GPU-тест: %s", e)

    logger.info("💻 libx264 (CPU)")
    return 'libx264', {'preset': 'fast', 'crf': 21}


def get_random_asset(folder: Path, exts: tuple) -> Optional[Path]:
    """Возвращает случайный файл из папки с заданными расширениями."""
    if not folder.exists():
        return None
    files = [f for f in folder.iterdir() if f.suffix.lower() in exts]
    return random.choice(files) if files else None


# ----------------------------------------------------------------------
# Человекоподобные задержки и ввод текста
# ----------------------------------------------------------------------

def human_sleep(lo: float = 3.0, hi: float = 10.0, sigma: float = 0.25) -> None:
    """Случайная пауза с гауссовым шумом."""
    base   = random.uniform(lo, hi)
    jitter = random.gauss(0, sigma)
    delay  = max(0.3, base + jitter)
    time.sleep(delay)


def type_humanlike(
    page: Page,
    selector: str,
    text: str,
    clear_first: bool = True,
    min_char_delay: float = 0.04,
    max_char_delay: float = 0.16,
) -> None:
    """Вводит текст посимвольно со случайными задержками."""
    el = page.locator(selector).first
    el.click()
    if clear_first:
        el.fill("")
        human_sleep(0.2, 0.5)
    for char in text:
        page.keyboard.type(char)
        time.sleep(random.uniform(min_char_delay, max_char_delay))
    human_sleep(0.5, 1.5)


# ----------------------------------------------------------------------
# Работа с прокси и конфигами
# ----------------------------------------------------------------------

def load_proxy() -> Optional[str]:
    """Читает config.json и возвращает строку прокси."""
    if not config.CONFIG_JSON.exists():
        logger.debug("config.json не найден – прокси не используется.")
        return None

    try:
        data = json.loads(config.CONFIG_JSON.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Не удалось прочитать config.json: %s", exc)
        return None

    if proxy := data.get("proxy", "").strip():
        logger.info("Прокси (глобальный): %s", proxy)
        return proxy

    for acc in data.get("accounts", []):
        if proxy := acc.get("proxy", "").strip():
            logger.info("Прокси (accounts[0]): %s", proxy)
            return proxy

    logger.debug("Прокси в config.json не задан.")
    return None


# ----------------------------------------------------------------------
# Работа с текстовыми файлами (urls, keywords)
# ----------------------------------------------------------------------

def read_lines(path: Path) -> List[str]:
    """Читает файл, возвращает непустые строки без комментариев (#), уникальные."""
    if not path.exists():
        return []
    seen: dict = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            seen[line] = None
    return list(seen)


def write_lines(path: Path, lines: List[str]) -> None:
    """Записывает отсортированный список строк в файл."""
    path.write_text("\n".join(sorted(set(lines))) + "\n", encoding="utf-8")


def merge_and_save_urls(new_urls: List[str], path: Path = config.URLS_FILE) -> int:
    """Объединяет существующие URL с новыми, сохраняет. Возвращает количество добавленных."""
    existing = set(read_lines(path))
    merged   = existing | set(new_urls)
    added    = len(merged) - len(existing)
    write_lines(path, list(merged))
    logger.info("urls.txt: итого %d URL (добавлено +%d) → %s", len(merged), added, path)
    return added


def load_keywords() -> List[str]:
    """Загружает ключевые слова из keywords.txt."""
    if config.KEYWORDS_FILE.exists():
        keywords = read_lines(config.KEYWORDS_FILE)
        if keywords:
            logger.info("Ключевые слова из keywords.txt: %d шт.", len(keywords))
            return keywords

    example = (
        "# Ключевые слова для поиска (по одному на строку)\n"
        "funny cats\nlife hacks\nsatisfying videos\ncooking shorts\n"
        "gym motivation\ntravel reels\ndance challenge\namazing nature\n"
    )
    config.KEYWORDS_FILE.write_text(example, encoding="utf-8")
    logger.warning("Создан пример keywords.txt: %s – заполните его.", config.KEYWORDS_FILE)
    return []


# ----------------------------------------------------------------------
# Работа с аккаунтами и очередями загрузки
# ----------------------------------------------------------------------

def get_all_accounts() -> List[Dict]:
    """Возвращает список всех аккаунтов из папки accounts/."""
    root = Path(config.ACCOUNTS_ROOT)
    if not root.exists():
        logger.warning("Папка аккаунтов не найдена: %s", root)
        return []

    accounts = []
    for acc_dir in sorted(root.iterdir()):
        if not acc_dir.is_dir():
            continue
        config_file = acc_dir / "config.json"
        if not config_file.exists():
            logger.warning("Нет config.json в %s, пропускаю.", acc_dir)
            continue
        try:
            # Используем имя acc_cfg, чтобы не перекрывать модуль config
            acc_cfg = json.loads(config_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            logger.error("Ошибка разбора %s: %s", config_file, e)
            continue

        accounts.append({
            "name":            acc_dir.name,
            "config":          acc_cfg,
            "profile_dir":     acc_dir / "browser_profile",
            "upload_queue_dir": acc_dir / "upload_queue",
        })
    logger.info("Найдено аккаунтов: %d", len(accounts))
    return accounts


def get_upload_queue(upload_queue_dir: Path) -> List[Dict]:
    """
    Возвращает список видео для загрузки из папки upload_queue/.
    Каждый элемент: {video_path, meta_path, meta}
    """
    if not upload_queue_dir.exists():
        return []

    queue = []
    for video_file in sorted(upload_queue_dir.glob("*.mp4")):
        meta_file = video_file.with_suffix(".json")
        if not meta_file.exists():
            logger.warning("Нет метаданных для %s, пропускаю.", video_file.name)
            continue
        try:
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            logger.error("Ошибка разбора %s: %s", meta_file, e)
            continue

        queue.append({
            "video_path": video_file,
            "meta_path":  meta_file,
            "meta":       meta,
        })
    return queue


def mark_uploaded(item: Dict) -> None:
    """Помечает видео и метаданные как загруженные (добавляет суффикс .done)."""
    for key in ("video_path", "meta_path"):
        src: Path = item[key]
        if src.exists():
            dst = src.parent / (src.name + ".done")
            src.rename(dst)
            logger.debug("Помечено как done: %s", dst.name)


def _limit_file(acc_dir: Path) -> Path:
    return acc_dir / "daily_limit.json"


def _load_limit_data(acc_dir: Path) -> Dict:
    f = _limit_file(acc_dir)
    if not f.exists():
        return {}
    try:
        return json.loads(f.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_limit_data(acc_dir: Path, data: Dict) -> None:
    """Сохраняет данные лимитов, обрезая записи старше 30 дней."""
    cutoff = str(date.today() - timedelta(days=30))
    data   = {k: v for k, v in data.items() if k >= cutoff}
    _limit_file(acc_dir).write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def get_uploads_today(acc_dir: Path) -> int:
    """Возвращает количество загрузок за сегодня для данного аккаунта."""
    data  = _load_limit_data(acc_dir)
    today = str(date.today())
    return data.get(today, 0)


def increment_upload_count(acc_dir: Path) -> None:
    """Увеличивает счётчик загрузок на 1 для текущей даты."""
    data  = _load_limit_data(acc_dir)
    today = str(date.today())
    data[today] = data.get(today, 0) + 1
    _save_limit_data(acc_dir, data)


def is_daily_limit_reached(acc_dir: Path, limit: int = None) -> bool:
    """Проверяет, достигнут ли дневной лимит загрузок."""
    limit = limit if limit is not None else config.DAILY_UPLOAD_LIMIT
    return get_uploads_today(acc_dir) >= limit


def create_sample_account(account_name: str, platform: str = "youtube") -> None:
    """Создаёт структуру папок и config.json для нового аккаунта."""
    acc_dir = Path(config.ACCOUNTS_ROOT) / account_name
    (acc_dir / "browser_profile").mkdir(parents=True, exist_ok=True)
    (acc_dir / "upload_queue").mkdir(parents=True, exist_ok=True)

    config_path = acc_dir / "config.json"
    if not config_path.exists():
        sample = {
            "platform": platform,
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "proxy": {
                "host":     "proxy.example.com",
                "port":     8080,
                "username": "user",
                "password": "pass",
            },
        }
        config_path.write_text(
            json.dumps(sample, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info("Создан пример конфига: %s", config_path)
    else:
        logger.info("Конфиг уже существует: %s", config_path)


# ----------------------------------------------------------------------
# Вспомогательные функции для JSON
# ----------------------------------------------------------------------

def load_json(path: Path) -> Optional[Dict]:
    """Загружает JSON из файла, возвращает None при ошибке."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_json(path: Path, data: Dict) -> None:
    """Сохраняет словарь в JSON-файл."""
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ----------------------------------------------------------------------
# Создание всех необходимых директорий
# ----------------------------------------------------------------------

def ensure_dirs() -> None:
    """Создаёт все папки, используемые в проекте."""
    dirs = [
        config.ASSETS_DIR,
        config.BG_DIR,
        config.BANNER_DIR,
        config.MUSIC_DIR,
        config.PREPARING_DIR,
        config.TEMP_DIR,
        config.OUTPUT_DIR,
        config.ARCHIVE_DIR,
        Path(config.ACCOUNTS_ROOT),
        config.LOG_FILE.parent,
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
    logger.info("Все директории созданы/проверены.")
