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

# New imports for hashing
import hashlib
from PIL import Image
import imagehash

HASH_DB = config.BASE_DIR / "data" / "video_hashes.json"

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
        'duration':    float(info['format'].get('duration', 0)),
        'has_audio':   bool(a),
        'sample_rate': int(a.get('sample_rate', 0)) if a else 0,
    }


def check_video_integrity(path: Union[str, Path]) -> bool:
    """Проверяет целостность видео через ffprobe."""
    try:
        ffmpeg.probe(str(path))
        return True
    except ffmpeg.Error:
        return False


def detect_encoder() -> Tuple[str, Optional[Dict]]:
    """Определяет доступный кодек: h264_nvenc (GPU) или libx264 (CPU)."""
    try:
        subprocess.check_output(["ffmpeg", "-encoders"])
        return "h264_nvenc", {"preset": "fast", "cq": "23"}
    except Exception:
        return "libx264", {"preset": "fast", "crf": "23"}


# ----------------------------------------------------------------------
# Человекоподобные задержки
# ----------------------------------------------------------------------

def human_sleep(min_sec: float, max_sec: float) -> None:
    """Случайная задержка для имитации человека."""
    time.sleep(random.uniform(min_sec, max_sec))


def type_humanlike(page: Page, selector: str, text: str) -> None:
    """Имитирует набор текста человеком с паузами."""
    elem = page.locator(selector)
    elem.click()
    for char in text:
        elem.type(char, delay=random.uniform(0.05, 0.2))


# ----------------------------------------------------------------------
# Загрузка ресурсов
# ----------------------------------------------------------------------

def get_random_asset(dir_path: Path, exts: Tuple[str, ...]) -> Optional[Path]:
    """Возвращает случайный файл из папки с заданными расширениями."""
    files = [f for f in dir_path.iterdir() if f.suffix.lower() in exts]
    return random.choice(files) if files else None


def load_keywords() -> List[str]:
    """Загружает ключевые слова из keywords.txt, по одному на строку."""
    if not config.KEYWORDS_FILE.exists():
        return []
    return [line.strip() for line in config.KEYWORDS_FILE.read_text(encoding="utf-8").splitlines() if line.strip()]


def load_proxy() -> Optional[str]:
    """Загружает прокси из .env или возвращает None."""
    proxy = os.environ.get("PROXY")
    return proxy if proxy else None


# ----------------------------------------------------------------------
# Работа с аккаунтами и очередями загрузки
# ----------------------------------------------------------------------

def get_all_accounts() -> List[Dict]:
    """Собирает все аккаунты из ACCOUNTS_ROOT."""
    accounts = []
    for acc_dir in Path(config.ACCOUNTS_ROOT).iterdir():
        if acc_dir.is_dir():
            cfg_path = acc_dir / "config.json"
            if cfg_path.exists():
                acc_cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
                accounts.append({
                    "name": acc_dir.name,
                    "dir": acc_dir,
                    "config": acc_cfg,
                    "platforms": acc_cfg.get("platforms", [acc_cfg.get("platform", "youtube")]),
                })
    return accounts


def get_upload_queue(acc_dir: Path, platform: str) -> List[Dict]:
    """Собирает очередь загрузки для аккаунта и платформы."""
    queue_dir = acc_dir / "upload_queue" / platform
    queue = []
    for mp4 in sorted(queue_dir.glob("*.mp4")):
        meta_path = mp4.with_name(f"{mp4.stem}_meta.json")
        meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
        queue.append({"video_path": mp4, "meta": meta})
    return queue


def mark_uploaded(item: Dict) -> None:
    """Удаляет загруженное видео и метаданные."""
    item["video_path"].unlink(missing_ok=True)
    item["video_path"].with_name(f"{item['video_path'].stem}_meta.json").unlink(missing_ok=True)


def get_uploads_today(acc_dir: Path) -> int:
    """Возвращает количество загрузок сегодня для аккаунта."""
    limit_path = acc_dir / "daily_limit.json"
    if not limit_path.exists():
        return 0
    data = json.loads(limit_path.read_text(encoding="utf-8"))
    today = date.today().isoformat()
    return data.get("uploaded_today", {}).get(today, 0)


def increment_upload_count(acc_dir: Path) -> None:
    """Инкрементирует счётчик загрузок в daily_limit.json."""
    limit_path = acc_dir / "daily_limit.json"
    data = json.loads(limit_path.read_text(encoding="utf-8")) if limit_path.exists() else {}
    today = date.today().isoformat()
    uploaded_today = data.setdefault("uploaded_today", {})
    uploaded_today[today] = uploaded_today.get(today, 0) + 1
    limit_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def is_daily_limit_reached(acc_dir: Path) -> bool:
    """Проверяет, достигнут ли дневной лимит для аккаунта."""
    return get_uploads_today(acc_dir) >= config.DAILY_UPLOAD_LIMIT


def create_sample_account(name: str, platform: str) -> None:
    """Создаёт шаблон аккаунта с config.json."""
    acc_dir = Path(config.ACCOUNTS_ROOT) / name
    acc_dir.mkdir(parents=True, exist_ok=True)
    (acc_dir / "browser_profile").mkdir(parents=True, exist_ok=True)
    (acc_dir / "upload_queue").mkdir(parents=True, exist_ok=True)

    config_path = acc_dir / "config.json"
    if not config_path.exists():
        sample = {
            "platforms": ["youtube", "tiktok", "instagram"],
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


# New: Configuration Validation
def validate_config():
    errors = []
    if not config.BG_DIR.exists() or not list(config.BG_DIR.iterdir()):
        errors.append(f"BG_DIR ({config.BG_DIR}) is empty or does not exist.")
    keywords = load_keywords()
    if not keywords:
        errors.append("keywords.txt is empty or missing keywords.")
    if config.AI_ENABLED and not check_ollama():
        errors.append("Ollama is not available (check if running and model loaded).")
    if errors:
        logger.error("Configuration validation failed:\n" + "\n".join(errors))
        raise ValueError("Invalid configuration - see logs for details.")


# New: Perceptual Hashing for Duplicates
def compute_perceptual_hash(video_path: Path) -> Optional[str]:
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            hashes = []
            for i in range(3):
                frame_path = temp_path / f'frame_{i}.png'
                subprocess.run(['ffmpeg', '-i', str(video_path), '-vf', f'select=eq(n\,{i})', '-vframes', '1', str(frame_path)], check=True, capture_output=True)
                img = Image.open(frame_path)
                h = str(imagehash.phash(img))
                hashes.append(h)
            combined = ''.join(hashes)
            return hashlib.sha256(combined.encode()).hexdigest()
    except Exception as e:
        logger.error(f"Failed to compute hash for {video_path}: {e}")
        return None

def load_hashes() -> list:
    if HASH_DB.exists():
        return json.loads(HASH_DB.read_text(encoding='utf-8'))
    return []

def save_hashes(hashes: list):
    HASH_DB.parent.mkdir(parents=True, exist_ok=True)
    HASH_DB.write_text(json.dumps(hashes, ensure_ascii=False), encoding='utf-8')

def is_duplicate(video_path: Path) -> bool:
    h = compute_perceptual_hash(video_path)
    if h is None:
        return False
    hashes = load_hashes()
    if h in hashes:
        return True
    hashes.append(h)
    save_hashes(hashes)
    return False