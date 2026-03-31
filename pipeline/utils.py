"""
pipeline/utils.py – Единый набор утилит для всего проекта.
"""

import hashlib
import json
import logging
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import date, timedelta
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple, Union

import ffmpeg
from PIL import Image
import imagehash

# FIX #2: использовать rebrowser_playwright везде, не стандартный playwright
from rebrowser_playwright.sync_api import Page

from pipeline import config
from pipeline.logging_setup import setup_logger

logger = logging.getLogger(__name__)

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
        logger.error("FFmpeg не найден. Установите: https://ffmpeg.org")
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
    """
    Определяет доступный видеокодек.
    Проверяет h264_nvenc (NVIDIA GPU) через реальный тест кодирования,
    fallback на libx264 (CPU).
    """
    try:
        # Реальная проверка: пробуем закодировать 1 кадр через nvenc
        result = subprocess.run(
            [
                "ffmpeg", "-y",
                "-f", "lavfi", "-i", "color=black:s=64x64:d=0.1",
                "-c:v", "h264_nvenc", "-frames:v", "1",
                "-f", "null", "-",
            ],
            capture_output=True,
            timeout=10,
        )
        if result.returncode == 0:
            logger.info("Кодек: h264_nvenc (NVIDIA GPU)")
            return "h264_nvenc", {"preset": "p4", "cq": "23"}
    except Exception:
        pass

    logger.info("Кодек: libx264 (CPU fallback)")
    return "libx264", {"preset": "fast", "crf": "23"}


# ----------------------------------------------------------------------
# Человекоподобные задержки
# ----------------------------------------------------------------------

def human_sleep(min_sec: float, max_sec: float, **kwargs: Any) -> None:
    """
    Случайная задержка для имитации человека.
    kwargs → pipeline.humanize.human_pause (account_cfg, agent, memory, risk, context).
    """
    from pipeline.humanize import human_pause

    human_pause(min_sec, max_sec, **kwargs)


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
    if not dir_path.exists():
        return None
    files = [f for f in dir_path.iterdir() if f.suffix.lower() in exts]
    return random.choice(files) if files else None


# ── Ротация фоновых материалов (C) ───────────────────────────────────────────

_BG_USAGE_FILE = config.BASE_DIR / "data" / "bg_usage.json"
_bg_usage_lock = Lock()


def _load_bg_usage() -> Dict:
    if not _BG_USAGE_FILE.exists():
        return {}
    try:
        return json.loads(_BG_USAGE_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.debug("bg_usage.json read error: %s", e)
        return {}


def _save_bg_usage(data: Dict) -> None:
    with _bg_usage_lock:
        _BG_USAGE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _BG_USAGE_FILE.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def get_unique_bg(
    dir_path: Path,
    exts: Tuple[str, ...],
    video_stem: str,
) -> Optional[Path]:
    """
    Возвращает фоновое видео, которое ещё не использовалось для данного video_stem.
    При исчерпании всех вариантов — сбрасывает историю и начинает заново.
    """
    if not dir_path.exists():
        return None
    files = [f for f in dir_path.iterdir() if f.suffix.lower() in exts]
    if not files:
        return None

    usage = _load_bg_usage()
    used  = set(usage.get(video_stem, []))

    # Доступные — те, что ещё не использовались
    available = [f for f in files if f.name not in used]

    # Если всё использовано — сбрасываем историю для этого стема
    if not available:
        used = set()
        available = files

    chosen = random.choice(available)

    # Записываем использование
    used.add(chosen.name)
    usage[video_stem] = list(used)
    _save_bg_usage(usage)

    return chosen


def load_keywords() -> List[str]:
    """Загружает ключевые слова из keywords.txt, по одному на строку.
    Пустые строки и комментарии (# …) пропускаются.
    """
    if not config.KEYWORDS_FILE.exists():
        return []
    out: List[str] = []
    for line in config.KEYWORDS_FILE.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)
    return out


def proxy_cfg_to_url(proxy_cfg: dict) -> str:
    """
    Строка URL прокси из dict host/port/username/password/scheme.
    scheme по умолчанию: http.
    """
    from urllib.parse import quote

    host = proxy_cfg["host"]
    port = int(proxy_cfg.get("port", 8080))
    scheme = (proxy_cfg.get("scheme") or "http").strip().lower()
    user = (proxy_cfg.get("username") or "").strip()
    pwd = (proxy_cfg.get("password") or "").strip()
    if user:
        return (
            f"{scheme}://{quote(user, safe='')}:{quote(pwd, safe='')}@{host}:{port}"
        )
    return f"{scheme}://{host}:{port}"


def proxy_cfg_to_http_url(proxy_cfg: dict) -> str:
    """
    Backward-compatible alias: строит URL прокси из dict.
    """
    return proxy_cfg_to_url(proxy_cfg)


def proxy_url_to_cfg(proxy_url: str) -> Optional[Dict[str, Any]]:
    """
    Разбирает URL прокси (http/socks5/socks5h) в dict.
    """
    from urllib.parse import unquote, urlparse

    raw = (proxy_url or "").strip()
    if not raw:
        return None
    p = urlparse(raw)
    scheme = (p.scheme or "").lower().strip()
    if scheme not in ("http", "https", "socks5", "socks5h"):
        return None
    if not p.hostname or not p.port:
        return None
    out: Dict[str, Any] = {"host": p.hostname, "port": int(p.port), "scheme": scheme}
    if p.username:
        out["username"] = unquote(p.username)
    if p.password:
        out["password"] = unquote(p.password)
    return out


def _accounts_root_path() -> Path:
    r = Path(config.ACCOUNTS_ROOT)
    return r if r.is_absolute() else (Path(config.BASE_DIR) / r)


def resolve_pipeline_account_name() -> Optional[str]:
    """
    Имя каталога в accounts/ для подготовки контента: поиск (downloader),
    cookies yt-dlp (download), TrendScout YouTube.

    Приоритет:
      1) SHORTS_PIPELINE_ACCOUNT или YTDLP_COOKIES_ACCOUNT (фиксированный аккаунт);
      2) контекст цикла SCOUT при PIPELINE_ACCOUNT_ROTATION=1 (см. pipeline_account_rotation).
    """
    raw = (
        os.getenv("SHORTS_PIPELINE_ACCOUNT", "").strip()
        or os.getenv("YTDLP_COOKIES_ACCOUNT", "").strip()
    )
    if raw:
        acc_dir = _accounts_root_path() / raw
        if not (acc_dir / "config.json").is_file():
            logger.warning(
                "SHORTS_PIPELINE_ACCOUNT / YTDLP_COOKIES_ACCOUNT=%s: нет %s",
                raw,
                acc_dir / "config.json",
            )
            return None
        return raw

    try:
        from pipeline.pipeline_account_rotation import get_active_pipeline_account_name

        active = get_active_pipeline_account_name()
        if active:
            return active
    except Exception as exc:
        logger.debug("resolve_pipeline_account_name: контекст ротации: %s", exc)

    return None


def get_pipeline_account_bundle() -> Optional[Dict[str, Any]]:
    """
    Словарь с ключами name, dir, config — конфиг залогиненного аккаунта для пайплайна подготовки.
    """
    name = resolve_pipeline_account_name()
    if not name:
        return None
    acc_dir = _accounts_root_path() / name
    cfg_path = acc_dir / "config.json"
    try:
        acc_cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("get_pipeline_account_bundle: не читается %s: %s", cfg_path, exc)
        return None
    return {"name": name, "dir": acc_dir, "config": acc_cfg}


def requests_proxies_from_proxy_url(proxy_url: Optional[str]) -> Optional[Dict[str, str]]:
    """Словарь proxies для requests / urllib, если задан URL прокси (как у load_proxy)."""
    if not proxy_url:
        return None
    return {"http": proxy_url, "https": proxy_url}


def load_proxy() -> Optional[str]:
    """
    Прокси для yt-dlp, download.py, SCOUT, downloader.search_and_save.

    Приоритет:
      1. Переменная окружения PROXY (явный override).
      2. Иначе — прокси из mobileproxy API (MOBILEPROXY_API_KEY + MOBILEPROXY_PROXY_ID)
         и кэш data/mobileproxy_http_cache.json (см. mobileproxy_connection).
    """
    explicit = (os.environ.get("PROXY") or "").strip()
    if explicit:
        return explicit
    try:
        from pipeline.mobileproxy_connection import fetch_mobileproxy_http_proxy

        p = fetch_mobileproxy_http_proxy(force_refresh=False, use_cache_on_api_fail=True)
        if p and p.get("host"):
            return proxy_cfg_to_url(p)
    except Exception as exc:
        logger.debug("load_proxy: mobileproxy недоступен: %s", exc)
    return None


def check_proxy_health(proxy_cfg: dict, timeout: int = 10) -> bool:
    """
    Проверяет работоспособность прокси из конфига аккаунта.

    proxy_cfg — словарь вида:
        {"host": "...", "port": 8080, "username": "...", "password": "..."}

    Возвращает True если прокси отвечает, False если недоступен.
    """
    import requests

    if not proxy_cfg or not proxy_cfg.get("host"):
        return True  # прокси не настроен — считаем OK

    host = proxy_cfg["host"]
    port = proxy_cfg.get("port", 8080)
    proxy_url = proxy_cfg_to_url(proxy_cfg)
    proxies = {"http": proxy_url, "https": proxy_url}

    try:
        _ = requests.get(
            "http://httpbin.org/ip",
            headers={"User-Agent": "Mozilla/5.0"},
            proxies=proxies,
            timeout=timeout,
        )
        return True
    except Exception as exc:
        get_logger("utils").warning(
            "Прокси %s:%s недоступен: %s", host, port, exc
        )
        return False


def fetch_exit_ip_via_proxy(proxy_cfg: dict, timeout: float = 15.0) -> Optional[str]:
    """
    Внешний IPv4/IPv6 через HTTP-прокси (httpbin.org/ip).
    Используется реестром exit-IP без импорта browser.py.
    """
    import requests

    if not proxy_cfg or not proxy_cfg.get("host"):
        return None
    proxy_url = proxy_cfg_to_url(proxy_cfg)
    proxies = {"http": proxy_url, "https": proxy_url}
    try:
        resp = requests.get(
            "http://httpbin.org/ip",
            headers={"User-Agent": "Mozilla/5.0"},
            proxies=proxies,
            timeout=timeout,
        )
        data = resp.json()
        origin = data.get("origin", "") or ""
        return origin.split(",")[0].strip() or None
    except Exception as exc:
        get_logger("utils").debug("fetch_exit_ip_via_proxy: %s", exc)
        return None


def fetch_country_for_ip(external_ip: str, timeout: float = 8.0) -> Optional[str]:
    """countryCode по IP (ip-api.com), без прокси."""
    import json as _json
    import urllib.request

    if not external_ip:
        return None
    try:
        geo_req = urllib.request.Request(
            f"http://ip-api.com/json/{external_ip}?fields=countryCode",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urllib.request.urlopen(geo_req, timeout=timeout) as resp:
            geo_data = _json.loads(resp.read().decode())
            return (geo_data.get("countryCode") or "").upper() or None
    except Exception:
        return None


def unique_lines(path: Path) -> List[str]:
    """Читает файл и возвращает уникальные непустые строки (порядок сохраняется)."""
    if not path.exists():
        return []
    seen = set()
    result = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and line not in seen:
            seen.add(line)
            result.append(line)
    return result


def merge_and_save_urls(new_urls: List[str], urls_file: Path) -> int:
    """Добавляет новые URL в файл (без дубликатов). Возвращает количество добавленных."""
    existing = set(unique_lines(urls_file))
    to_add = [u for u in new_urls if u not in existing]
    if to_add:
        with urls_file.open("a", encoding="utf-8") as f:
            f.write("\n".join(to_add) + "\n")
    return len(to_add)


# ----------------------------------------------------------------------
# Работа с аккаунтами и очередями загрузки
# ----------------------------------------------------------------------

def sort_accounts_by_country(accounts: List[Dict]) -> List[Dict]:
    """
    Стабильная сортировка: ISO country (config), затем имя папки.
    Аккаунты без country — в конце (меньше переключений change_equipment при общем прокси).
    """
    def key(acc: Dict) -> tuple:
        c = (acc.get("config") or {}).get("country") or ""
        cu = str(c).upper().strip()
        return (cu if cu else "\uffff", acc.get("name") or "")
    return sorted(accounts, key=key)


def get_all_accounts() -> List[Dict]:
    """Собирает все аккаунты из ACCOUNTS_ROOT."""
    accounts = []
    root = Path(config.ACCOUNTS_ROOT)
    if not root.exists():
        return accounts
    for acc_dir in root.iterdir():
        if acc_dir.is_dir() and not acc_dir.is_symlink():
            cfg_path = acc_dir / "config.json"
            if cfg_path.exists():
                acc_cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
                accounts.append({
                    "name": acc_dir.name,
                    "dir": acc_dir,
                    "config": acc_cfg,
                    "platforms": acc_cfg.get("platforms", [acc_cfg.get("platform", "youtube")]),
                })
    if getattr(config, "SHORTS_ACCOUNT_ORDER_BY_COUNTRY", True):
        accounts = sort_accounts_by_country(accounts)
    return accounts


def get_upload_queue(acc_dir: Path, platform: str) -> List[Dict]:
    """Собирает очередь загрузки для аккаунта и платформы."""
    queue_dir = acc_dir / "upload_queue" / platform
    queue = []
    if not queue_dir.exists():
        return queue
    for mp4 in sorted(queue_dir.glob("*.mp4")):
        meta_path = mp4.with_name(f"{mp4.stem}_meta.json")
        meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
        queue.append({"video_path": mp4, "meta": meta})
    return queue


def mark_uploaded(item: Dict) -> None:
    """Удаляет загруженное видео и метаданные."""
    item["video_path"].unlink(missing_ok=True)
    item["video_path"].with_name(f"{item['video_path'].stem}_meta.json").unlink(missing_ok=True)


def get_uploads_today(acc_dir: Path, platform: Optional[str] = None) -> int:
    """
    Возвращает количество загрузок сегодня для аккаунта.
    Если platform задан — только по этой платформе (fix: раньше считалось суммарно).
    """
    limit_path = acc_dir / "daily_limit.json"
    if not limit_path.exists():
        return 0
    data = json.loads(limit_path.read_text(encoding="utf-8"))
    today = date.today().isoformat()
    if platform:
        # Per-platform ключ: "uploaded_today_youtube", "uploaded_today_tiktok" и т.д.
        val = data.get(f"uploaded_today_{platform}")
        if not isinstance(val, dict):
            return 0
        return val.get(today, 0)
    # Обратная совместимость — суммарно по всем платформам
    val = data.get("uploaded_today")
    if not isinstance(val, dict):
        return 0
    return val.get(today, 0)


def increment_upload_count(acc_dir: Path, platform: Optional[str] = None) -> None:
    """Инкрементирует счётчик загрузок в daily_limit.json (глобальный + per-platform)."""
    limit_path = acc_dir / "daily_limit.json"
    data = json.loads(limit_path.read_text(encoding="utf-8")) if limit_path.exists() else {}
    today = date.today().isoformat()
    # Глобальный счётчик (для обратной совместимости)
    uploaded_today = data.setdefault("uploaded_today", {})
    uploaded_today[today] = uploaded_today.get(today, 0) + 1
    # Per-platform счётчик
    if platform:
        plat_key = f"uploaded_today_{platform}"
        plat_today = data.setdefault(plat_key, {})
        plat_today[today] = plat_today.get(today, 0) + 1
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
    except (FileNotFoundError, json.JSONDecodeError, OSError) as e:
        logger.debug("load_json error for %s: %s", path, e)
        return None


def save_json(path: Path, data: Dict) -> None:
    """Сохраняет словарь в JSON-файл."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ----------------------------------------------------------------------
# Имена папок вывода (Ready-made_shorts)
# ----------------------------------------------------------------------

def safe_output_folder_name(stem: str, max_len: int = 48) -> str:
    """
    Безопасное короткое имя папки для Windows (без : * ? и т.д.).
    Длинные идентификаторы (TikTok и т.п.) обрезаются с суффиксом из хэша.
    """
    import re

    s = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", stem).strip(" .")
    if not s:
        s = "video"
    if len(s) <= max_len:
        return s
    digest = hashlib.sha256(stem.encode("utf-8")).hexdigest()[:8]
    return f"{s[: max_len - 9]}_{digest}"


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


# FIX #3: validate_config — ленивый импорт check_ollama, чтобы избежать
# кольцевого импорта и NameError (check_ollama живёт в ai.py, не здесь)
def validate_config() -> None:
    """Проверяет корректность конфигурации перед запуском пайплайна."""
    errors = []
    if not config.BG_DIR.exists() or not any(config.BG_DIR.iterdir()):
        errors.append(f"BG_DIR ({config.BG_DIR}) пуст или не существует.")
    keywords = load_keywords()
    if not keywords:
        errors.append("keywords.txt пуст или не содержит ключевых слов.")
    if config.AI_ENABLED:
        try:
            from pipeline.ai import check_ollama  # ленивый импорт — избегаем цикл
            if not check_ollama():
                errors.append("Ollama недоступен (проверьте, запущен ли сервис и загружена ли модель).")
        except ImportError as exc:
            errors.append(f"Не удалось импортировать pipeline.ai: {exc}")
    if errors:
        logger.error("Проверка конфигурации не прошла:\n%s", "\n".join(errors))
        raise ValueError("Некорректная конфигурация — см. лог для деталей.")


# ----------------------------------------------------------------------
# Перцептивное хеширование для определения дублей
# ----------------------------------------------------------------------

def _get_video_duration(video_path: Path) -> Optional[float]:
    """Возвращает длительность видео в секундах через ffprobe."""
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(video_path),
            ],
            check=True, capture_output=True, text=True,
        )
        return float(result.stdout.strip())
    except Exception as e:
        logger.warning("ffprobe не смог определить длину %s: %s", video_path, e)
        return None


def compute_perceptual_hash(video_path: Path) -> Optional["imagehash.ImageHash"]:
    """
    Вычисляет perceptual hash по кадрам видео.
    Возвращает усреднённый ImageHash объект для сравнения по расстоянию Хэмминга.
    """
    import numpy as np
    try:
        duration = _get_video_duration(video_path)
        interval = config.DEDUP_FRAME_INTERVAL_SEC

        timestamps: list = []
        if duration and duration > 0:
            t = interval / 2.0
            while t < duration:
                timestamps.append(t)
                t += interval
        if not timestamps:
            timestamps = [duration / 2.0] if duration else [0.5]

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path    = Path(tmp)
            frame_hashes: list = []
            for idx, ts in enumerate(timestamps):
                frame_path = tmp_path / f"frame_{idx}.png"
                subprocess.run(
                    ["ffmpeg", "-ss", str(ts), "-i", str(video_path),
                     "-vframes", "1", "-q:v", "2", str(frame_path)],
                    check=True, capture_output=True,
                )
                if frame_path.exists():
                    img = Image.open(frame_path)
                    frame_hashes.append(imagehash.phash(img))

            if not frame_hashes:
                logger.warning("Не удалось извлечь ни одного кадра из %s", video_path)
                return None

            # Усредняем хэши через numpy
            arrays    = np.array([h.hash.flatten() for h in frame_hashes], dtype=float)
            avg_array = (arrays.mean(axis=0) >= 0.5).reshape(8, 8)
            return imagehash.ImageHash(avg_array)

    except Exception as e:
        logger.error("Не удалось вычислить хеш для %s: %s", video_path, e)
        return None


def load_hashes() -> List[str]:
    """Загружает список hex-строк phash из HASH_DB."""
    if HASH_DB.exists():
        try:
            return json.loads(HASH_DB.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            logger.debug("load_hashes error: %s", e)
            return []
    return []


def save_hashes(hashes: List[str]) -> None:
    HASH_DB.parent.mkdir(parents=True, exist_ok=True)
    HASH_DB.write_text(json.dumps(hashes, ensure_ascii=False), encoding="utf-8")


def is_duplicate(video_path: Path) -> bool:
    """
    Возвращает True если видео похоже на уже обработанное.
    Сравнение по расстоянию Хэмминга (config.DEDUP_HAMMING_THRESHOLD).
    Порог 10 из 64 бит — находит похожие клоны, не только точные копии.
    """
    new_hash = compute_perceptual_hash(video_path)
    if new_hash is None:
        return False

    threshold = config.DEDUP_HAMMING_THRESHOLD
    stored    = load_hashes()

    for hex_str in stored:
        try:
            if (new_hash - imagehash.hex_to_hash(hex_str)) <= threshold:
                logger.info(
                    "Дубликат: %s (расстояние Хэмминга ≤ %d)", video_path.name, threshold
                )
                return True
        except Exception:
            continue

    stored.append(str(new_hash))
    save_hashes(stored)
    return False

