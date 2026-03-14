# ai.py
import concurrent.futures
import hashlib
import json
import logging
import re
import subprocess
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import cv2
import ollama
import requests

from pipeline.config import (
    OLLAMA_MODEL, AI_NUM_FRAMES,
    AI_NUM_VARIANTS, OLLAMA_TIMEOUT, HASHTAGS_FILE,
    OVERLAY_DEFAULT_DURATION,
    OLLAMA_AUTOSTART, OLLAMA_AUTOSTART_WAIT_SEC,
    CLIP_MIN_LEN, CLIP_MAX_LEN,
    VL_CACHE_FILE,
)
from pipeline.utils import save_json

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# VL-кеш (CURATOR + SCOUT)
# Файл: data/vl_cache.json  |  Ключи: sha256 (видео) или "yt_{video_id}"
# ─────────────────────────────────────────────────────────────────────────────

_VL_CACHE_LOCK = threading.Lock()


def _file_hash(path: Path) -> str:
    """SHA-256 первых 8 KB файла — быстрый уникальный идентификатор."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        h.update(f.read(8192))
    return h.hexdigest()


def _vl_cache_get(key: str) -> Optional[Dict]:
    """Читает запись из VL-кеша. Возвращает None если нет."""
    try:
        with _VL_CACHE_LOCK:
            if not VL_CACHE_FILE.exists():
                return None
            return json.loads(VL_CACHE_FILE.read_text(encoding="utf-8")).get(key)
    except Exception:
        return None


def _vl_cache_set(key: str, value: Dict) -> None:
    """Атомарно записывает запись в VL-кеш."""
    try:
        with _VL_CACHE_LOCK:
            VL_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            cache: Dict = {}
            if VL_CACHE_FILE.exists():
                try:
                    cache = json.loads(VL_CACHE_FILE.read_text(encoding="utf-8"))
                except Exception:
                    pass
            cache[key] = value
            VL_CACHE_FILE.write_text(
                json.dumps(cache, ensure_ascii=False, indent=None),
                encoding="utf-8",
            )
    except Exception as e:
        logger.warning("VL cache write error: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Управление Ollama (автозапуск)
# ─────────────────────────────────────────────────────────────────────────────

def _try_start_ollama() -> None:
    """Пытается запустить 'ollama serve' в фоне если OLLAMA_AUTOSTART включён."""
    if not OLLAMA_AUTOSTART:
        return
    try:
        subprocess.Popen(
            ["ollama", "serve"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        logger.info("Ollama запущен автоматически, ожидаю %d сек...", OLLAMA_AUTOSTART_WAIT_SEC)
        time.sleep(OLLAMA_AUTOSTART_WAIT_SEC)
    except FileNotFoundError:
        logger.warning("Команда 'ollama' не найдена — установите Ollama: https://ollama.com")
    except Exception as exc:
        logger.warning("Не удалось запустить Ollama: %s", exc)


_check_ollama_cache: dict = {"result": None, "ts": 0.0}
_CHECK_OLLAMA_TTL = 60.0   # повторная проверка не чаще раза в минуту
_check_ollama_lock = threading.Lock()


def check_ollama() -> bool:
    """
    Проверяет доступность Ollama и VL-модели.
    Результат кешируется на 60 сек — защита от спама при параллельных агентах.
    При недоступности — пытается запустить автоматически (OLLAMA_AUTOSTART).
    """
    with _check_ollama_lock:
        now = time.monotonic()
        if _check_ollama_cache["result"] is not None and (now - _check_ollama_cache["ts"]) < _CHECK_OLLAMA_TTL:
            return _check_ollama_cache["result"]

    def _probe() -> bool:
        try:
            response = requests.get("http://localhost:11434/api/tags", timeout=5)
            if response.status_code != 200:
                return False
            models = response.json().get("models", [])
            model_names = [m["name"] for m in models]
            model_base  = OLLAMA_MODEL.split(':')[0]
            return any(model_base in mn for mn in model_names)
        except Exception:
            return False

    result = _probe()
    if not result:
        _try_start_ollama()
        result = _probe()

    with _check_ollama_lock:
        _check_ollama_cache["result"] = result
        _check_ollama_cache["ts"]     = time.monotonic()
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Хэштеги
# ─────────────────────────────────────────────────────────────────────────────

def load_trending_hashtags() -> List[str]:
    """Загружает трендовые хэштеги из файла."""
    hashtags_path = HASHTAGS_FILE
    if not hashtags_path.exists():
        return []
    return [
        line.strip() for line in hashtags_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Извлечение кадров
# ─────────────────────────────────────────────────────────────────────────────

def extract_frames(video_path: Path, num_frames: int = AI_NUM_FRAMES) -> List[bytes]:
    """Извлекает равномерно распределённые кадры из видео (raw JPEG bytes для VL-модели)."""
    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        raise ValueError(f"Не удалось открыть видео: {video_path}")

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    step = max(1, total_frames // num_frames)
    frames = []
    for i in range(num_frames):
        frame_num = min(i * step, total_frames - 1)
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_num)
        ret, frame = cap.read()
        if not ret:
            break
        _, buf = cv2.imencode('.jpg', frame)
        frames.append(buf.tobytes())
    cap.release()
    return frames


# ─────────────────────────────────────────────────────────────────────────────
# Генерация через Ollama VL
# ─────────────────────────────────────────────────────────────────────────────

def ollama_generate_with_timeout(
    model: str,
    prompt: str,
    images: Optional[List[bytes]] = None,
    timeout: int = OLLAMA_TIMEOUT,
) -> Dict:
    """Генерирует ответ Ollama VL-модели с таймаутом.

    Args:
        model:   название модели (напр. 'qwen2.5-vl:7b')
        prompt:  текстовый промпт
        images:  список JPEG-байт кадров для VL-анализа (опционально)
        timeout: таймаут в секундах
    """
    kwargs: Dict = dict(model=model, prompt=prompt)
    if images:
        kwargs["images"] = images
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(ollama.generate, **kwargs)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            raise TimeoutError(f"Ollama timeout after {timeout}s")


def generate_video_metadata(
    video_path: Path,
    trending_hashtags: Optional[List[str]] = None,
    num_variants: int = AI_NUM_VARIANTS,
) -> List[Dict]:
    """Генерирует метаданные для видео через Ollama VL — модель видит реальные кадры."""
    cache_path = video_path.with_suffix('.ai_cache.json')
    if cache_path.exists():
        try:
            return json.loads(cache_path.read_text(encoding='utf-8'))
        except Exception:
            logger.warning("Невалидный кеш для %s, пересоздаю.", video_path)

    if not check_ollama():
        logger.warning("Ollama недоступен — fallback метаданные.")
        return _fallback_meta(video_path, num_variants)

    try:
        frames = extract_frames(video_path)

        hashtag_hint = ""
        if trending_hashtags:
            hashtag_hint = f"Контекст и ключевые темы: {', '.join(trending_hashtags[:10])}\n"

        prompt = (
            f"Ты анализируешь вертикальное короткое видео (YouTube Shorts / TikTok / Reels).\n"
            f"Тебе показаны {len(frames)} равномерно распределённых кадров из видео.\n"
            f"{hashtag_hint}"
            f"Создай {num_variants} варианта метаданных для вирального Shorts.\n\n"
            "Требования к hook_text: интрига, вопрос или неожиданный факт — 3–7 слов.\n"
            "Требования к title: цепляет с первых слов, без 'смотри как' и 'это видео'.\n\n"
            "Ответ — ТОЛЬКО валидный JSON-массив (без markdown, без пояснений):\n"
            '[\n'
            '  {\n'
            '    "title": "заголовок до 60 символов",\n'
            '    "description": "описание с эмодзи до 150 символов",\n'
            '    "tags": ["тег1", "тег2"],\n'
            '    "thumbnail_idea": "идея для превью",\n'
            '    "hook_text": "текст первые 3 сек (3-7 слов)",\n'
            '    "best_segment": <секунды или null>,\n'
            '    "overlays": [{"text": "...", "start": 0, "duration": 2}],\n'
            '    "loop_prompt": "фраза для петли"\n'
            '  }\n'
            ']'
        )

        response = ollama_generate_with_timeout(OLLAMA_MODEL, prompt, images=frames)
        raw = response["response"].strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        variants = json.loads(raw)

        save_json(cache_path, variants)
        return variants
    except Exception as e:
        logger.error("Ошибка AI для %s: %s — использую fallback.", video_path.name, e)
        return _fallback_meta(video_path, num_variants)


def generate_cut_points(
    video_path: Path,
    duration: float,
    num_frames: int = AI_NUM_FRAMES,
    silences: Optional[List[float]] = None,
) -> List[float]:
    """Определяет точки нарезки видео через VL-модель — модель видит кадры."""
    silences_str = (
        f"\nТихие паузы (секунды): {', '.join(f'{s:.1f}' for s in silences)}"
        if silences else ""
    )

    try:
        frames = extract_frames(video_path, num_frames)
    except Exception as e:
        logger.warning("Не удалось извлечь кадры для cut points: %s", e)
        frames = []

    prompt = (
        f"Видео длительностью {duration:.1f} секунд.\n"
        f"Тебе показаны {len(frames)} равномерно распределённых кадров.\n"
        f"Нужно нарезать на клипы по {CLIP_MIN_LEN:.0f}–{CLIP_MAX_LEN:.0f} секунд.{silences_str}\n"
        "Найди лучшие точки реза: смена сцены, логический переход, завершение действия.\n"
        "Предпочитай резать в тихих паузах (если есть).\n"
        "Ответ: ТОЛЬКО числа секунд, по одному на строке. Никакого другого текста."
    )

    response = ollama_generate_with_timeout(
        OLLAMA_MODEL,
        prompt,
        images=frames if frames else None,
    )
    return _parse_timestamps(response["response"])


def _parse_timestamps(text: str) -> List[float]:
    """Извлекает числа-секунды из ответа Ollama."""
    numbers = re.findall(r"\b(\d+(?:\.\d+)?)\b", text)
    return sorted(float(n) for n in numbers)


def _fallback_meta(video_path: Path, num_variants: int) -> List[Dict]:
    """Возвращает заглушку метаданных при недоступном AI."""
    base = {
        "title":          f"Amazing Short #{video_path.stem[:40]}",
        "description":    "Subscribe for more! 🔔 #shorts #viral #trending",
        "tags":           ["shorts", "viral", "trending"],
        "thumbnail_idea": "A key moment from the video.",
        "hook_text":      "",
        "best_segment":   None,
        "overlays":       [],
        "loop_prompt":    "",
    }
    return [base] * num_variants


# ─────────────────────────────────────────────────────────────────────────────
# VL-фильтрация контента
# ─────────────────────────────────────────────────────────────────────────────

def vl_quality_check_video(
    video_path: Path,
    num_frames: int = 4,
) -> Tuple[bool, str]:
    """
    CURATOR: VL-оценка качества видео перед обработкой.

    Результат кешируется по SHA-256 первых 8 KB файла — повторный вызов
    для того же видео бесплатен (без GPU).

    Returns:
        (True, "ok")          — видео пригодно
        (False, "причина")    — отбраковано
        (True, "vl_error")    — ошибка VL, пропускаем (default PASS)
    """
    file_hash = _file_hash(video_path)
    cached = _vl_cache_get(file_hash)
    if cached is not None:
        return cached["result"] == "PASS", cached["reason"]

    try:
        frames = extract_frames(video_path, num_frames)
    except Exception as e:
        logger.warning("[VL-CURATOR] Не удалось извлечь кадры из %s: %s", video_path.name, e)
        return True, "extract_failed"

    prompt = (
        "Оцени пригодность этого видео для публикации как Shorts/Reels.\n"
        "Отбракуй (REJECT) если: статичное изображение выдаётся за видео, "
        "скучный скринкаст без действия, только слайды с текстом, "
        "сильное размытие на всех кадрах, зациклённый 2-3 секундный клип.\n"
        "Всё остальное — одобряй (PASS).\n"
        "Ответ: ТОЛЬКО 'PASS' или 'REJECT' и одно слово-причина через пробел."
    )

    try:
        response = ollama_generate_with_timeout(OLLAMA_MODEL, prompt, images=frames)
        raw     = response["response"].strip().upper()
        passed  = raw.startswith("PASS")
        parts   = raw.split()
        reason  = parts[1].lower() if len(parts) > 1 else ("ok" if passed else "low_quality")
        _vl_cache_set(file_hash, {"result": "PASS" if passed else "REJECT", "reason": reason})
        logger.debug("[VL-CURATOR] %s → %s (%s)", video_path.name, "PASS" if passed else "REJECT", reason)
        return passed, reason
    except Exception as e:
        logger.warning("[VL-CURATOR] Ошибка для %s: %s — default PASS", video_path.name, e)
        return True, "vl_error"


def vl_score_thumbnail(video_url: str) -> Optional[int]:
    """
    SCOUT: Оценивает thumbnail YouTube-видео через VL (1–10).

    Для не-YouTube URL возвращает None (URL добавляется без фильтрации).
    Результат кешируется по video_id — повторный поиск по тому же видео
    не тратит GPU.

    Returns:
        int 1–10  — оценка контента
        None      — не YouTube или ошибка fetch
    """
    match = re.search(r"(?:v=|youtu\.be/)([a-zA-Z0-9_-]{11})", video_url)
    if not match:
        return None

    video_id  = match.group(1)
    cache_key = f"yt_{video_id}"

    cached = _vl_cache_get(cache_key)
    if cached is not None:
        return cached.get("score")

    thumbnail_url = f"https://img.youtube.com/vi/{video_id}/hqdefault.jpg"
    try:
        resp = requests.get(thumbnail_url, timeout=10)
        if resp.status_code != 200:
            return None
        img_bytes = resp.content
    except Exception as e:
        logger.warning("[VL-SCOUT] Не удалось загрузить thumbnail %s: %s", video_id, e)
        return None

    prompt = (
        "Оцени превью видео для отбора контента для Shorts/Reels.\n"
        "Повышай оценку за: динамичность, эмоции, интересное действие, качество.\n"
        "Снижай оценку за: скучный статичный контент, скринкасты, слайды, размытие.\n"
        "Ответ: ТОЛЬКО целое число от 1 до 10."
    )

    try:
        response = ollama_generate_with_timeout(OLLAMA_MODEL, prompt, images=[img_bytes])
        raw      = response["response"].strip()
        numbers  = re.findall(r"\b(\d+)\b", raw)
        score    = max(1, min(10, int(numbers[0]))) if numbers else 5
        _vl_cache_set(cache_key, {"score": score})
        logger.debug("[VL-SCOUT] %s → score=%d", video_id, score)
        return score
    except Exception as e:
        logger.warning("[VL-SCOUT] Ошибка оценки thumbnail %s: %s", video_id, e)
        return None
