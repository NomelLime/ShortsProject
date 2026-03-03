# ai.py
# FIX #4: все импорты перенесены в начало файла (PEP 8).
#         Добавлены json, re, threading (были пропущены).
#         save_json импортируется из utils.
import concurrent.futures
import json
import logging
import random
import re
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import cv2
import ollama
import requests
from ultralytics import YOLO

from pipeline.config import (
    OLLAMA_MODEL, YOLO_MODEL_PT, AI_NUM_FRAMES,
    AI_NUM_VARIANTS, OLLAMA_TIMEOUT, HASHTAGS_FILE,
    OVERLAY_DEFAULT_DURATION,
    OLLAMA_AUTOSTART, OLLAMA_AUTOSTART_WAIT_SEC,
    CLIP_MIN_LEN, CLIP_MAX_LEN,
)
from pipeline.utils import save_json

logger = logging.getLogger(__name__)

# FIX #18 (ai): double-checked locking — безопасная ленивая инициализация YOLO
_yolo_model: Optional[YOLO] = None
_yolo_lock = threading.Lock()


def load_yolo() -> YOLO:
    """Загружает модель YOLO (глобально, с кэшированием и thread-safe блокировкой)."""
    global _yolo_model
    if _yolo_model is None:
        with _yolo_lock:
            if _yolo_model is None:   # double-checked locking
                logger.info("Загрузка YOLO модели...")
                _yolo_model = YOLO(YOLO_MODEL_PT)
    return _yolo_model


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


def check_ollama() -> bool:
    """
    Проверяет доступность Ollama и модели.
    При недоступности — пытается запустить автоматически (OLLAMA_AUTOSTART).
    """
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

    if _probe():
        return True
    _try_start_ollama()
    return _probe()


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

def extract_frames(video_path: Path, num_frames: int = AI_NUM_FRAMES) -> List[str]:
    """Извлекает равномерно распределённые кадры из видео."""
    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        raise ValueError(f"Не удалось открыть видео: {video_path}")

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS)

    step = max(1, total_frames // num_frames)
    frames = []
    for i in range(num_frames):
        frame_num = min(i * step, total_frames - 1)
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_num)
        ret, frame = cap.read()
        if not ret:
            break
        _, buf = cv2.imencode('.jpg', frame)
        frames.append(buf.tobytes().hex())
    cap.release()
    return frames


# ─────────────────────────────────────────────────────────────────────────────
# YOLO-детекция
# ─────────────────────────────────────────────────────────────────────────────

def run_yolo_on_frames(frames: List[str]) -> List[List[str]]:
    """Запускает YOLO на кадрах параллельно."""
    model = load_yolo()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(_detect_single_frame, model, f) for f in frames]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
    return results


def _detect_single_frame(model: YOLO, frame_hex: str) -> List[str]:
    frame_bytes = bytes.fromhex(frame_hex)
    results = model(frame_bytes, verbose=False)
    detections = []
    for r in results:
        for box in r.boxes:
            cls = int(box.cls)
            conf = float(box.conf)
            if conf > 0.5:
                detections.append(f"{model.names[cls]} ({conf:.2f})")
    return list(set(detections))


# ─────────────────────────────────────────────────────────────────────────────
# Генерация метаданных через Ollama
# ─────────────────────────────────────────────────────────────────────────────

def ollama_generate_with_timeout(model: str, prompt: str, timeout: int = OLLAMA_TIMEOUT) -> Dict:
    """Генерирует ответ Ollama с таймаутом."""
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(ollama.generate, model=model, prompt=prompt)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            raise TimeoutError(f"Ollama timeout after {timeout}s")


def generate_video_metadata(
    video_path: Path,
    trending_hashtags: Optional[List[str]] = None,
    num_variants: int = AI_NUM_VARIANTS,
) -> List[Dict]:
    """Генерирует несколько вариантов метаданных через Ollama + YOLO."""
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
        yolo_per_frame = run_yolo_on_frames(frames)

        hashtag_hint = ""
        if trending_hashtags:
            hashtag_hint = f"Trending hashtags to consider: {', '.join(trending_hashtags[:10])}\n"

        prompt = (
            f"Analyze this video based on {len(frames)} frames and YOLO detections.\n"
            f"Frames descriptions: {', '.join([f'Frame {i+1}: objects {d}' for i, d in enumerate(yolo_per_frame)])}\n"
            f"{hashtag_hint}"
            f"Generate {num_variants} variants of metadata for viral Shorts video:\n"
            "- title: Catchy title (under 60 chars)\n"
            "- description: Engaging description with emojis (under 150 chars)\n"
            "- tags: 5-10 trending tags\n"
            "- thumbnail_idea: Description for thumbnail\n"
            "- hook_text: Opening text overlay (short phrase)\n"
            "- best_segment: Start time of best 3-10s segment\n"
            "- overlays: List of timed text overlays [{text, start, duration}]\n"
            "- loop_prompt: End prompt to loop video\n"
            "Output as JSON list of dicts. Return ONLY valid JSON, no markdown."
        )

        response = ollama_generate_with_timeout(OLLAMA_MODEL, prompt)
        raw = response["response"].strip()
        # Убираем возможные markdown-заборы
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
    yolo_per_frame: List[List[str]],
    num_frames: int = AI_NUM_FRAMES,
    silences: Optional[List[float]] = None,
) -> List[float]:
    """Запрашивает у AI точки нарезки видео."""
    silences_str = (
        f"Silence pauses at seconds: {', '.join(f'{s:.1f}' for s in silences)}"
        if silences else ""
    )
    prompt = (
        f"Suggest cut points for {duration:.1f}s video into {CLIP_MIN_LEN}-{CLIP_MAX_LEN}s clips.\n"
        f"Frames: {', '.join([f'Frame {i+1}: {d}' for i, d in enumerate(yolo_per_frame)])}\n"
        f"{silences_str}\n"
        "Prefer cuts during silences to avoid mid-sentence. "
        "Output ONLY a list of timestamps in seconds, one per line."
    )
    response = ollama_generate_with_timeout(OLLAMA_MODEL, prompt)
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
