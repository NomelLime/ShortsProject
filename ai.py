# ai.py
import concurrent.futures
import logging
import random
import subprocess
import tempfile
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

logger = logging.getLogger(__name__)

_yolo_model = None


def load_yolo() -> YOLO:
    """Загружает модель YOLO (глобально, с кэшированием)."""
    global _yolo_model
    if _yolo_model is None:
        logger.info("🔄 Загрузка YOLO модели...")
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
        logger.info("🚀 Ollama запущен автоматически, ожидаю %d сек...", OLLAMA_AUTOSTART_WAIT_SEC)
        time.sleep(OLLAMA_AUTOSTART_WAIT_SEC)
    except FileNotFoundError:
        logger.warning("⚠️ Команда 'ollama' не найдена — установите Ollama: https://ollama.com")
    except Exception as exc:
        logger.warning("⚠️ Не удалось запустить Ollama: %s", exc)


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
            model_base  = OLLAMA_MODEL.split(":")[0]
            model_found = OLLAMA_MODEL in model_names or any(
                m == OLLAMA_MODEL or m.startswith(model_base + ":")
                for m in model_names
            )
            if not model_found:
                logger.info("⚠️ Модель %s не найдена, скачиваю...", OLLAMA_MODEL)
                ollama.pull(OLLAMA_MODEL)
            # Тестовый запрос
            ollama.generate(
                model=OLLAMA_MODEL,
                prompt="Respond with 'OK' if you are working.",
                options={"num_predict": 2},
            )
            return True
        except Exception as exc:
            logger.debug("Ollama probe failed: %s", exc)
            return False

    if _probe():
        return True

    # Первая попытка не удалась — пробуем автозапуск
    logger.warning("⚠️ Ollama недоступен. Пробую запустить автоматически...")
    _try_start_ollama()

    if _probe():
        logger.info("✅ Ollama успешно запущен и отвечает.")
        return True

    logger.error("❌ Ollama недоступен даже после автозапуска. Продолжаю без AI.")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Утилиты
# ─────────────────────────────────────────────────────────────────────────────

def ollama_generate_with_timeout(
    model: str,
    prompt: str,
    images: Optional[List[str]] = None,
    timeout: int = OLLAMA_TIMEOUT,
) -> Dict:
    """Вызывает ollama.generate с таймаутом через ThreadPoolExecutor."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(ollama.generate, model=model, prompt=prompt, images=images)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            raise TimeoutError(f"Ollama generate timed out after {timeout} seconds")


def extract_key_frames(video_path: Path, num_frames: int = AI_NUM_FRAMES) -> List[Path]:
    """Извлекает ключевые кадры из видео равномерно по времени."""
    cap = cv2.VideoCapture(str(video_path))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    if total_frames <= 0:
        cap.release()
        return []

    frame_indices = [int(total_frames * i / (num_frames + 1)) for i in range(1, num_frames + 1)]
    frames = []
    for idx in frame_indices:
        cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
        ret, frame = cap.read()
        if ret:
            frames.append(frame)
    cap.release()

    temp_dir = Path(tempfile.gettempdir()) / "youtubchik_frames"
    temp_dir.mkdir(exist_ok=True)
    tmp_paths = []
    for i, frame in enumerate(frames):
        tmp = temp_dir / f"{video_path.stem}_frame{i}_{random.randint(1000, 9999)}.jpg"
        cv2.imwrite(str(tmp), frame)
        tmp_paths.append(tmp)
    return tmp_paths


def load_trending_hashtags() -> List[str]:
    """Загружает список популярных хэштегов из файла."""
    if HASHTAGS_FILE.exists():
        with open(HASHTAGS_FILE, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    return []


# ─────────────────────────────────────────────────────────────────────────────
# YOLO — детекция объектов на ВСЕХ кадрах (исправлено: было только первый кадр)
# ─────────────────────────────────────────────────────────────────────────────

def run_yolo_all_frames(
    frame_paths: List[Path],
) -> Tuple[List[List[str]], str]:
    """
    Запускает YOLO на ВСЕХ извлечённых кадрах.
    Возвращает:
      - per_frame: список [objects_in_frame0, objects_in_frame1, ...]
      - obj_str:   объединённая строка уникальных объектов для промпта
    """
    yolo    = load_yolo()
    per_frame: List[List[str]] = []
    all_objects: set[str]      = set()

    for fp in frame_paths:
        frame = cv2.imread(str(fp))
        if frame is None:
            per_frame.append([])
            continue
        results = yolo(frame, verbose=False)
        if len(results[0].boxes) > 0:
            names = [yolo.names[int(box.cls)] for box in results[0].boxes]
        else:
            names = []
        per_frame.append(names)
        all_objects.update(names)

    obj_str = ", ".join(sorted(all_objects)) if all_objects else "none"
    logger.debug("YOLO по %d кадрам: %s", len(frame_paths), obj_str)
    return per_frame, obj_str


# ─────────────────────────────────────────────────────────────────────────────
# AI-определение точек нарезки (заменяет silencedetect + scenedetect)
# ─────────────────────────────────────────────────────────────────────────────

def generate_cut_points(
    video_path: Path,
    duration: float,
    yolo_per_frame: List[List[str]],
    num_frames: int = AI_NUM_FRAMES,
) -> List[float]:
    """
    Использует AI (Ollama + визуальный анализ кадров) для определения
    оптимальных точек нарезки видео.

    Логика:
      - Определяет примерное время каждого кадра по равномерному распределению
      - Передаёт кадры + YOLO-данные по каждому кадру в Ollama
      - Просит указать, где происходят смены сцен / спады активности
      - Возвращает отсортированный список временных меток в секундах

    Fallback: если AI не дал пригодных точек — возвращает равномерные срезы
    (каждые CLIP_MAX_LEN секунд).
    """
    frame_paths = []
    try:
        frame_paths = extract_key_frames(video_path, num_frames=num_frames)
        if not frame_paths:
            logger.warning("generate_cut_points: кадры не извлечены — fallback")
            return _uniform_cuts(duration)

        # Формируем контекст по кадрам: время + объекты
        frame_descs: List[str] = []
        for i, (fp, objects) in enumerate(zip(frame_paths, yolo_per_frame)):
            t = duration * (i + 1) / (num_frames + 1)
            obj_str = ", ".join(set(objects)) if objects else "none"
            frame_descs.append(f"  Frame {i+1} at {t:.1f}s — objects: [{obj_str}]")

        prompt = (
            f"You are analyzing a video of {duration:.1f} seconds total duration.\n"
            f"Here are key frames with YOLO-detected objects at each timestamp:\n"
            f"{chr(10).join(frame_descs)}\n\n"
            f"Task: Suggest the BEST timestamps (in seconds) to cut this video into short clips.\n"
            f"Requirements:\n"
            f"  - Each clip should be {CLIP_MIN_LEN:.0f}–{CLIP_MAX_LEN:.0f} seconds long\n"
            f"  - Cut at natural scene transitions, pauses in action, or topic changes\n"
            f"  - Use visual context (object changes between frames) to identify transitions\n"
            f"  - Do NOT cut within the first {CLIP_MIN_LEN:.0f}s or last {CLIP_MIN_LEN:.0f}s\n\n"
            f"Return ONLY a comma-separated list of timestamps, example: 18.5, 37.0, 55.2\n"
            f"No explanation, no extra text."
        )

        try:
            response = ollama_generate_with_timeout(
                model=OLLAMA_MODEL,
                prompt=prompt,
                images=[str(p) for p in frame_paths],
                timeout=OLLAMA_TIMEOUT,
            )
            text = response["response"].strip()
            cuts = _parse_timestamps(text, duration)

            if cuts:
                logger.info(
                    "AI нарезка: %d точек для видео %.1f с: %s",
                    len(cuts), duration,
                    ", ".join(f"{c:.1f}" for c in cuts),
                )
                return cuts
            else:
                logger.warning("AI не вернул пригодных точек — используем равномерные срезы")
                return _uniform_cuts(duration)

        except Exception as exc:
            logger.warning("generate_cut_points AI error: %s — fallback", exc)
            return _uniform_cuts(duration)

    finally:
        for p in frame_paths:
            p.unlink(missing_ok=True)


def _parse_timestamps(text: str, duration: float) -> List[float]:
    """Парсит строку с временными метками, фильтрует невалидные."""
    cuts: List[float] = []
    # ищем все числа (включая дробные) в тексте
    for token in re.findall(r"\d+(?:\.\d+)?", text):
        try:
            t = float(token)
            if CLIP_MIN_LEN <= t <= duration - CLIP_MIN_LEN:
                cuts.append(t)
        except ValueError:
            pass
    return sorted(set(cuts))


def _uniform_cuts(duration: float) -> List[float]:
    """
    Fallback: равномерная нарезка через каждые CLIP_MAX_LEN секунд.
    """
    cuts: List[float] = []
    t = CLIP_MAX_LEN
    while t < duration - CLIP_MIN_LEN:
        cuts.append(t)
        t += CLIP_MAX_LEN
    return cuts


# ─────────────────────────────────────────────────────────────────────────────
# Генерация метаданных (основная функция)
# ─────────────────────────────────────────────────────────────────────────────

def generate_video_metadata(video_path: Path, trending_hashtags: List[str]) -> List[Dict]:
    """
    Генерирует несколько вариантов метаданных для видео.

    Изменения:
      - YOLO теперь запускается на ВСЕХ кадрах (не только на первом)
      - YOLO-данные по кадрам сохраняются в каждом варианте метаданных
        (поле 'yolo_per_frame') для последующего использования в slicer

    Returns:
        Список словарей с вариантами метаданных.
    """
    frame_paths: List[Path] = []
    try:
        frame_paths = extract_key_frames(video_path)
        if not frame_paths:
            return _fallback_meta(video_path, count=AI_NUM_VARIANTS)

        # YOLO на ВСЕХ кадрах
        yolo_per_frame, obj_str = run_yolo_all_frames(frame_paths)

        hashtag_context = ""
        if trending_hashtags:
            sample_tags = random.sample(trending_hashtags, min(10, len(trending_hashtags)))
            hashtag_context = f"Consider these trending hashtags: {', '.join(sample_tags)}.\n"

        prompt = f"""You are Qwen2.5-VL expert and a professional YouTube Shorts content creator \
with deep visual understanding capabilities. Carefully analyze ALL provided keyframes from the video, \
paying close attention to visual details, actions, emotions, colors, and context.
Detected objects per frame (YOLO, all frames analyzed): {obj_str}.

{hashtag_context}
Based on your visual analysis, generate {AI_NUM_VARIANTS} distinct and creative sets of metadata \
for this YouTube Shorts video. Make each set feel UNIQUE in tone (e.g. hype, curious, emotional, funny).

Each set must include ALL of the following fields:

STANDARD FIELDS:
- Title: catchy scroll-stopping title (max 60 chars). Each variant must differ in tone and wording.
- Description: 2-3 sentences with relevant emojis, a strong call to action (e.g. \"Drop a 🔥 below!\"),\
  and hashtags. Be specific to what's happening in the video — avoid generic phrases.
- Hashtags: 5-10 hashtags (always include #shorts + trending ones). Group by theme:\
  content type, emotion, topic. No repetition across variants.
- Thumbnail: describe a visually compelling thumbnail with bold text overlay suggestion.

VIRALITY FIELDS (critical for engagement):
- HookText: short attention-grabbing phrase shown at the START of the clip (max 20 chars, no punctuation overload).
  Examples: \"Wait for it 👀\", \"No way this happened\", \"Watch till end 🔥\"
- BestSegment: time in seconds (float) of the funniest/most energetic moment in the video.
  Estimate based on keyframe positions (frames are evenly spaced). Return as a number like 4.5
- Overlays: 2-4 timed text comments that appear during the video to boost engagement.
  Format each as: TIME|TEXT  or  TIME|TEXT|DURATION
  where TIME is seconds from start (float), TEXT is the comment (max 30 chars),
  DURATION is optional display seconds (default {OVERLAY_DEFAULT_DURATION}s).
  Examples: \"1.5|Bro really did that 😂\", \"5.0|Wait for the reaction|3\", \"8.2|TOP 10 moment 🔥\"
- LoopPrompt: short phrase shown at the END to encourage rewatching (max 30 chars).
  Examples: \"👀 Watch again!\", \"Did you see that? 🔄\", \"Rewatch from the start 😂\"

Format EACH SET EXACTLY as follows (no extra blank lines between fields within a set):

SET 1:
Title: <title>
Description: <description>
Hashtags: <comma-separated list>
Thumbnail: <thumbnail idea>
HookText: <hook phrase>
BestSegment: <seconds as float>
Overlays:
<time>|<text>
<time>|<text>|<duration>
LoopPrompt: <loop phrase>

SET 2:
Title: <title>
Description: <description>
Hashtags: <comma-separated list>
Thumbnail: <thumbnail idea>
HookText: <hook phrase>
BestSegment: <seconds as float>
Overlays:
<time>|<text>
<time>|<text>|<duration>
LoopPrompt: <loop phrase>

SET 3:
Title: <title>
Description: <description>
Hashtags: <comma-separated list>
Thumbnail: <thumbnail idea>
HookText: <hook phrase>
BestSegment: <seconds as float>
Overlays:
<time>|<text>
<time>|<text>|<duration>
LoopPrompt: <loop phrase>
"""
        try:
            response = ollama_generate_with_timeout(
                model=OLLAMA_MODEL,
                prompt=prompt,
                images=[str(p) for p in frame_paths],
                timeout=OLLAMA_TIMEOUT,
            )
            text = response["response"].strip()
            variants = parse_ollama_response(text)
        except Exception as e:
            logger.warning("⚠️ Ошибка генерации метаданных: %s", e)
            variants = _fallback_meta(video_path, count=AI_NUM_VARIANTS)

        # Сохраняем YOLO-данные в каждый вариант для последующего использования в slicer
        for v in variants:
            v["yolo_per_frame"] = yolo_per_frame

        return variants

    finally:
        for p in frame_paths:
            p.unlink(missing_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Парсинг ответа Ollama
# ─────────────────────────────────────────────────────────────────────────────

def _parse_overlays(lines: List[str]) -> List[Dict]:
    result = []
    for line in lines:
        line = line.strip()
        if not line or "|" not in line:
            continue
        parts = line.split("|")
        try:
            time_val = float(parts[0].strip())
            text_val = parts[1].strip() if len(parts) > 1 else ""
            dur_val  = float(parts[2].strip()) if len(parts) > 2 else float(OVERLAY_DEFAULT_DURATION)
            if text_val:
                result.append({"time": time_val, "text": text_val, "duration": dur_val})
        except (ValueError, IndexError) as e:
            logger.debug("Не удалось распарсить строку оверлея '%s': %s", line, e)
    return result


def parse_ollama_response(text: str) -> List[Dict]:
    variants: List[Dict] = []
    current_set: Dict    = {}
    in_overlays           = False
    overlay_lines: List[str] = []

    def _flush_overlays():
        if overlay_lines:
            current_set["overlays"] = _parse_overlays(overlay_lines)
        overlay_lines.clear()

    for line in text.splitlines():
        stripped = line.strip()

        if stripped.startswith("SET ") and ":" in stripped:
            if in_overlays:
                _flush_overlays()
                in_overlays = False
            if current_set:
                variants.append(current_set)
            current_set   = {}
            overlay_lines = []
            continue

        if stripped.startswith("Overlays:"):
            in_overlays   = True
            overlay_lines = []
            rest = stripped[9:].strip()
            if rest:
                overlay_lines.append(rest)
            continue

        if in_overlays:
            if "|" in stripped:
                overlay_lines.append(stripped)
                continue
            else:
                _flush_overlays()
                in_overlays = False

        if stripped.startswith("Title:"):
            current_set["title"] = stripped[6:].strip()
        elif stripped.startswith("Description:"):
            current_set["description"] = stripped[12:].strip()
        elif stripped.startswith("Hashtags:"):
            tags_str = stripped[9:].strip()
            current_set["tags"] = [t.strip().lstrip("#") for t in tags_str.split(",") if t.strip()]
        elif stripped.startswith("Thumbnail:"):
            current_set["thumbnail_idea"] = stripped[10:].strip()
        elif stripped.startswith("HookText:"):
            current_set["hook_text"] = stripped[9:].strip()
        elif stripped.startswith("BestSegment:"):
            raw = stripped[12:].strip()
            try:
                current_set["best_segment"] = float(raw)
            except ValueError:
                current_set["best_segment"] = None
        elif stripped.startswith("LoopPrompt:"):
            current_set["loop_prompt"] = stripped[11:].strip()

    if in_overlays:
        _flush_overlays()
    if current_set:
        variants.append(current_set)

    if not variants:
        logger.warning("parse_ollama_response: не найдено ни одного SET, возвращаю fallback.")
        return _fallback_meta(None, count=AI_NUM_VARIANTS)

    for v in variants:
        v.setdefault("title",          "Amazing Shorts Video")
        v.setdefault("description",    "Check out this awesome video! 🔥\nSubscribe for more! 🚀")
        v.setdefault("tags",           ["shorts", "viral", "trending"])
        v.setdefault("thumbnail_idea", "A key moment from the video with bold text overlay.")
        v.setdefault("hook_text",    "")
        v.setdefault("best_segment", None)
        v.setdefault("overlays",     [])
        v.setdefault("loop_prompt",  "")
        v.setdefault("yolo_per_frame", [])

    return variants


def _fallback_meta(video_path: Optional[Path] = None, count: int = 1) -> List[Dict]:
    base_title = f"Amazing Clip {video_path.stem}" if video_path else "Amazing Shorts Video"
    return [{
        "title":          base_title,
        "description":    f"{base_title}\n\nSubscribe! 🔔\n#shorts #viral #trending",
        "tags":           ["shorts", "viral", "trending"],
        "thumbnail_idea": "Use a dramatic moment from the video.",
        "hook_text":      "",
        "best_segment":   None,
        "overlays":       [],
        "loop_prompt":    "",
        "yolo_per_frame": [],
    } for _ in range(count)]


def generate_clone_title(original_title: str) -> str:
    """Генерирует немного изменённый заголовок для клона с помощью Ollama."""
    prompt = (
        f"Rewrite this YouTube Shorts title to make it slightly different but equally catchy: "
        f"'{original_title}'. Just output the new title."
    )
    try:
        response = ollama_generate_with_timeout(OLLAMA_MODEL, prompt, timeout=30)
        new_title = response["response"].strip().strip('"')
        if new_title and len(new_title) < 100:
            return new_title
    except Exception:
        pass
    suffixes = ["🔥", "🚀", "💯", "Best", "Awesome", "Must watch"]
    return f"{original_title} {random.choice(suffixes)}"


# ─────────────────────────────────────────────────────────────────────────────
# Импорт re — нужен для _parse_timestamps (добавлен в конец во избежание кольца)
# ─────────────────────────────────────────────────────────────────────────────
import re  # noqa: E402 (необходимо после определения функции)
