# ai.py
import concurrent.futures
import hashlib
import io
import json
import logging
from pipeline import config
import re
import subprocess
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import cv2
import ollama
import requests
from PIL import Image

from pipeline.config import (
    OLLAMA_MODEL, AI_NUM_FRAMES,
    AI_NUM_VARIANTS, OLLAMA_TIMEOUT, HASHTAGS_FILE,
    OVERLAY_DEFAULT_DURATION,
    OLLAMA_AUTOSTART, OLLAMA_AUTOSTART_WAIT_SEC,
    CLIP_MIN_LEN, CLIP_MAX_LEN,
    VL_CACHE_FILE,
)
from pipeline.slicer_cut_utils import normalize_best_segment
from pipeline.utils import probe_video, save_json
from pipeline.content_locale import (
    FALLBACK_CONTENT_LOCALE,
    content_language_name_for_prompt,
    locale_language_code,
    normalize_content_locale,
    platform_meta_hint_line,
    resolve_content_locale_for_account,
)

logger = logging.getLogger(__name__)


def _locale_meta_strings(content_locale: str) -> Dict[str, str]:
    """Строки-заглушки для детерминированных путей (ru vs остальное → en)."""
    base = (content_locale or FALLBACK_CONTENT_LOCALE).split("-")[0].lower()
    if base == "ru":
        return {
            "plot_title": "Сюжет по ключевым кадрам",
            "desc_generic": (
                "В видео показано действие персонажа и развитие сцены "
                "в нескольких последовательных кадрах."
            ),
            "in_frame_a": "В кадре",
            "in_frame_b": "В кадре",
            "then": "затем",
            "after": "после этого",
            "hook_fallback": "Смотри, что происходит",
            "loop_fallback": "Чем это закончится?",
            "thumb_prefix": "Крупный план ключевого момента:",
            "fallback_title": "Ключевой момент из видео",
            "fallback_desc": "Короткий динамичный эпизод с фокусом на главном действии в кадре.",
            "fallback_thumb": "Крупный план ключевого действия в кадре.",
        }
    return {
        "plot_title": "Story from key frames",
        "desc_generic": (
            "The video shows action and how the scene unfolds across several frames."
        ),
        "in_frame_a": "In the frame",
        "in_frame_b": "in the frame",
        "then": "then",
        "after": "after that",
        "hook_fallback": "See what happens",
        "loop_fallback": "How will it end?",
        "thumb_prefix": "Close-up of the key moment:",
        "fallback_title": "Key moment from the video",
        "fallback_desc": "A short, dynamic clip focused on the main action in frame.",
        "fallback_thumb": "Close-up of the main action in frame.",
    }


def _build_meta_language_block(lang_name: str, content_locale: str, platform_line: str) -> str:
    return (
        f"TARGET LANGUAGE: Write ALL user-facing text fields (title, description, tags, hook_text, "
        f"thumbnail_idea, loop_prompt, and overlay text) in {lang_name} (BCP-47 locale {content_locale}). "
        "Do not mix languages unless a proper noun from the video requires it.\n"
        f"{platform_line}\n\n"
    )

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
    except (FileNotFoundError, json.JSONDecodeError, OSError) as e:
        logger.debug("VL cache read error: %s", e)
        return None


def _vl_cache_set(key: str, value: Dict) -> None:
    """Атомарно записывает запись в VL-кеш (tempfile + os.replace)."""
    import os
    import tempfile
    try:
        with _VL_CACHE_LOCK:
            VL_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            cache: Dict = {}
            if VL_CACHE_FILE.exists():
                try:
                    cache = json.loads(VL_CACHE_FILE.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    pass
            cache[key] = value
            # Атомарная запись: пишем во временный файл, затем os.replace
            fd, tmp_path = tempfile.mkstemp(
                dir=str(VL_CACHE_FILE.parent), suffix=".tmp"
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as tmp_f:
                    json.dump(cache, tmp_f, ensure_ascii=False)
                os.replace(tmp_path, str(VL_CACHE_FILE))
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
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

def _scene_aware_frame_indices(cap: cv2.VideoCapture, total_frames: int, num_frames: int) -> List[int]:
    """Подбирает индексы кадров: равномерные якоря + пики изменений между сценами."""
    if total_frames <= 0 or num_frames <= 0:
        return []

    # Равномерные якоря по всей длине ролика (гарантируют охват таймлайна).
    anchors: List[int] = []
    for i in range(num_frames):
        idx = min(int(round(i * (total_frames - 1) / max(1, num_frames - 1))), total_frames - 1)
        anchors.append(idx)

    # Детект резких изменений по sampled-кадрам.
    sample_step = max(1, total_frames // max(24, num_frames * 10))
    diffs: List[Tuple[float, int]] = []
    prev_gray = None
    pos = 0
    while pos < total_frames:
        cap.set(cv2.CAP_PROP_POS_FRAMES, pos)
        ok, frame = cap.read()
        if not ok:
            break
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        if prev_gray is not None:
            score = float(cv2.mean(cv2.absdiff(gray, prev_gray))[0])
            diffs.append((score, pos))
        prev_gray = gray
        pos += sample_step

    scene_slots = max(0, num_frames - len(anchors) // 2)
    scene_points = [p for _, p in sorted(diffs, key=lambda x: x[0], reverse=True)[:scene_slots]]

    merged = sorted(set(anchors + scene_points))
    if len(merged) > num_frames:
        # Нормализуем обратно до нужного количества, сохраняя порядок по таймлайну.
        step = len(merged) / num_frames
        merged = [merged[min(int(i * step), len(merged) - 1)] for i in range(num_frames)]
        merged = sorted(set(merged))
    while len(merged) < num_frames:
        merged.append(min(total_frames - 1, merged[-1] + 1 if merged else 0))
    return merged[:num_frames]


def extract_frames(video_path: Path, num_frames: int = AI_NUM_FRAMES) -> List[bytes]:
    """Извлекает кадры для VL: scene-aware выборка + охват всего таймлайна."""
    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        raise ValueError(f"Не удалось открыть видео: {video_path}")

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    if total_frames <= 0:
        cap.release()
        raise ValueError(f"Видео не содержит кадров: {video_path}")

    frame_indices = _scene_aware_frame_indices(cap, total_frames, num_frames)
    frames: List[bytes] = []
    for frame_num in frame_indices:
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
    response_format: Optional[str] = None,
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
    if response_format:
        kwargs["format"] = response_format
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(ollama.generate, **kwargs)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            raise TimeoutError(f"Ollama timeout after {timeout}s")


_HF_QWEN_MODEL = None
_HF_QWEN_PROCESSOR = None
_HF_QWEN_VIDEO_HELPER = None
_HF_QWEN_LOCK = threading.Lock()


def _use_transformers_vl() -> bool:
    return getattr(config, "VL_BACKEND", "ollama") == "transformers"


def _load_hf_qwen_components():
    """Ленивая загрузка Qwen2.5-VL через transformers."""
    global _HF_QWEN_MODEL, _HF_QWEN_PROCESSOR, _HF_QWEN_VIDEO_HELPER
    if _HF_QWEN_MODEL is not None and _HF_QWEN_PROCESSOR is not None and _HF_QWEN_VIDEO_HELPER is not None:
        return _HF_QWEN_MODEL, _HF_QWEN_PROCESSOR, _HF_QWEN_VIDEO_HELPER
    with _HF_QWEN_LOCK:
        if _HF_QWEN_MODEL is not None and _HF_QWEN_PROCESSOR is not None and _HF_QWEN_VIDEO_HELPER is not None:
            return _HF_QWEN_MODEL, _HF_QWEN_PROCESSOR, _HF_QWEN_VIDEO_HELPER
        from transformers import Qwen2_5_VLForConditionalGeneration, AutoProcessor
        from qwen_vl_utils import process_vision_info
        model_id = getattr(config, "QWEN_VL_MODEL_ID", "Qwen/Qwen2.5-VL-7B-Instruct")
        common_kwargs: Dict = {
            "torch_dtype": "auto",
            "device_map": "auto",
        }
        if getattr(config, "QWEN_VL_FLASH_ATTN", True):
            common_kwargs["attn_implementation"] = "flash_attention_2"
        try:
            model = Qwen2_5_VLForConditionalGeneration.from_pretrained(model_id, **common_kwargs)
        except Exception:
            # Fallback без flash attention для сред, где он недоступен.
            common_kwargs.pop("attn_implementation", None)
            model = Qwen2_5_VLForConditionalGeneration.from_pretrained(model_id, **common_kwargs)
        processor = AutoProcessor.from_pretrained(model_id)
        _HF_QWEN_MODEL = model
        _HF_QWEN_PROCESSOR = processor
        _HF_QWEN_VIDEO_HELPER = process_vision_info
        return _HF_QWEN_MODEL, _HF_QWEN_PROCESSOR, _HF_QWEN_VIDEO_HELPER


def _hf_qwen_video_generate(
    video_path: Path,
    prompt: str,
    max_new_tokens: Optional[int] = None,
    temperature: Optional[float] = None,
) -> str:
    """Генерация через Qwen2.5-VL (transformers) с входом типа video."""
    model, processor, process_vision_info = _load_hf_qwen_components()
    fps = float(getattr(config, "QWEN_VL_VIDEO_FPS", 1.0))
    max_pixels = int(getattr(config, "QWEN_VL_MAX_PIXELS", 360 * 420))
    max_new = int(max_new_tokens or getattr(config, "QWEN_VL_MAX_NEW_TOKENS", 512))
    temp = float(temperature if temperature is not None else getattr(config, "QWEN_VL_TEMPERATURE", 0.4))
    messages = [{
        "role": "user",
        "content": [
            {
                "type": "video",
                "video": str(video_path.resolve()),
                "fps": fps,
                "max_pixels": max_pixels,
            },
            {"type": "text", "text": prompt},
        ],
    }]
    text = processor.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    image_inputs, video_inputs, video_kwargs = process_vision_info(messages, return_video_kwargs=True)
    inputs = processor(
        text=[text],
        images=image_inputs,
        videos=video_inputs,
        padding=True,
        return_tensors="pt",
        **video_kwargs,
    )
    device = getattr(model, "device", None)
    if device is not None:
        inputs = inputs.to(device)
    do_sample = temp > 0
    gen_kwargs: Dict = {
        "max_new_tokens": max_new,
        "do_sample": do_sample,
    }
    if do_sample:
        gen_kwargs["temperature"] = temp
    generated_ids = model.generate(**inputs, **gen_kwargs)
    out = processor.batch_decode(
        [ids[len(inputs.input_ids[0]):] for ids in generated_ids],
        skip_special_tokens=True,
        clean_up_tokenization_spaces=False,
    )[0]
    return out


def _hf_qwen_images_generate(
    frames: List[bytes],
    prompt: str,
    max_new_tokens: Optional[int] = None,
    temperature: Optional[float] = None,
) -> str:
    """Генерация через Qwen2.5-VL по набору изображений в хронологическом порядке."""
    if not frames:
        return ""
    model, processor, _ = _load_hf_qwen_components()
    max_new = int(max_new_tokens or getattr(config, "QWEN_VL_MAX_NEW_TOKENS", 512))
    temp = float(temperature if temperature is not None else getattr(config, "QWEN_VL_TEMPERATURE", 0.4))
    pil_images: List[Image.Image] = []
    for b in frames:
        try:
            pil_images.append(Image.open(io.BytesIO(b)).convert("RGB"))
        except Exception:
            continue
    if not pil_images:
        return ""
    messages = [{
        "role": "user",
        "content": ([{"type": "image"} for _ in pil_images] + [{"type": "text", "text": prompt}]),
    }]
    text = processor.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    inputs = processor(
        text=[text],
        images=pil_images,
        padding=True,
        return_tensors="pt",
    )
    device = getattr(model, "device", None)
    if device is not None:
        inputs = inputs.to(device)
    do_sample = temp > 0
    gen_kwargs: Dict = {
        "max_new_tokens": max_new,
        "do_sample": do_sample,
    }
    if do_sample:
        gen_kwargs["temperature"] = temp
    generated_ids = model.generate(**inputs, **gen_kwargs)
    out = processor.batch_decode(
        [ids[len(inputs.input_ids[0]):] for ids in generated_ids],
        skip_special_tokens=True,
        clean_up_tokenization_spaces=False,
    )[0]
    return out


def _video_vl_generate_with_fallback(
    video_path: Path,
    prompt: str,
    frames: Optional[List[bytes]] = None,
    response_format: Optional[str] = None,
) -> str:
    """Пробует transformers-backend, при ошибке откатывается на Ollama."""
    if _use_transformers_vl():
        try:
            if frames:
                return _hf_qwen_images_generate(frames, prompt)
            return _hf_qwen_video_generate(video_path, prompt)
        except Exception as exc:
            logger.warning("Transformers VL failed, fallback to Ollama: %s", exc)
    response = ollama_generate_with_timeout(
        OLLAMA_MODEL,
        prompt,
        images=frames,
        response_format=response_format,
    )
    return response.get("response", "")


def _dump_meta_debug_response(video_path: Path, attempt: int, reason: str, raw_text: str) -> None:
    """Сохраняет сырой ответ Ollama для диагностики невалидного/пустого JSON."""
    try:
        dbg_dir = config.BASE_DIR / "data" / "ollama_meta_debug"
        dbg_dir.mkdir(parents=True, exist_ok=True)
        stamp = int(time.time())
        out = dbg_dir / f"{video_path.stem}_{stamp}_attempt{attempt}.txt"
        payload = (
            f"video={video_path.name}\n"
            f"attempt={attempt}\n"
            f"reason={reason}\n"
            f"model={OLLAMA_MODEL}\n"
            "---- RAW RESPONSE ----\n"
            f"{raw_text or ''}\n"
        )
        out.write_text(payload, encoding="utf-8")
    except Exception as exc:
        logger.debug("meta debug dump failed: %s", exc)


_GENERIC_META_PATTERNS = [
    r"subscribe\s+for\s+more",
    r"#shorts\b",
    r"#viral\b",
    r"#trending\b",
    r"смотри\s+как",
    r"это\s+видео",
    r"<\|im_start\|>",
    r"<\|im_end\|>",
    r"\bassistant\b",
    r"\buser\b",
    r"^название$",
    r"^описание$",
    r"^тег\d*$",
    r"^tag\d*$",
    r"^оверлей\d*$",
    r"^overlay\d*$",
    r"идея\s+для\s+миниатюры",
    r"текст\s+привлечения\s+внимания",
    r"повторяющийся\s+текст",
    r"ключевой\s+момент\s+из\s+видео",
    r"яркий\s+момент",
    r"интересный\s+момент",
    r"сюжет\s+собран\s+по\s+ключевым\s+кадрам",
]

_NICHE_STYLE_PROFILES = {
    "animals": (
        "Стиль animals/pets: эмоциональный, тёплый, с акцентом на поведение животного и неожиданную реакцию."
    ),
    "gaming": (
        "Стиль gaming: динамика момента, конкретный игровой эпизод, короткие формулировки без жаргона ради жаргона."
    ),
    "tech": (
        "Стиль tech: конкретная польза/факт, без громких обещаний, понятные формулировки для широкой аудитории."
    ),
    "crypto": (
        "Стиль crypto/finance: осторожные формулировки, без обещаний дохода, акцент на событие и риск."
    ),
    "motivation": (
        "Стиль motivation: энергичная, поддерживающая подача, но без абстрактной воды."
    ),
    "food": (
        "Стиль food: упор на вкус/процесс/результат, конкретные детали кадра."
    ),
    "travel": (
        "Стиль travel: место + конкретная эмоция/ситуация, минимум клише."
    ),
}


def _clean_meta_text(value: str, max_len: int) -> str:
    """Лёгкая очистка title/description после LLM."""
    if not isinstance(value, str):
        return ""
    text = re.sub(r"\s+", " ", value).strip()
    text = re.sub(r"<\|im_(?:start|end)\|>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b(?:assistant|user|system)\b\s*:?", " ", text, flags=re.IGNORECASE)
    text = text.strip("\"'`")
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > max_len:
        text = text[:max_len].rstrip()
    return text


def _is_generic_meta_text(text: str) -> bool:
    if not text:
        return True
    low = text.lower()
    return any(re.search(p, low) for p in _GENERIC_META_PATTERNS)


def _normalize_meta_variant(v: Dict) -> Dict:
    """Нормализует и подчищает один вариант метаданных."""
    title = _clean_meta_text(v.get("title", ""), 60)
    description = _clean_meta_text(v.get("description", ""), 150)
    hook_text = _clean_meta_text(v.get("hook_text", ""), 80)
    loop_prompt = _clean_meta_text(v.get("loop_prompt", ""), 80)
    thumbnail_idea = _clean_meta_text(v.get("thumbnail_idea", ""), 120)

    tags_raw = v.get("tags", [])
    if isinstance(tags_raw, str):
        tags = [t.strip() for t in re.split(r"[,;]", tags_raw) if t.strip()]
    elif isinstance(tags_raw, list):
        tags = [str(t).strip() for t in tags_raw if str(t).strip()]
    else:
        tags = []

    overlays_raw = v.get("overlays", [])
    overlays = overlays_raw if isinstance(overlays_raw, list) else []

    return {
        "title": title,
        "description": description,
        "tags": tags[:10],
        "thumbnail_idea": thumbnail_idea,
        "hook_text": hook_text,
        "best_segment": v.get("best_segment"),
        "overlays": overlays,
        "loop_prompt": loop_prompt,
    }


def _meta_quality_ok(v: Dict) -> bool:
    """Быстрая эвристика качества title/description."""
    title = v.get("title", "").strip()
    desc = v.get("description", "").strip()
    if len(title) < 8 or len(desc) < 20:
        return False
    if "<|" in title or "<|" in desc:
        return False
    if _is_generic_meta_text(title) or _is_generic_meta_text(desc):
        return False
    return True


def _derive_tags(
    title: str,
    description: str,
    transcript: str,
    trending_hashtags: Optional[List[str]],
) -> List[str]:
    tags: List[str] = []
    if trending_hashtags:
        for h in trending_hashtags[:5]:
            t = re.sub(r"^#+", "", str(h).strip()).lower()
            t = re.sub(r"\s+", "", t)
            if t and t not in tags:
                tags.append(t)
    text_words = re.findall(r"[a-zA-Zа-яА-ЯёЁ]{4,}", f"{title} {description} {transcript}")
    for w in text_words[:8]:
        lw = w.lower()
        if lw not in tags:
            tags.append(lw)
    return tags[:10]


def _normalize_tags(tags_raw: object) -> List[str]:
    tags: List[str] = []
    if isinstance(tags_raw, list):
        for t in tags_raw:
            txt = _clean_meta_text(str(t), 30).lower()
            txt = re.sub(r"^#+", "", txt).strip()
            if not txt or _is_generic_meta_text(txt):
                continue
            if txt not in tags:
                tags.append(txt)
    return tags[:10]


def _derive_hook_text(
    title: str,
    description: str,
    transcript: str,
    ms: Optional[Dict[str, str]] = None,
) -> str:
    # 3-7 слов: берём сильный фрагмент title/description/transcript.
    if ms is None:
        ms = _locale_meta_strings(FALLBACK_CONTENT_LOCALE)
    base = title or description
    if not base:
        base = transcript
    words = re.findall(r"[^\s]+", base)
    if len(words) >= 3:
        return " ".join(words[:7]).strip(" .,!?:;")
    return ms["hook_fallback"]


def _derive_loop_prompt(
    description: str,
    hook_text: str,
    ms: Optional[Dict[str, str]] = None,
) -> str:
    if ms is None:
        ms = _locale_meta_strings(FALLBACK_CONTENT_LOCALE)
    src = description or hook_text
    if "?" in src:
        q = src.split("?")[0].strip()
        if q:
            return (q + "?")[:80]
    return ms["loop_fallback"][:80]


def _summarize_video_context_vl(
    video_path: Path,
    frames: List[bytes],
    transcript: str,
    lang_name: str = "English",
) -> str:
    """Короткая сводка сюжета по КАДРАМ (и транскрипту), без использования имени файла."""
    if not frames:
        return ""
    transcript_hint = f"\nTranscript (if relevant): {transcript[:400]}\n" if transcript else ""
    prompt = (
        "Below are frames from ONE video in chronological order.\n"
        "Summarize the story in 3 short points: (1) who/what is in frame, "
        "(2) main action, (3) what makes the moment engaging.\n"
        "Be factual and brief. Do not mention the filename."
        f"{transcript_hint}"
        f"\nWrite the entire summary in {lang_name}.\n"
    )
    try:
        raw = _video_vl_generate_with_fallback(
            video_path=video_path,
            prompt=prompt,
            frames=frames,
            response_format=None,
        )
        return _clean_meta_text(raw, 500)
    except Exception as exc:
        logger.debug("VL context summary failed: %s", exc)
        return ""


def _caption_keyframes_vl(
    frames: List[bytes],
    transcript: str = "",
    video_path: Optional[Path] = None,
    lang_name: str = "English",
) -> List[str]:
    """Обязательный caption-pass: краткое описание каждого ключевого кадра."""
    captions: List[str] = []
    transcript_hint = f"\nTranscript (if useful): {transcript[:300]}" if transcript else ""
    lang_line = f"\nWrite every caption in {lang_name}.\n"
    if _use_transformers_vl() and video_path is not None:
        try:
            prompt = (
                "Create caption-per-frame lines for key moments of the video.\n"
                f"Need {max(3, min(len(frames), 12))} short lines in order (one action per line).\n"
                "Format: one line each, no numbering, no fluff."
                f"{transcript_hint}{lang_line}"
            )
            raw = _hf_qwen_images_generate(frames, prompt, max_new_tokens=350, temperature=0.2)
            for line in re.split(r"[\r\n]+", raw):
                txt = _clean_meta_text(re.sub(r"^\s*[-*•\d\.\)]\s*", "", line), 120)
                if txt and not _is_generic_meta_text(txt):
                    captions.append(txt)
            if captions:
                return captions[:12]
        except Exception as exc:
            logger.debug("video caption-per-frame via transformers failed: %s", exc)

    for idx, frame in enumerate(frames, start=1):
        prompt = (
            f"Frame #{idx}. Describe only what is visible: who/what and action/state. "
            "One short phrase, no fluff."
            f"{transcript_hint}{lang_line}"
        )
        try:
            r = ollama_generate_with_timeout(
                OLLAMA_MODEL,
                prompt,
                images=[frame],
                timeout=max(15, min(OLLAMA_TIMEOUT, 60)),
            )
            txt = _clean_meta_text(r.get("response", ""), 120)
            if txt and not _is_generic_meta_text(txt):
                captions.append(txt)
        except Exception as exc:
            logger.debug("keyframe caption failed idx=%s: %s", idx, exc)
    return captions


def _build_deterministic_meta_seed(
    frame_captions: List[str],
    transcript: str,
    context_summary: str,
    ms: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """Детерминированная основа title/description из caption-per-frame."""
    if ms is None:
        ms = _locale_meta_strings(FALLBACK_CONTENT_LOCALE)
    joined = " ".join(frame_captions[:8]).strip()
    tr = _clean_meta_text(transcript or "", 220)
    ctx = _clean_meta_text(context_summary or "", 220)
    src = joined or tr or ctx
    words = re.findall(r"[а-яА-ЯёЁa-zA-Z0-9]{3,}", src)
    uniq: List[str] = []
    for w in words:
        lw = w.lower()
        if lw not in uniq:
            uniq.append(lw)
        if len(uniq) >= 18:
            break
    title = _clean_meta_text(" ".join(uniq[:8]), 60)
    desc_seed = _clean_meta_text(" ".join(uniq[:24]), 150)
    description = desc_seed if len(desc_seed) >= 35 else _clean_meta_text(src, 150)
    # Детерминированное «человеческое» описание из реальных caption'ов.
    if frame_captions:
        c1 = _clean_meta_text(frame_captions[0], 85)
        c2 = _clean_meta_text(frame_captions[1], 85) if len(frame_captions) > 1 else ""
        c3 = _clean_meta_text(frame_captions[2], 85) if len(frame_captions) > 2 else ""
        parts = [x for x in [c1, c2, c3] if x]
        if len(parts) >= 2:
            description = _clean_meta_text(
                f"{ms['in_frame_a']} {parts[0].lower()}, {ms['then']} {parts[1].lower()}"
                + (f", {ms['after']} {parts[2].lower()}." if c3 else "."),
                150,
            )
        elif parts:
            description = _clean_meta_text(f"{ms['in_frame_a']} {parts[0].lower()}.", 150)
    if len(title) < 12:
        title = ms["plot_title"]
    if len(description) < 35:
        if frame_captions:
            joined2 = _clean_meta_text("; ".join(frame_captions[:3]), 150)
            if joined2:
                description = joined2
            else:
                description = ms["desc_generic"]
        else:
            description = ms["desc_generic"]
    return {"title": title, "description": description}


def _variant_too_generic(v: Dict) -> bool:
    """Жёсткий фильтр общих ответов для автоперегенерации."""
    title = _clean_meta_text(v.get("title", ""), 80).lower()
    desc = _clean_meta_text(v.get("description", ""), 180).lower()
    if _is_generic_meta_text(title) or _is_generic_meta_text(desc):
        return True
    bad_phrases = [
        "ключевой момент",
        "яркий момент",
        "интересный момент",
        "в этом видео",
        "в ролике показан",
        "key moment",
        "interesting moment",
        "in this video",
        "this video shows",
    ]
    if any(p in title for p in bad_phrases):
        return True
    if any(p in desc for p in bad_phrases) and len(re.findall(r"[а-яa-z0-9]{4,}", desc)) < 10:
        return True
    return False


def _enrich_metadata_variant(
    v: Dict,
    video_path: Path,
    transcript: str,
    trending_hashtags: Optional[List[str]],
    context_summary: str = "",
    deterministic_seed: Optional[Dict[str, str]] = None,
    ms: Optional[Dict[str, str]] = None,
) -> Dict:
    """Дозаполняет пустые поля метаданных осмысленными значениями."""
    if ms is None:
        ms = _locale_meta_strings(FALLBACK_CONTENT_LOCALE)
    title = _clean_meta_text(v.get("title", ""), 60)
    desc = _clean_meta_text(v.get("description", ""), 150)
    seed_title = _clean_meta_text((deterministic_seed or {}).get("title", ""), 60)
    seed_desc = _clean_meta_text((deterministic_seed or {}).get("description", ""), 150)

    if len(title) < 8 or _is_generic_meta_text(title):
        if seed_title:
            title = seed_title
        else:
            src = _clean_meta_text(context_summary or transcript, 120)
            words = re.findall(r"[^\s]+", src)
            title = _clean_meta_text(" ".join(words[:8]) if words else ms["plot_title"], 60)

    if len(desc) < 20 or _is_generic_meta_text(desc):
        if seed_desc:
            desc = seed_desc
        else:
            tr = _clean_meta_text(transcript or "", 150)
            if len(tr) >= 25:
                desc = tr
            else:
                ctx = _clean_meta_text(context_summary, 150)
                if len(ctx) >= 25:
                    desc = ctx
                else:
                    desc = ms["desc_generic"]

    tags = _normalize_tags(v.get("tags"))
    if not tags:
        tags = _derive_tags(title, desc, transcript, trending_hashtags)

    thumbnail_idea = _clean_meta_text(v.get("thumbnail_idea", ""), 120)
    if not thumbnail_idea or _is_generic_meta_text(thumbnail_idea):
        thumbnail_idea = _clean_meta_text(f"{ms['thumb_prefix']} {title}", 120)

    hook_text = _clean_meta_text(v.get("hook_text", ""), 80)
    if not hook_text or _is_generic_meta_text(hook_text):
        hook_text = _derive_hook_text(title, desc, transcript or context_summary, ms=ms)

    overlays = v.get("overlays")
    if isinstance(overlays, list):
        normalized_overlays: List[Dict] = []
        for item in overlays:
            if isinstance(item, dict):
                txt = _clean_meta_text(str(item.get("text", "")), 60)
                if txt and not _is_generic_meta_text(txt):
                    normalized_overlays.append({
                        "text": txt,
                        "start": int(item.get("start", 0) or 0),
                        "duration": int(item.get("duration", OVERLAY_DEFAULT_DURATION) or OVERLAY_DEFAULT_DURATION),
                    })
            elif isinstance(item, str):
                txt = _clean_meta_text(item, 60)
                if txt and not _is_generic_meta_text(txt):
                    normalized_overlays.append({
                        "text": txt,
                        "start": 0,
                        "duration": OVERLAY_DEFAULT_DURATION,
                    })
        overlays = normalized_overlays
    if not isinstance(overlays, list) or not overlays:
        overlays = [{"text": hook_text[:40], "start": 0, "duration": 2}]

    loop_prompt = _clean_meta_text(v.get("loop_prompt", ""), 80)
    if not loop_prompt or _is_generic_meta_text(loop_prompt):
        loop_prompt = _derive_loop_prompt(desc, hook_text, ms=ms)

    out = dict(v)
    out.update({
        "title": title,
        "description": desc,
        "tags": tags[:10],
        "thumbnail_idea": thumbnail_idea,
        "hook_text": hook_text,
        "overlays": overlays,
        "loop_prompt": loop_prompt,
    })
    return out


def _infer_niche_style_hint(
    video_path: Path,
    transcript: str,
    trending_hashtags: Optional[List[str]],
    context_summary: str = "",
) -> str:
    """Определяет нишу по контенту кадра/транскрипту/хэштегам и возвращает style-hint."""
    bag = " ".join(
        x for x in [
            (context_summary or "").lower(),
            (transcript or "").lower(),
            " ".join(trending_hashtags or []).lower(),
        ] if x
    )
    rules = [
        ("animals", ["cat", "dog", "pet", "кот", "кошка", "собака", "животн"]),
        ("gaming", ["game", "gaming", "minecraft", "cs2", "dota", "игр", "гейм"]),
        ("tech", ["ai", "chatgpt", "tech", "python", "code", "код", "тех", "нейросет"]),
        ("crypto", ["btc", "bitcoin", "eth", "crypto", "крипт", "биткоин", "эфир"]),
        ("motivation", ["motivation", "discipline", "успех", "привычк", "мотивац"]),
        ("food", ["recipe", "cook", "food", "еда", "рецепт", "кухн", "готов"]),
        ("travel", ["travel", "trip", "city", "страна", "путеш", "поездк"]),
    ]
    for niche, keys in rules:
        if any(k in bag for k in keys):
            return _NICHE_STYLE_PROFILES[niche]
    return ""


def _extract_first_json_array(raw: str) -> Optional[str]:
    """Пытается вырезать первый JSON-массив [...] из «грязного» текста."""
    if not raw:
        return None
    start = raw.find("[")
    if start < 0:
        return None
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(raw)):
        ch = raw[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == "\"":
                in_str = False
            continue
        if ch == "\"":
            in_str = True
            continue
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                return raw[start:i + 1]
    return None


def _parse_metadata_json_response(raw_text: str) -> List[Dict]:
    """Устойчиво парсит ответ LLM в список словарей метаданных."""
    raw = (raw_text or "").strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    candidates = [raw]
    extracted = _extract_first_json_array(raw)
    if extracted and extracted != raw:
        candidates.append(extracted)

    last_exc: Optional[Exception] = None
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, list):
                return [x for x in parsed if isinstance(x, dict)]
            if isinstance(parsed, dict):
                # Некоторые модели возвращают один объект вместо массива.
                if any(k in parsed for k in ("title", "description", "hook_text", "tags")):
                    return [parsed]
                for key in ("variants", "items", "data", "result"):
                    val = parsed.get(key)
                    if isinstance(val, list):
                        return [x for x in val if isinstance(x, dict)]
        except Exception as exc:
            last_exc = exc

    if last_exc:
        raise last_exc
    raise ValueError("Не удалось распарсить JSON-массив метаданных")


def _repair_metadata_json_with_llm(
    raw_text: str,
    lang_name: str = "English",
    content_locale: str = FALLBACK_CONTENT_LOCALE,
) -> List[Dict]:
    """Пытается восстановить валидный JSON-массив из неструктурированного ответа."""
    repair_prompt = (
        f"All string values must remain in {lang_name} (locale {content_locale}).\n"
        "Below is an invalid model response. Convert it to a VALID JSON array of objects.\n"
        "Keep only fields: title, description, tags, thumbnail_idea, hook_text, best_segment, overlays, loop_prompt.\n"
        "Output only the JSON array, no markdown or comments.\n\n"
        f"INPUT:\n{(raw_text or '')[:4000]}"
    )
    response = ollama_generate_with_timeout(
        OLLAMA_MODEL,
        repair_prompt,
        images=None,
        response_format="json",
    )
    return _parse_metadata_json_response(response.get("response", ""))


def _generate_metadata_from_context_with_llm(
    context_summary: str,
    transcript: str,
    trending_hashtags: Optional[List[str]],
    num_variants: int,
    frame_captions: Optional[List[str]] = None,
    lang_name: str = "English",
    content_locale: str = FALLBACK_CONTENT_LOCALE,
    platform_line: str = "",
) -> List[Dict]:
    """Запасной путь: генерирует metadata JSON из уже собранного контекста (без картинок)."""
    ctx = _clean_meta_text(context_summary or "", 700)
    tr = _clean_meta_text(transcript or "", 700)
    hashtags = ", ".join((trending_hashtags or [])[:10])
    captions = "\n- ".join((frame_captions or [])[:12])
    captions_block = f"Caption-per-frame:\n- {captions}\n" if captions else ""
    plat = platform_line or platform_meta_hint_line("youtube")
    prompt = (
        _build_meta_language_block(lang_name, content_locale, plat)
        + "Build metadata for a short vertical video.\n"
        "Use only the context below. Do not invent facts or use the filename.\n"
        f"Frame context: {ctx or 'none'}\n"
        f"{captions_block}"
        f"Transcript: {tr or 'none'}\n"
        f"Hashtag hints: {hashtags or 'none'}\n\n"
        f"Return {num_variants} variants.\n"
        "Response must be a valid JSON array only (no markdown or comments).\n"
        "Object fields: title, description, tags, thumbnail_idea, hook_text, best_segment, overlays, loop_prompt.\n"
    )
    response = ollama_generate_with_timeout(
        OLLAMA_MODEL,
        prompt,
        images=None,
        response_format="json",
        timeout=max(20, min(OLLAMA_TIMEOUT, 90)),
    )
    return _parse_metadata_json_response(response.get("response", ""))


def _salvage_metadata_from_raw_text(
    raw_text: str,
    video_path: Path,
    transcript: str = "",
    context_summary: str = "",
    deterministic_seed: Optional[Dict[str, str]] = None,
    ms: Optional[Dict[str, str]] = None,
) -> List[Dict]:
    """Минимальный salvage, если модель не дала валидный JSON."""
    if ms is None:
        ms = _locale_meta_strings(FALLBACK_CONTENT_LOCALE)
    text = re.sub(r"\s+", " ", (raw_text or "")).strip()
    if not text:
        return []

    # Пробуем вытащить кандидат title из кавычек или первого фрагмента.
    quoted = re.findall(r"[\"“](.{10,120}?)[\"”]", text)
    title = quoted[0] if quoted else text[:60]
    title = _clean_meta_text(title, 60)

    # Description: первые 1-2 предложения из ответа.
    parts = [p.strip() for p in re.split(r"[.!?]+", text) if p.strip()]
    desc = ". ".join(parts[:2]) if parts else text
    desc = _clean_meta_text(desc, 150)

    seed_title = _clean_meta_text((deterministic_seed or {}).get("title", ""), 60)
    seed_desc = _clean_meta_text((deterministic_seed or {}).get("description", ""), 150)

    if len(title) < 8 or "<|" in title or _is_generic_meta_text(title):
        if seed_title:
            title = seed_title
        else:
            src = _clean_meta_text(context_summary or transcript, 120)
            words = re.findall(r"[^\s]+", src)
            title = _clean_meta_text(" ".join(words[:8]) if words else ms["plot_title"], 60)

    transcript_short = _clean_meta_text(transcript or "", 150)
    context_short = _clean_meta_text(context_summary or "", 150)
    if len(desc) < 20 or "<|" in desc or _is_generic_meta_text(desc):
        if seed_desc:
            desc = seed_desc
        elif len(transcript_short) >= 30:
            desc = transcript_short
        elif len(context_short) >= 30:
            desc = context_short
        else:
            desc = ms["desc_generic"]

    return [{
        "title": title,
        "description": desc,
        "tags": [],
        "thumbnail_idea": "",
        "hook_text": "",
        "best_segment": None,
        "overlays": [],
        "loop_prompt": "",
    }]


def generate_video_metadata(
    video_path: Path,
    trending_hashtags: Optional[List[str]] = None,
    num_variants: int = AI_NUM_VARIANTS,
    content_locale: Optional[str] = None,
    account_cfg: Optional[dict] = None,
    target_platform: str = "youtube",
) -> List[Dict]:
    """Генерирует метаданные для видео через Ollama VL — модель видит реальные кадры."""
    if content_locale and str(content_locale).strip():
        resolved = normalize_content_locale(str(content_locale).strip())
    elif account_cfg is not None:
        resolved = resolve_content_locale_for_account(account_cfg)
    else:
        resolved = FALLBACK_CONTENT_LOCALE
    lang_name = content_language_name_for_prompt(resolved)
    ms = _locale_meta_strings(resolved)
    plat_line = platform_meta_hint_line(target_platform)
    safe_loc = resolved.replace("/", "_").replace("\\", "_")
    cache_path = video_path.parent / f"{video_path.stem}.{safe_loc}.ai_cache.json"
    if cache_path.exists():
        try:
            return json.loads(cache_path.read_text(encoding='utf-8'))
        except Exception:
            logger.warning("Невалидный кеш для %s, пересоздаю.", video_path)

    if not check_ollama():
        logger.warning("Ollama недоступен — fallback метаданные.")
        return _fallback_meta(video_path, num_variants, ms=ms, content_locale=resolved)

    try:
        frames = extract_frames(video_path)

        hashtag_hint = ""
        if trending_hashtags:
            hashtag_hint = f"Topics / hashtag hints: {', '.join(trending_hashtags[:10])}\n"

        # Whisper-транскрипция аудио (ФИЧА 2) — улучшает релевантность метаданных
        transcript_hint = ""
        transcript = ""
        if config.META_WHISPER_ENABLED:
            try:
                from pipeline.transcript import transcribe_for_metadata
                transcript = transcribe_for_metadata(
                    video_path,
                    model_size=config.META_WHISPER_MODEL,
                    max_duration_sec=config.META_WHISPER_MAX_SEC,
                    language=(config.META_WHISPER_LANGUAGE or locale_language_code(resolved)),
                )
                if transcript:
                    transcript_hint = f"Speech transcript: \"{transcript}\"\n"
            except Exception as _te:
                logger.warning("Транскрипция для meta не удалась: %s", _te)

        context_summary = _summarize_video_context_vl(
            video_path=video_path,
            frames=frames,
            transcript=transcript,
            lang_name=lang_name,
        )
        frame_captions = _caption_keyframes_vl(
            frames,
            transcript=transcript,
            video_path=video_path,
            lang_name=lang_name,
        )
        deterministic_seed = _build_deterministic_meta_seed(
            frame_captions=frame_captions,
            transcript=transcript,
            context_summary=context_summary,
            ms=ms,
        )
        context_hint = f"Frame summary: {context_summary}\n" if context_summary else ""
        captions_hint = (
            "Caption-per-frame:\n- "
            + "\n- ".join(frame_captions[:12])
            + "\n"
        ) if frame_captions else ""
        deterministic_hint = (
            f"Concrete seeds: title_seed='{deterministic_seed.get('title', '')}', "
            f"description_seed='{deterministic_seed.get('description', '')}'.\n"
        )

        niche_style_hint = _infer_niche_style_hint(
            video_path=video_path,
            transcript=transcript,
            trending_hashtags=trending_hashtags,
            context_summary=context_summary,
        )
        niche_style_block = f"Style hint: {niche_style_hint}\n" if niche_style_hint else ""

        lang_block = _build_meta_language_block(lang_name, resolved, plat_line)
        prompt = (
            f"{lang_block}"
            f"You analyze a short vertical video (YouTube Shorts / TikTok / Reels).\n"
            f"You see {len(frames)} evenly spaced frames.\n"
            f"{hashtag_hint}"
            f"{transcript_hint}"
            f"{context_hint}"
            f"{captions_hint}"
            f"{deterministic_hint}"
            f"{niche_style_block}"
            f"Create {num_variants} metadata variants optimized for short-form.\n\n"
            "CRITICAL: Never use the filename, path, technical tokens, or placeholders.\n"
            "Rely only on visuals, speech transcript, and hashtag context.\n"
            "Forbidden vague phrases (also in translated form): 'key moment', 'interesting moment', "
            "'bright moment', generic 'in this video'.\n"
            "hook_text: intrigue, question, or surprising fact — 3–7 words.\n"
            "title: 35–60 characters, specific to the frame/action, no empty clickbait.\n"
            "description: 90–150 characters, 1–2 simple sentences, no filler.\n"
            "Do not use: 'Subscribe for more', '#shorts #viral #trending' spam, empty hype.\n\n"
            "Response — ONLY a valid JSON array (no markdown, no prose):\n"
            '[\n'
            '  {\n'
            '    "title": "up to 60 chars",\n'
            '    "description": "up to 150 chars, emoji ok",\n'
            '    "tags": ["tag1", "tag2"],\n'
            '    "thumbnail_idea": "preview idea",\n'
            '    "hook_text": "first 3s on-screen (3-7 words)",\n'
            '    "best_segment": <seconds or null>,\n'
            '    "overlays": [{"text": "...", "start": 0, "duration": 2}],\n'
            '    "loop_prompt": "loop phrase"\n'
            '  }\n'
            ']'
        )

        variants: List[Dict] = []
        last_exc: Optional[Exception] = None
        last_raw_response = ""
        quality_min = max(1, num_variants // 2)

        for attempt in range(3):
            local_prompt = prompt
            if attempt == 1:
                local_prompt += (
                    "\nRETRY: previous titles/descriptions were too generic.\n"
                    "Make title/description more specific to frames and actions.\n"
                    "Use caption-per-frame and avoid vague wording.\n"
                )
            elif attempt == 2:
                local_prompt += (
                    "\nCRITICAL: return ONLY a valid JSON array. "
                    "No prefixes, comments, markdown, or explanations.\n"
                    "If a line sounds generic, rewrite with concrete action detail.\n"
                )

            try:
                raw_response = _video_vl_generate_with_fallback(
                    video_path=video_path,
                    prompt=local_prompt,
                    frames=frames,
                    response_format="json",
                )
                last_raw_response = raw_response or last_raw_response
                try:
                    parsed = _parse_metadata_json_response(raw_response)
                except Exception:
                    # Последняя попытка: просим модель «починить» формат JSON.
                    _dump_meta_debug_response(video_path, attempt, "parse_failed", raw_response)
                    parsed = _repair_metadata_json_with_llm(
                        raw_response,
                        lang_name=lang_name,
                        content_locale=resolved,
                    )
                normalized = [
                    _normalize_meta_variant(x) for x in parsed if isinstance(x, dict)
                ]
                normalized = [
                    _enrich_metadata_variant(
                        x,
                        video_path,
                        transcript,
                        trending_hashtags,
                        context_summary=context_summary,
                        deterministic_seed=deterministic_seed,
                        ms=ms,
                    )
                    for x in normalized
                ]
                if normalized:
                    variants = normalized[:num_variants]
                    good = sum(
                        1 for x in variants
                        if _meta_quality_ok(x) and not _variant_too_generic(x)
                    )
                    if good >= quality_min:
                        break
                    # Жёсткая браковка «слишком общих» вариантов.
                    if attempt < 2:
                        continue
                else:
                    _dump_meta_debug_response(video_path, attempt, "parsed_empty", raw_response)
            except Exception as exc:
                last_exc = exc
                _dump_meta_debug_response(video_path, attempt, f"attempt_exception: {exc}", last_raw_response)

        if not variants:
            # Запасной шанс: генерируем JSON из storyboard-контекста и транскрипта без изображений.
            if context_summary or transcript:
                try:
                    from_context = _generate_metadata_from_context_with_llm(
                        context_summary=context_summary,
                        transcript=transcript,
                        trending_hashtags=trending_hashtags,
                        num_variants=num_variants,
                        frame_captions=frame_captions,
                        lang_name=lang_name,
                        content_locale=resolved,
                        platform_line=plat_line,
                    )
                    normalized_ctx = [
                        _normalize_meta_variant(x) for x in from_context if isinstance(x, dict)
                    ]
                    normalized_ctx = [
                        _enrich_metadata_variant(
                            x,
                            video_path,
                            transcript,
                            trending_hashtags,
                            context_summary=context_summary,
                            deterministic_seed=deterministic_seed,
                            ms=ms,
                        )
                        for x in normalized_ctx
                    ]
                    if normalized_ctx:
                        variants = normalized_ctx[:num_variants]
                except Exception as ctx_exc:
                    logger.debug("metadata from context failed: %s", ctx_exc)

        if not variants:
            salvaged = _salvage_metadata_from_raw_text(
                last_raw_response,
                video_path,
                transcript=transcript,
                context_summary=context_summary,
                deterministic_seed=deterministic_seed,
                ms=ms,
            )
            if salvaged:
                logger.warning("AI metadata salvage: использую минимально восстановленный вариант.")
                variants = [_normalize_meta_variant(salvaged[0])]
                variants = [
                    _enrich_metadata_variant(
                        x,
                        video_path,
                        transcript,
                        trending_hashtags,
                        context_summary=context_summary,
                        deterministic_seed=deterministic_seed,
                        ms=ms,
                    )
                    for x in variants
                ]
            else:
                _dump_meta_debug_response(video_path, 99, "empty_variants_after_retries", last_raw_response)
                if last_exc is not None:
                    raise last_exc
                raise ValueError("LLM вернул пустой список метаданных")

        while len(variants) < num_variants:
            variants.append(dict(variants[-1]))

        try:
            dur_meta = probe_video(video_path)["duration"]
            for v in variants:
                if isinstance(v, dict):
                    v["best_segment"] = normalize_best_segment(
                        v.get("best_segment"), dur_meta
                    )
        except Exception as _norm_exc:
            logger.debug("best_segment normalize: %s", _norm_exc)

        for v in variants:
            if isinstance(v, dict):
                v["content_locale"] = resolved

        save_json(cache_path, variants)
        return variants
    except Exception as e:
        logger.error("Ошибка AI для %s: %s — использую fallback.", video_path.name, e)
        return _fallback_meta(
            video_path,
            num_variants,
            ms=_locale_meta_strings(resolved),
            content_locale=resolved,
        )


def generate_cut_points(
    video_path: Path,
    duration: float,
    num_frames: int = AI_NUM_FRAMES,
    silences: Optional[List[float]] = None,
    coarse_hints: Optional[List[float]] = None,
) -> List[float]:
    """Определяет точки нарезки видео через VL-модель — модель видит кадры."""
    silences_str = (
        f"\nТихие паузы (начало, секунды): {', '.join(f'{s:.1f}' for s in silences)}"
        if silences else ""
    )
    coarse_str = ""
    if coarse_hints:
        coarse_str = (
            "\nГрубые границы (тишина/длина клипа) — ориентиры, уточни по кадрам:\n"
            + ", ".join(f"{c:.1f}" for c in sorted(coarse_hints))
            + "\nЕсли кадр указывает лучший рез рядом — смести на 0.2–1.0 с.\n"
        )

    try:
        frames = extract_frames(video_path, num_frames)
    except Exception as e:
        logger.warning("Не удалось извлечь кадры для cut points: %s", e)
        frames = []

    prompt = (
        f"Видео длительностью {duration:.1f} секунд.\n"
        f"Тебе показаны {len(frames)} равномерно распределённых кадров.\n"
        f"Нужно нарезать на клипы по {CLIP_MIN_LEN:.0f}–{CLIP_MAX_LEN:.0f} секунд.{silences_str}{coarse_str}\n"
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


def extract_frames_around_time(
    video_path: Path,
    center_sec: float,
    duration: float,
    num_frames: int,
    window_sec: float,
) -> List[bytes]:
    """JPEG-кадры равномерно по времени в окне [center ± window/2] ∩ [0, duration]."""
    if duration <= 0 or num_frames < 1:
        return []
    half = window_sec / 2.0
    t0 = max(0.0, center_sec - half)
    t1 = min(duration, center_sec + half)
    if t1 <= t0 + 1e-3:
        t0 = max(0.0, center_sec - 0.5)
        t1 = min(duration, center_sec + 0.5)
    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        return []
    fps = float(cap.get(cv2.CAP_PROP_FPS) or 30.0)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    if num_frames == 1:
        times = [(t0 + t1) / 2.0]
    else:
        times = [t0 + (t1 - t0) * i / (num_frames - 1) for i in range(num_frames)]
    frames: List[bytes] = []
    for t in times:
        frame_idx = int(min(max(0.0, t * fps), max(0, total_frames - 1)))
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
        ret, frame = cap.read()
        if not ret:
            continue
        _, buf = cv2.imencode(".jpg", frame)
        frames.append(buf.tobytes())
    cap.release()
    return frames


def _parse_first_timestamp(text: str) -> Optional[float]:
    nums = re.findall(r"\b(\d+(?:\.\d+)?)\b", text)
    if not nums:
        return None
    return float(nums[0])


def refine_single_cut_boundary_vl(
    video_path: Path,
    duration: float,
    candidate_sec: float,
    window_sec: float,
    num_frames: int,
    timeout: int,
) -> Optional[float]:
    """
    Один VL-запрос: уточнить секунду разреза в окне вокруг candidate_sec.
    Возвращает абсолютное время на шкале ролика или None.
    """
    half = window_sec / 2.0
    t0 = max(0.0, candidate_sec - half)
    t1 = min(duration, candidate_sec + half)
    if t1 <= t0 + 1e-3:
        return None
    frames = extract_frames_around_time(
        video_path, candidate_sec, duration, num_frames, window_sec
    )
    if len(frames) < 2:
        logger.warning(
            "[slicer] refine: мало кадров вокруг %.2f с — пропуск", candidate_sec
        )
        return None
    prompt = (
        f"Окно фрагмента: {t0:.2f}–{t1:.2f} с (вся длительность ролика {duration:.1f} с).\n"
        f"Показано {len(frames)} кадров по времени слева направо.\n"
        "Нужна ОДНА абсолютная секунда от начала файла для лучшего разреза клипа "
        "(тишина, смена сцены, конец реплики).\n"
        f"Число должно быть в [{t0:.1f}, {t1:.1f}].\n"
        "Ответ: только одно число, без текста."
    )
    try:
        response = ollama_generate_with_timeout(
            OLLAMA_MODEL,
            prompt,
            images=frames,
            timeout=timeout,
        )
        raw = response.get("response", "") if isinstance(response, dict) else str(response)
        val = _parse_first_timestamp(str(raw))
        if val is None:
            return None
        val = max(t0, min(t1, val))
        val = max(0.0, min(duration, val))
        return val
    except Exception as e:
        logger.warning("[slicer] refine VL около %.2f с: %s", candidate_sec, e)
        return None


def refine_disputed_cut_boundaries(
    video_path: Path,
    duration: float,
    cuts: List[float],
    silence_intervals: List[Tuple[float, float]],
) -> List[float]:
    """
    Отдельные VL-вызовы для границ, далёких от тишины (см. rank_disputed_cuts_for_refinement).
    Лимит — SLICER_DISPUTED_MAX_CALLS. Без интервалов тишины не вызывается.
    """
    from pipeline.slicer_cut_utils import rank_disputed_cuts_for_refinement

    if not getattr(config, "SLICER_DISPUTED_VL_REFINE", False):
        return cuts
    if not check_ollama():
        logger.warning("Ollama недоступен — refine спорных границ пропущен")
        return cuts
    if not silence_intervals:
        logger.info("[slicer] нет интервалов тишины — refine спорных границ пропущен")
        return cuts
    if not cuts:
        return cuts

    prox = float(getattr(config, "SLICER_DISPUTED_SILENCE_PROX_SEC", 1.2))
    ranked = rank_disputed_cuts_for_refinement(cuts, silence_intervals, prox)
    max_calls = int(getattr(config, "SLICER_DISPUTED_MAX_CALLS", 12))
    if max_calls <= 0 or not ranked:
        return cuts

    win = float(getattr(config, "SLICER_DISPUTED_WINDOW_SEC", 2.5))
    nf = max(2, int(getattr(config, "SLICER_DISPUTED_FRAMES", 5)))
    to = int(getattr(config, "SLICER_DISPUTED_VL_TIMEOUT", 45))

    ranked = ranked[:max_calls]
    replacements: Dict[float, float] = {}

    for i, t in enumerate(ranked):
        new_t = refine_single_cut_boundary_vl(
            video_path, duration, t, win, nf, to,
        )
        if new_t is not None and abs(new_t - t) > 1e-3:
            replacements[float(t)] = new_t
            logger.info(
                "[slicer] спорная граница %d/%d: %.2f с → %.2f с",
                i + 1,
                len(ranked),
                t,
                new_t,
            )

    if not replacements:
        return cuts

    out: List[float] = []
    for c in cuts:
        repl: Optional[float] = None
        for old_t, new_v in replacements.items():
            if abs(float(c) - old_t) < 0.05:
                repl = new_v
                break
        out.append(repl if repl is not None else c)
    return sorted(set(out))


def _fallback_meta(
    video_path: Path,
    num_variants: int,
    ms: Optional[Dict[str, str]] = None,
    content_locale: str = FALLBACK_CONTENT_LOCALE,
) -> List[Dict]:
    """Возвращает заглушку метаданных при недоступном AI."""
    if ms is None:
        ms = _locale_meta_strings(content_locale)
    base_lang = locale_language_code(content_locale)
    tags_by_lang = {
        "ru": ["шортс", "видео", "момент"],
        "es": ["shorts", "video", "momento"],
        "pt": ["shorts", "video", "momento"],
        "de": ["shorts", "video", "moment"],
        "fr": ["shorts", "video", "moment"],
        "en": ["shorts", "video", "moment"],
    }
    base = {
        "title":           ms["fallback_title"],
        "description":     ms["fallback_desc"],
        "tags":            tags_by_lang.get(base_lang, tags_by_lang["en"]),
        "thumbnail_idea":  ms["fallback_thumb"],
        "hook_text":       "",
        "best_segment":    None,
        "overlays":        [],
        "loop_prompt":     "",
        "content_locale":  content_locale,
    }
    return [dict(base)] * num_variants


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
        from pipeline import utils as _u

        _px = _u.requests_proxies_from_proxy_url(_u.load_proxy())
        resp = requests.get(thumbnail_url, timeout=10, proxies=_px)
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
