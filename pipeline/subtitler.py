"""
pipeline/subtitler.py — Авто-субтитры + перевод.

Этапы:
  1. Транскрипция: faster-whisper (локально, GPU через get_gpu_manager())
  2. Перевод: Helsinki-NLP/opus-mt через transformers (CPU) ИЛИ Ollama (если доступна)
  3. Hardsub: ffmpeg subtitles= фильтр — встраивает субтитры в видео

Конфиг (config.py через .env):
  SUBTITLE_ENABLED    = 1            — включить/выключить
  SUBTITLE_LANGUAGES  = ru           — языки через запятую (ru,en,es,pt)
  WHISPER_MODEL_SIZE  = base         — tiny/base/small/medium/large
  SUBTITLE_STYLE      = bottom_white — стиль (bottom_white | top_yellow)

Публичный API:
  add_subtitles(clip_path, source_lang="auto") → Path  — in-place или новый файл
  transcribe_srt(audio_path)                  → str   — SRT-строка (для дебага)
"""
from __future__ import annotations

import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)


# ── Стили субтитров ────────────────────────────────────────────────────────

_STYLES = {
    "bottom_white": (
        "Fontname=Arial,Fontsize=20,PrimaryColour=&HFFFFFF,OutlineColour=&H000000,"
        "BorderStyle=1,Outline=2,Shadow=0,Alignment=2,MarginV=30"
    ),
    "top_yellow": (
        "Fontname=Arial,Fontsize=20,PrimaryColour=&H00FFFF,OutlineColour=&H000000,"
        "BorderStyle=1,Outline=2,Shadow=0,Alignment=8,MarginV=20"
    ),
}


# ── Публичный API ──────────────────────────────────────────────────────────

def add_subtitles(
    clip_path: Path,
    source_lang: str = "auto",
) -> Path:
    """
    Добавляет hardsub субтитры к клипу.
    Возвращает путь к новому файлу (suffix _sub.mp4).
    Если отключено или ошибка — возвращает исходный clip_path.
    """
    from pipeline import config as cfg

    if not getattr(cfg, "SUBTITLE_ENABLED", False):
        return clip_path

    langs_str = str(getattr(cfg, "SUBTITLE_LANGUAGES", "ru"))
    languages = [lg.strip() for lg in langs_str.split(",") if lg.strip()]
    if not languages:
        return clip_path

    # Используем только первый язык для hardsub (многоязычные = отдельные клоны)
    target_lang = languages[0]
    model_size  = str(getattr(cfg, "WHISPER_MODEL_SIZE", "base"))
    style_name  = str(getattr(cfg, "SUBTITLE_STYLE", "bottom_white"))

    return _process_clip(clip_path, source_lang, target_lang, model_size, style_name)


def add_subtitles_for_lang(
    clip_path: Path,
    target_lang: str,
    source_lang: str = "auto",
) -> Path:
    """
    Добавляет hardsub субтитры для конкретного языка (без глобального списка).
    Используется для JIT-подготовки ролика под locale аккаунта.
    """
    from pipeline import config as cfg

    if not getattr(cfg, "SUBTITLE_ENABLED", False):
        return clip_path

    lang = (target_lang or "").strip().lower()
    if not lang:
        return clip_path

    model_size = str(getattr(cfg, "WHISPER_MODEL_SIZE", "base"))
    style_name = str(getattr(cfg, "SUBTITLE_STYLE", "bottom_white"))
    return _process_clip(clip_path, source_lang, lang, model_size, style_name)


def add_subtitles_multi(
    clip_path: Path,
    source_lang: str = "auto",
) -> List[Tuple[str, Path]]:
    """
    Добавляет субтитры для каждого настроенного языка.
    Возвращает [(lang, path)] — список языков и пути к файлам.
    """
    from pipeline import config as cfg

    if not getattr(cfg, "SUBTITLE_ENABLED", False):
        return []

    langs_str = str(getattr(cfg, "SUBTITLE_LANGUAGES", "ru"))
    languages = [lg.strip() for lg in langs_str.split(",") if lg.strip()]
    model_size = str(getattr(cfg, "WHISPER_MODEL_SIZE", "base"))
    style_name = str(getattr(cfg, "SUBTITLE_STYLE", "bottom_white"))

    results: List[Tuple[str, Path]] = []
    for lang in languages:
        try:
            out = _process_clip(clip_path, source_lang, lang, model_size, style_name)
            if out != clip_path:
                results.append((lang, out))
        except Exception as exc:
            logger.warning("[Subtitler] Ошибка для языка %s: %s", lang, exc)

    return results


def transcribe_srt(audio_or_video: Path, model_size: str = "base", lang: str = "auto") -> str:
    """Возвращает SRT-строку. Используется для дебага."""
    segments = _whisper_transcribe(audio_or_video, model_size, lang)
    return _segments_to_srt(segments)


# ── Внутренняя логика ──────────────────────────────────────────────────────

def _process_clip(
    clip_path: Path,
    source_lang: str,
    target_lang: str,
    model_size: str,
    style_name: str,
) -> Path:
    """Полный пайплайн: транскрипция → перевод → hardsub."""
    logger.info("[Subtitler] Обработка: %s → язык=%s", clip_path.name, target_lang)

    # 1. Транскрипция
    segments = _whisper_transcribe(clip_path, model_size, source_lang)
    if not segments:
        logger.warning("[Subtitler] Транскрипция вернула пустой результат: %s", clip_path.name)
        return clip_path

    # 2. Перевод (если нужен)
    detected_lang = segments[0].get("lang", source_lang) if segments else source_lang
    if target_lang != "auto" and target_lang != detected_lang:
        segments = _translate_segments(segments, target_lang)

    # 3. Hardsub через ffmpeg
    srt_content = _segments_to_srt(segments)
    return _apply_hardsub(clip_path, srt_content, style_name)


def _whisper_transcribe(
    file_path: Path,
    model_size: str,
    language: str,
) -> List[dict]:
    """
    Транскрибирует аудио через faster-whisper.
    Возвращает список dict: {start, end, text, lang}
    """
    try:
        from faster_whisper import WhisperModel  # type: ignore
    except ImportError:
        logger.warning("[Subtitler] faster-whisper не установлен: pip install faster-whisper")
        return []

    try:
        from pipeline.agents.gpu_manager import get_gpu_manager, GPUPriority
        gpu = get_gpu_manager()

        # Определяем device: cuda если GPU доступен
        device = "cuda" if _cuda_available() else "cpu"
        compute_type = "float16" if device == "cuda" else "int8"

        logger.debug("[Subtitler] Whisper %s на %s", model_size, device)

        with gpu.acquire("SUBTITLER", GPUPriority.ENCODE):
            model = WhisperModel(model_size, device=device, compute_type=compute_type)
            lang_arg = None if language == "auto" else language
            segments_iter, info = model.transcribe(
                str(file_path),
                language=lang_arg,
                beam_size=5,
            )
            detected = info.language

            result = []
            for seg in segments_iter:
                result.append({
                    "start": seg.start,
                    "end":   seg.end,
                    "text":  seg.text.strip(),
                    "lang":  detected,
                })

        logger.debug("[Subtitler] Транскрибировано %d сегментов (lang=%s)", len(result), detected)
        return result

    except Exception as exc:
        logger.error("[Subtitler] Ошибка faster-whisper: %s", exc, exc_info=True)
        return []


def _translate_segments(segments: List[dict], target_lang: str) -> List[dict]:
    """
    Переводит текст сегментов.
    Сначала пробует Helsinki-NLP, fallback → Ollama.
    """
    texts = [s["text"] for s in segments]
    translated = _translate_helsinki(texts, target_lang)
    if not translated:
        translated = _translate_ollama(texts, target_lang)

    if translated and len(translated) == len(segments):
        result = []
        for seg, tr in zip(segments, translated):
            seg = dict(seg)
            seg["text"] = tr
            result.append(seg)
        return result

    return segments  # fallback — оригинал


def _translate_helsinki(texts: List[str], target_lang: str) -> Optional[List[str]]:
    """Перевод через Helsinki-NLP/opus-mt (transformers, CPU)."""
    try:
        from transformers import MarianMTModel, MarianTokenizer  # type: ignore
    except ImportError:
        logger.debug("[Subtitler] transformers не установлен — пропускаем Helsinki перевод")
        return None

    # Определяем source lang из первых сегментов — берём "en" как default
    model_name = f"Helsinki-NLP/opus-mt-en-{target_lang}"
    try:
        tokenizer = MarianTokenizer.from_pretrained(model_name)
        model     = MarianMTModel.from_pretrained(model_name)

        # Переводим батчами по 8 сегментов
        result = []
        batch_size = 8
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            tokens = tokenizer(batch, return_tensors="pt", padding=True, truncation=True, max_length=512)
            translated_tokens = model.generate(**tokens)
            decoded = tokenizer.batch_decode(translated_tokens, skip_special_tokens=True)
            result.extend(decoded)

        logger.debug("[Subtitler] Helsinki перевод: %d сегментов → %s", len(result), target_lang)
        return result

    except Exception as exc:
        logger.debug("[Subtitler] Helsinki ошибка: %s", exc)
        return None


def _translate_ollama(texts: List[str], target_lang: str) -> Optional[List[str]]:
    """Перевод через Ollama (fallback)."""
    try:
        import urllib.request, json
        combined = "\n---\n".join(texts)
        prompt = (
            f"Переведи следующий текст на язык: {target_lang}. "
            f"Каждый абзац разделён '---'. Ответь только переводом, сохраняя разделители '---'.\n\n"
            f"{combined}"
        )
        payload = json.dumps({"model": "qwen2.5:7b", "prompt": prompt, "stream": False}).encode()
        req = urllib.request.Request(
            "http://localhost:11434/api/generate",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read())
            translated_combined = data.get("response", "")
            parts = translated_combined.split("---")
            if len(parts) == len(texts):
                return [p.strip() for p in parts]
    except Exception as exc:
        logger.debug("[Subtitler] Ollama перевод ошибка: %s", exc)
    return None


def _segments_to_srt(segments: List[dict]) -> str:
    """Конвертирует список сегментов в SRT-строку."""
    lines = []
    for i, seg in enumerate(segments, 1):
        start = _fmt_time(seg["start"])
        end   = _fmt_time(seg["end"])
        text  = seg["text"]
        lines.append(f"{i}\n{start} --> {end}\n{text}\n")
    return "\n".join(lines)


def _fmt_time(seconds: float) -> str:
    """Форматирует секунды как SRT timestamp: HH:MM:SS,mmm"""
    ms  = int((seconds % 1) * 1000)
    s   = int(seconds)
    m   = s // 60
    h   = m // 60
    m  %= 60
    s  %= 60
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


def _apply_hardsub(clip_path: Path, srt_content: str, style_name: str) -> Path:
    """
    Встраивает SRT как hardsub через ffmpeg subtitles= фильтр.
    Возвращает путь к новому файлу (suffix _sub.mp4).
    """
    out_path = clip_path.with_stem(clip_path.stem + "_sub")

    style = _STYLES.get(style_name, _STYLES["bottom_white"])

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".srt", delete=False, encoding="utf-8"
    ) as srt_file:
        srt_file.write(srt_content)
        srt_path = srt_file.name

    try:
        # Экранируем путь к SRT для Windows
        srt_escaped = srt_path.replace("\\", "/").replace(":", "\\:")
        cmd = [
            "ffmpeg", "-y",
            "-i", str(clip_path),
            "-vf", f"subtitles='{srt_escaped}':force_style='{style}'",
            "-c:a", "copy",
            str(out_path),
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=120)
        if result.returncode == 0 and out_path.exists():
            logger.info("[Subtitler] Hardsub OK: %s", out_path.name)
            return out_path
        else:
            logger.warning(
                "[Subtitler] ffmpeg hardsub ошибка: %s",
                result.stderr.decode(errors="replace")[:200],
            )
            return clip_path
    except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
        logger.warning("[Subtitler] ffmpeg ошибка: %s", exc)
        return clip_path
    finally:
        try:
            os.unlink(srt_path)
        except OSError:
            pass


def _cuda_available() -> bool:
    """Проверяет доступность CUDA."""
    try:
        import torch  # type: ignore
        return torch.cuda.is_available()
    except ImportError:
        pass
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            capture_output=True, timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False
