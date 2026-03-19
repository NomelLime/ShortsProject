"""
pipeline/transcript.py — Транскрипция аудиодорожки для AI-метаданных.

Использует faster-whisper (уже установлен для subtitler.py).
Возвращает текст (до 500 символов) для инжекции в LLM-промпт generate_video_metadata().

Особенности:
  - Кеширует результат в <видео>.transcript_cache.txt рядом с видео
  - При повторном вызове читает из кеша (не транскрибирует повторно)
  - При ошибке или отсутствии аудио возвращает "" (graceful degradation)
  - НЕ захватывает GPU самостоятельно — вызывающий код (ai.py) должен
    быть внутри GPU:LLM блока
"""

from __future__ import annotations

import logging
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

_TRANSCRIPT_MAX_CHARS = 500


def transcribe_for_metadata(
    video_path: Path,
    model_size: str = "base",
    max_duration_sec: int = 120,
    language: str = "",
) -> str:
    """
    Транскрибирует аудиодорожку видео для использования в AI-метаданных.

    При ошибке или отсутствии аудио возвращает пустую строку (graceful).
    Результат кешируется: при повторном вызове возвращается из кеша.

    Args:
        video_path:       путь к видеофайлу
        model_size:       размер Whisper-модели (tiny/base/small/medium)
        max_duration_sec: максимальная длина для транскрипции (секунды)
        language:         ISO код языка ("" = автодетект)

    Returns:
        Текст транскрипта, обрезанный до 500 символов. Пустая строка при ошибке.

    Note:
        Вызывать ВНУТРИ GPU:LLM блока (Editor уже удерживает GPU).
        faster-whisper использует GPU автоматически если доступен.
    """
    video_path = Path(video_path)
    cache_path = video_path.with_suffix(".transcript_cache.txt")

    # ── 1. Кеш ───────────────────────────────────────────────────────────────
    if cache_path.exists():
        try:
            cached = cache_path.read_text(encoding="utf-8").strip()
            logger.debug("[Transcript] Из кеша: %s (%d символов)", video_path.name, len(cached))
            return cached[:_TRANSCRIPT_MAX_CHARS]
        except Exception as exc:
            logger.warning("[Transcript] Не удалось прочитать кеш %s: %s", cache_path, exc)

    # ── 2. Извлечь аудио через ffmpeg → temp WAV (16kHz mono) ────────────────
    tmp_wav: str | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
            tmp_wav = f.name

        cmd = [
            "ffmpeg", "-y",
            "-i", str(video_path),
            "-t", str(max_duration_sec),
            "-ar", "16000",          # 16 kHz — требование Whisper
            "-ac", "1",              # mono
            "-vn",                   # только аудио
            "-f", "wav",
            tmp_wav,
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=60)
        if result.returncode != 0:
            logger.debug("[Transcript] ffmpeg: нет аудио или ошибка — %s", video_path.name)
            return ""

    except Exception as exc:
        logger.warning("[Transcript] Ошибка извлечения аудио из %s: %s", video_path.name, exc)
        _cleanup_tmp(tmp_wav)
        return ""

    # ── 3. Транскрибировать через faster-whisper ──────────────────────────────
    try:
        from faster_whisper import WhisperModel  # type: ignore

        model = WhisperModel(
            model_size,
            device="auto",       # GPU если доступен, иначе CPU
            compute_type="auto",
        )
        segments, _info = model.transcribe(
            tmp_wav,
            language=language or None,
            beam_size=1,         # быстро, качество достаточно для метаданных
            vad_filter=True,     # пропускать тишину
        )

        text = " ".join(seg.text.strip() for seg in segments).strip()
        text = text[:_TRANSCRIPT_MAX_CHARS]

        logger.info(
            "[Transcript] %s: %d символов (модель: %s)",
            video_path.name, len(text), model_size,
        )

    except ImportError:
        logger.warning("[Transcript] faster-whisper не установлен — пропуск транскрипции")
        _cleanup_tmp(tmp_wav)
        return ""
    except Exception as exc:
        logger.warning("[Transcript] Ошибка транскрипции %s: %s", video_path.name, exc)
        _cleanup_tmp(tmp_wav)
        return ""
    finally:
        _cleanup_tmp(tmp_wav)

    # ── 4. Сохранить в кеш ───────────────────────────────────────────────────
    if text:
        try:
            cache_path.write_text(text, encoding="utf-8")
            logger.debug("[Transcript] Кеш записан: %s", cache_path)
        except Exception as exc:
            logger.warning("[Transcript] Не удалось записать кеш: %s", exc)

    return text


def _cleanup_tmp(path: str | None) -> None:
    """Удаляет временный файл, игнорируя ошибки."""
    if path:
        try:
            import os
            os.unlink(path)
        except OSError:
            pass
