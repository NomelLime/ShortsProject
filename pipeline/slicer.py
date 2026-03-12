# slicer.py
"""
Нарезка видео на клипы.

Изменения:
  - Точки нарезки определяются ТОЛЬКО через AI (Ollama + визуальный анализ).
    silencedetect and scenedetect deleted.
  - AI (VL-модель) получает реальные кадры видео и определяет точки нарезки
    на основе визуального содержания: смена сцены, переходы, паузы.
  - При недоступности AI — равномерная нарезка каждые CLIP_MAX_LEN секунд.
"""

import logging
import shutil
import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

from pipeline.config import (
    CLIP_MIN_LEN, CLIP_MAX_LEN,
    SHORT_VIDEO_THRESHOLD,
    SILENCE_THRESHOLD, SILENCE_MIN_DUR,
    AI_NUM_FRAMES,
)
from pipeline.utils import probe_video

# generate_video_metadata и generate_cut_points — ленивый импорт внутри функций
# чтобы избежать кольцевого импорта: ai → utils → slicer → ai

logger = logging.getLogger(__name__)

_BEST_SEGMENT_MIN_LEN = 3.0


def group_into_clips(cut_points: List[float], total: float) -> List[Tuple[float, float]]:
    """Группирует точки нарезки в клипы длиной CLIP_MIN_LEN..CLIP_MAX_LEN."""
    if total <= 0:
        return []
    if total <= CLIP_MIN_LEN:
        return [(0.0, total)]

    clips = []
    cur   = 0.0
    pts   = sorted(p for p in cut_points if 0 < p < total)
    pts.append(total)

    while cur < total:
        ideal      = cur + CLIP_MAX_LEN
        min_e      = cur + CLIP_MIN_LEN
        candidates = [p for p in pts if min_e <= p <= min(ideal, total)]
        end        = max(candidates) if candidates else min(ideal, total)
        if end > cur:
            clips.append((cur, end))
        cur = end

    return clips


def extract_clip(src: str, out: Path, idx: int, start: float, end: float) -> bool:
    """Вырезает сегмент, применяет crop до 9:16 если нужно."""
    try:
        info = probe_video(src)
        w, h = info["width"], info["height"]
        cmd = [
            "ffmpeg", "-ss", str(start), "-i", src,
            "-t", str(end - start),
            "-c:v", "libx264", "-c:a", "aac", "-preset", "fast",
        ]
        if w / h > 9 / 16:
            cmd += ["-vf", "crop=ih*9/16:ih:(iw-ow)/2:0"]
        cmd += [str(out)]
        subprocess.run(cmd, check=True, capture_output=True)
        return True
    except Exception as e:
        logger.error("Ошибка нарезки клипа %d: %s", idx, e)
        return False


def slice_short_video(video_path: Path, clip_dir: Path) -> List[Path]:
    """Обрабатывает короткое видео как один клип."""
    out = clip_dir / f"{video_path.stem}_clip0000.mp4"
    if extract_clip(str(video_path), out, 0, 0, probe_video(video_path)['duration']):
        return [out]
    return []


def slice_long_video(video_path: Path, clip_dir: Path,
                     metadata_variants: Optional[List[dict]] = None) -> List[Path]:
    """Нарезает длинное видео через AI."""
    # Ленивый импорт — избегаем кольцевого импорта на уровне модуля
    from pipeline.ai import generate_video_metadata, generate_cut_points  # noqa: F811

    dur = probe_video(video_path)['duration']
    source_name = video_path.stem
    result = []
    clip_counter = 0

    # ── Best segment как отдельный клип ──────────────────────────────────────
    if metadata_variants:
        metadata = metadata_variants[0]
    else:
        metadata = generate_video_metadata(video_path, num_variants=1)[0]
    best_segment = metadata.get("best_segment")
    best_segment_end = best_segment + _BEST_SEGMENT_MIN_LEN if best_segment else None

    if best_segment is not None:
        out = clip_dir / f"{source_name}_best_segment.mp4"
        if extract_clip(str(video_path), out, clip_counter, best_segment, best_segment_end):
            result.append(out)
        clip_counter += 1

    # ── AI-точки нарезки ────────────────────────────────────────
    logger.info("   🤖 AI определяет точки нарезки...")

    # Reintroduced silencedetect
    silences = detect_silences(str(video_path))

    ai_cuts = generate_cut_points(
        video_path=video_path,
        duration=dur,
        silences=silences,
    )
    logger.info("   AI точки: %s", ", ".join(f"{c:.1f}" for c in ai_cuts) if ai_cuts else "нет → равномерно")

    # ── Нарезка диапазонов, исключая занятый отрезок best_segment ───────────
    segments_to_slice: List[Tuple[float, float]] = []

    if best_segment is not None and best_segment_end is not None:
        if best_segment > CLIP_MIN_LEN:
            segments_to_slice.append((0.0, best_segment))
        if dur - best_segment_end > CLIP_MIN_LEN:
            segments_to_slice.append((best_segment_end, dur))
    else:
        segments_to_slice.append((0.0, dur))

    for seg_start, seg_end in segments_to_slice:
        seg_len = seg_end - seg_start
        if seg_len <= 0:
            continue

        # Фильтруем AI-точки в пределах текущего сегмента, смещаем на начало сегмента
        local_cuts = [c - seg_start for c in ai_cuts if seg_start < c < seg_end]
        clips_ts   = group_into_clips(local_cuts, seg_len)
        logger.info(
            "   Сегмент [%.1f→%.1f]: клипов %d",
            seg_start, seg_end, len(clips_ts),
        )

        for local_s, local_e in clips_ts:
            abs_s = seg_start + local_s
            abs_e = seg_start + local_e
            out   = clip_dir / f"{source_name}_clip{clip_counter:04d}.mp4"
            if extract_clip(str(video_path), out, clip_counter, abs_s, abs_e):
                result.append(out)
            clip_counter += 1

    logger.info(
        "   Итого клипов: %d (включая best_segment: %s)",
        len(result), best_segment is not None,
    )
    return result


# FIX #8: metadata_variants добавлен — совместимость с main_processing.py
def stage_slice(video_path: Path, clip_dir: Path,
               metadata_variants: Optional[List[dict]] = None) -> List[Path]:
    """Главный этап нарезки."""
    if probe_video(video_path)['duration'] < SHORT_VIDEO_THRESHOLD:
        return slice_short_video(video_path, clip_dir)
    return slice_long_video(video_path, clip_dir, metadata_variants=metadata_variants)


# New: Silencedetect function
def detect_silences(video_path: str, threshold: float = SILENCE_THRESHOLD, min_dur: float = SILENCE_MIN_DUR) -> List[float]:
    try:
        out = subprocess.run(['ffmpeg', '-i', video_path, '-af', f'silencedetect=n={threshold}dB:d={min_dur}', '-f', 'null', '-'], capture_output=True, text=True)
        lines = out.stderr.splitlines()
        silences = []
        for line in lines:
            if 'silence_start' in line:
                silences.append(float(line.split('silence_start: ')[1].split(' ')[0]))
        return silences
    except Exception as e:
        logger.warning(f"Silencedetect failed: {e}")
        return []