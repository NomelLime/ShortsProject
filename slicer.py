# slicer.py
"""
Нарезка видео на клипы.

Изменения:
  - Точки нарезки определяются ТОЛЬКО через AI (Ollama + визуальный анализ).
    silencedetect и scenedetect удалены.
  - AI получает кадры видео + YOLO-детекции по каждому кадру (из metadata_variants),
    что позволяет учитывать содержание при выборе точек нарезки.
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
    AI_NUM_FRAMES,
)
from pipeline.utils import probe_video

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
        cmd += ["-y", str(out)]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if r.returncode != 0:
            logger.warning("Клип %d: %s", idx, r.stderr[-120:])
            return False
        return True
    except Exception as e:
        logger.warning("Клип %d: %s", idx, e)
        return False


def _clip_len_for_segment(start: float, total: float) -> Optional[float]:
    remaining = total - start
    if remaining <= _BEST_SEGMENT_MIN_LEN:
        return None
    desired = min(CLIP_MAX_LEN, remaining)
    if desired < CLIP_MIN_LEN:
        return desired
    return desired


def stage_slice(
    source_name: str,
    video_path: Path,
    clip_dir: Path,
    metadata_variants: Optional[List[dict]] = None,
) -> List[Path]:
    """
    Этап 1: нарезает одно видео на клипы с использованием AI для определения точек.

    Порядок работы:
      1. Если best_segment известен из AI-метаданных — первый клип начинается с него.
      2. AI (Ollama) анализирует видео (кадры + YOLO-детекции) и предлагает точки нарезки.
      3. Остаток видео нарезается по этим точкам через group_into_clips.
      4. При недоступности AI — равномерная нарезка.

    Возвращает список путей к нарезанным файлам.
    """
    logger.info("✂️  Нарезка: %s", video_path.name)
    info = probe_video(str(video_path))
    dur  = info["duration"]
    logger.info("   Длительность: %.1f сек", dur)

    if dur <= SHORT_VIDEO_THRESHOLD:
        logger.info("   Видео короче %s сек, копирую как есть.", SHORT_VIDEO_THRESHOLD)
        clip_dir.mkdir(parents=True, exist_ok=True)
        out = clip_dir / f"{source_name}_clip0001.mp4"
        shutil.copy2(video_path, out)
        return [out]

    # Извлекаем best_segment и YOLO-данные из метаданных
    best_segment: Optional[float]      = None
    yolo_per_frame: List[List[str]]    = []

    if metadata_variants:
        meta0 = metadata_variants[0]
        raw = meta0.get("best_segment")
        if raw is not None:
            try:
                val = float(raw)
                if 0.0 <= val < dur - _BEST_SEGMENT_MIN_LEN:
                    best_segment = val
                else:
                    logger.debug(
                        "best_segment=%.2f вне допустимого диапазона для видео %.1f сек, игнорирую.",
                        val, dur,
                    )
            except (TypeError, ValueError) as e:
                logger.debug("Не удалось прочитать best_segment: %s", e)

        # YOLO-данные по кадрам, собранные ранее на этапе AI
        yolo_per_frame = meta0.get("yolo_per_frame", [])

    clip_dir.mkdir(parents=True, exist_ok=True)
    result: List[Path] = []
    clip_counter = 1

    # ── Клип с best_segment (идёт ПЕРВЫМ) ──────────────────────────────────
    best_segment_end: Optional[float] = None

    if best_segment is not None:
        bs_len = _clip_len_for_segment(best_segment, dur)
        if bs_len is not None:
            best_segment_end = best_segment + bs_len
            out = clip_dir / f"{source_name}_clip{clip_counter:04d}.mp4"
            logger.info(
                "   🌟 best_segment клип: [%.2f → %.2f] (%.1f сек)",
                best_segment, best_segment_end, bs_len,
            )
            if extract_clip(str(video_path), out, clip_counter, best_segment, best_segment_end):
                result.append(out)
            clip_counter += 1
        else:
            logger.debug("best_segment слишком близко к концу видео, пропускаю.")

    # ── AI-определение точек нарезки ────────────────────────────────────────
    logger.info("   🤖 AI определяет точки нарезки...")

    # Импорт здесь чтобы избежать кольцевого импорта на уровне модуля
    from pipeline.ai import generate_cut_points

    ai_cuts = generate_cut_points(
        video_path=video_path,
        duration=dur,
        yolo_per_frame=yolo_per_frame,
        num_frames=AI_NUM_FRAMES,
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
