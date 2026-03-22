"""
Постобработка точек нарезки: округление, снап к keyframe, отсев коротких сегментов,
грубые границы по тишине для двухпроходного VL, нормализация best_segment.
"""
from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

from pipeline import config

logger = logging.getLogger(__name__)

_BEST_SEG_MIN = 3.0  # синхронно с slicer._BEST_SEGMENT_MIN_LEN


def normalize_best_segment(
    best: Optional[float],
    duration: float,
    seg_min_len: float = _BEST_SEG_MIN,
) -> Optional[float]:
    """
    Стабильный best_segment: не выходит за длительность, сегмент не короче seg_min_len.
    Значения вроде отрицательных или полного выхода за ролик → None.
    """
    if best is None or duration <= 0:
        return None
    try:
        b = float(best)
    except (TypeError, ValueError):
        return None
    if b < 0:
        b = 0.0
    decimals = max(0, int(getattr(config, "SLICER_ROUND_DECIMALS", 1)))
    b = round(b, decimals)
    if b >= duration:
        return None
    if b + seg_min_len > duration + 1e-3:
        b = max(0.0, round(duration - seg_min_len - 0.05, decimals))
    if b < 0 or b + seg_min_len > duration + 0.1:
        return None
    return b


def round_times(times: Sequence[float], decimals: int) -> List[float]:
    d = max(0, decimals)
    return [round(float(t), d) for t in times]


def _snap_one(t: float, keyframes: Sequence[float], max_delta: float) -> float:
    if not keyframes:
        return t
    nearest = min(keyframes, key=lambda k: abs(k - t))
    if abs(nearest - t) <= max_delta:
        return nearest
    return t


def probe_iframe_times(video_path: Path, duration: float) -> List[float]:
    """
    Времена I-кадров (ffprobe). На длинных роликах сверх порога не вызывается.
    """
    max_dur = float(getattr(config, "SLICER_KEYFRAME_PROBE_MAX_DURATION_SEC", 600))
    if duration > max_dur:
        logger.debug(
            "[slicer] keyframe probe пропущен: duration %.0f > max %.0f",
            duration,
            max_dur,
        )
        return []
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "frame=pkt_pts_time,pict_type",
        "-of",
        "csv=p=0",
        str(video_path),
    ]
    try:
        r = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=min(120.0, max(30.0, duration * 0.5)),
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        logger.warning("[slicer] ffprobe keyframes: %s", e)
        return []
    if r.returncode != 0:
        logger.debug("[slicer] ffprobe keyframes stderr: %s", (r.stderr or "")[:300])
        return []
    out: List[float] = []
    for line in r.stdout.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(",")
        if len(parts) < 2:
            continue
        try:
            pts = float(parts[0])
        except ValueError:
            continue
        pict = parts[1].strip().upper()
        if pict == "I" or pict == "1":
            out.append(pts)
    out.sort()
    return out


def filter_cut_points(
    points: Sequence[float],
    duration: float,
    min_gap: float,
    decimals: int,
) -> List[float]:
    """
    Уникальные точки в (0, duration), с минимальным зазором min_gap между соседними
    (чтобы не резать «впритык» и не получать сегменты короче CLIP_MIN_LEN).
    """
    if duration <= 0:
        return []
    d = max(0, decimals)
    raw = sorted({round(float(p), d) for p in points if 0 < float(p) < duration})
    if not raw:
        return []
    merged: List[float] = []
    prev = 0.0
    for p in raw:
        if p - prev < min_gap:
            continue
        if duration - p < min_gap:
            break
        merged.append(p)
        prev = p
    return merged


def postprocess_cut_times(
    cuts: Sequence[float],
    video_path: Path,
    duration: float,
) -> List[float]:
    """
    Округление → фильтр по CLIP_MIN_LEN → опционально снап к I-frame.
    """
    decimals = max(0, int(getattr(config, "SLICER_ROUND_DECIMALS", 1)))
    min_len = float(getattr(config, "CLIP_MIN_LEN", 15.0))
    # зазор между точками реза — доля от мин. длины клипа (не режем чаще чем раз в ~пол-клипа)
    min_gap = max(3.0, min_len * 0.45)

    pts = round_times(list(cuts), decimals)
    pts = filter_cut_points(pts, duration, min_gap=min_gap, decimals=decimals)

    if getattr(config, "SLICER_KEYFRAME_SNAP", True) and pts:
        kf = probe_iframe_times(video_path, duration)
        if kf:
            delta = float(getattr(config, "SLICER_KEYFRAME_MAX_DELTA_SEC", 0.5))
            pts = [
                round(_snap_one(p, kf, delta), decimals)
                for p in pts
            ]
            pts = filter_cut_points(pts, duration, min_gap=min_gap, decimals=decimals)

    logger.info(
        "[slicer] точки после постобработки: %s",
        ", ".join(f"{c:.{decimals}f}" for c in pts) if pts else "нет",
    )
    return pts


def silence_intervals_to_midpoints(
    intervals: Sequence[Tuple[float, float]],
    duration: float,
) -> List[float]:
    """Середины интервалов тишины — кандидаты на рез."""
    mids: List[float] = []
    for s, e in intervals:
        if e <= s or s >= duration:
            continue
        e = min(e, duration)
        s = max(0.0, s)
        m = (s + e) / 2.0
        if 0 < m < duration:
            mids.append(m)
    return sorted(set(mids))


def distance_to_nearest_silence_feature(
    t: float,
    silence_intervals: Sequence[Tuple[float, float]],
) -> float:
    """Минимальное расстояние до середины/края интервала тишины; 0 если t внутри паузы."""
    if not silence_intervals:
        return 1e9
    best = 1e9
    for s, e in silence_intervals:
        if e <= s:
            continue
        if s <= t <= e:
            return 0.0
        mid = (s + e) / 2.0
        for x in (s, e, mid):
            best = min(best, abs(t - x))
    return best


def is_cut_aligned_with_silence(
    t: float,
    silence_intervals: Sequence[Tuple[float, float]],
    proximity_sec: float,
) -> bool:
    """Рез согласован с тишиной: внутри паузы или близко к её признакам."""
    if not silence_intervals:
        return False
    d = distance_to_nearest_silence_feature(t, silence_intervals)
    return d <= proximity_sec


def rank_disputed_cuts_for_refinement(
    cuts: Sequence[float],
    silence_intervals: Sequence[Tuple[float, float]],
    proximity_sec: float,
) -> List[float]:
    """
    Границы, которые далеко от тишины, по убыванию «плохости» (сначала самые спорные).
    Без интервалов тишины — пусто (нечему подстраиваться).
    """
    if not silence_intervals or not cuts:
        return []
    ranked: List[Tuple[float, float]] = []
    for t in cuts:
        if is_cut_aligned_with_silence(t, silence_intervals, proximity_sec):
            continue
        d = distance_to_nearest_silence_feature(t, silence_intervals)
        ranked.append((t, d))
    ranked.sort(key=lambda x: -x[1])
    return [t for t, _ in ranked]


def coarse_cuts_heuristic(
    duration: float,
    silence_intervals: Sequence[Tuple[float, float]],
) -> List[float]:
    """
    Грубые границы: середины пауз; если пауз мало — равномерные точки по CLIP_MAX_LEN.
    """
    if duration <= 0:
        return []

    lo = float(getattr(config, "CLIP_MIN_LEN", 15.0))
    hi = float(getattr(config, "CLIP_MAX_LEN", 35.0))
    decimals = max(0, int(getattr(config, "SLICER_ROUND_DECIMALS", 1)))

    hints = silence_intervals_to_midpoints(silence_intervals, duration)

    if not hints and duration > hi:
        t = hi
        while t < duration - lo:
            hints.append(min(t, duration - lo))
            t += hi

    hints = round_times(hints, decimals)
    return filter_cut_points(
        hints, duration, min_gap=max(3.0, lo * 0.45), decimals=decimals
    )
