"""Тесты pipeline/slicer_cut_utils.py."""
from __future__ import annotations

from pipeline.slicer_cut_utils import (
    coarse_cuts_heuristic,
    distance_to_nearest_silence_feature,
    filter_cut_points,
    is_cut_aligned_with_silence,
    normalize_best_segment,
    rank_disputed_cuts_for_refinement,
    round_times,
)


def test_normalize_best_segment_clamps():
    assert normalize_best_segment(None, 100.0) is None
    assert normalize_best_segment(-1, 10.0) == 0.0
    # слишком поздно начать — уводим назад
    bs = normalize_best_segment(95.0, 100.0, seg_min_len=3.0)
    assert bs is not None and bs + 3.0 <= 100.0 + 0.1


def test_filter_cut_points_min_gap():
    pts = filter_cut_points([5.0, 5.05, 20.0, 90.0], duration=100.0, min_gap=6.0, decimals=1)
    assert 5.05 not in pts
    assert len(pts) >= 1


def test_round_times():
    assert round_times([1.234, 2.567], 1) == [1.2, 2.6]


def test_coarse_cuts_heuristic_uniform():
    # без тишины — равномерные точки по CLIP_MAX_LEN
    h = coarse_cuts_heuristic(120.0, [])
    assert h
    assert all(0 < x < 120 for x in h)


def test_distance_and_disputed():
    intervals = [(10.0, 12.0), (40.0, 43.0)]
    assert distance_to_nearest_silence_feature(11.0, intervals) == 0.0
    assert is_cut_aligned_with_silence(11.0, intervals, 1.2) is True
    assert is_cut_aligned_with_silence(25.0, intervals, 1.2) is False
    ranked = rank_disputed_cuts_for_refinement([11.0, 25.0, 50.0], intervals, 1.2)
    assert 11.0 not in ranked
    assert 25.0 in ranked or 50.0 in ranked
