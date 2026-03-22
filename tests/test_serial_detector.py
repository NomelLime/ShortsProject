"""Юнит-тесты pipeline/serial_detector.py."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def test_collect_records_min_views():
    from pipeline.serial_detector import _collect_records

    data = {
        "v1": {
            "title": "t1",
            "tags": ["a", "b"],
            "uploads": {
                "youtube": {"views": 100, "likes": 10, "comments": 5},
            },
        },
        "v2": {
            "uploads": {"youtube": {"views": 2000, "likes": 200, "comments": 100}},
        },
    }
    rec = _collect_records(data, min_views=500)
    assert len(rec) == 1
    assert rec[0]["stem"] == "v2"
    assert rec[0]["total_views"] == 2000
    assert abs(rec[0]["engagement_rate"] - 300 / 2000) < 1e-6


def test_collect_records_skips_non_dict():
    from pipeline.serial_detector import _collect_records

    assert _collect_records({"x": "bad"}, 100) == []


@pytest.fixture
def analytics_dense():
    """40 видео с растущим engagement — хватает для SERIAL_MIN_HISTORY=30."""
    d = {}
    for i in range(40):
        v = 500 + i * 50
        likes = int(v * (0.02 + i * 0.002))
        d[f"stem{i}"] = {
            "title": f"T{i}",
            "tags": ["tagx", f"t{i % 5}"],
            "uploads": {"youtube": {"views": v, "likes": likes, "comments": 1}},
        }
    return d


def test_detect_serial_candidates_respects_disabled():
    from pipeline import config as cfg
    from pipeline.serial_detector import detect_serial_candidates

    if not cfg.SERIAL_ENABLED:
        assert detect_serial_candidates(force=False) == []


def test_detect_serial_candidates_force(analytics_dense):
    mem = MagicMock()
    with patch("pipeline.analytics._load_analytics", return_value=analytics_dense), patch(
        "pipeline.agent_memory.get_memory", return_value=mem
    ):
        from pipeline.serial_detector import detect_serial_candidates

        out = detect_serial_candidates(force=True)
    assert isinstance(out, list)
    mem.set.assert_called()
    assert mem.set.call_args_list[0][0][0] == "serial_candidates"


def test_find_serial_parent():
    from pipeline.serial_detector import find_serial_parent

    with patch("pipeline.serial_detector.get_serial_candidates") as g:
        g.return_value = [
            {"stem": "a", "tags": ["fitness", "gym"]},
            {"stem": "b", "tags": ["cooking"]},
        ]
        p = find_serial_parent(["Fitness", "other"], stem_exclude="b")
        assert p is not None
        assert p["stem"] == "a"


def test_find_serial_parent_no_overlap():
    from pipeline.serial_detector import find_serial_parent

    with patch("pipeline.serial_detector.get_serial_candidates") as g:
        g.return_value = [{"stem": "a", "tags": ["x"]}]
        assert find_serial_parent(["y", "z"]) is None


def test_make_serial_hook():
    from pipeline.serial_detector import make_serial_hook

    assert "Часть 2:" in make_serial_hook({"title": "P"}, "hook")
    assert "Часть 2:" in make_serial_hook({"title": "Long title here"}, "")
