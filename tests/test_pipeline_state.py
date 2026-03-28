"""Тесты pipeline_state: reset, save, get_next_stage."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def state_file(tmp_path, monkeypatch):
    import pipeline.pipeline_state as ps

    p = tmp_path / "pipeline_state.json"
    monkeypatch.setattr(ps, "STATE_FILE", p)
    return p


def test_reset_and_next(state_file):
    from pipeline import pipeline_state as ps

    ps.reset_state()
    assert ps.get_next_stage() == "search"
    ps.save_stage_result("search", True)
    assert ps.get_next_stage() == "download"
    ps.save_stage_result("download", True)
    assert ps.is_stage_done("search")


def test_load_roundtrip(state_file):
    from pipeline import pipeline_state as ps

    ps.reset_state()
    ps.save_stage_result("search", False, {"err": "x"})
    data = json.loads(state_file.read_text(encoding="utf-8"))
    assert data["stages"]["search"]["status"] == "failed"
