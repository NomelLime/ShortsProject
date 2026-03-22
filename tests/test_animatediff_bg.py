"""Тесты pipeline/animatediff_bg.py."""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest


def test_slug_topic():
    from pipeline.animatediff_bg import _slug_topic

    assert _slug_topic("Hello World!!!") == "Hello World"
    assert _slug_topic("   ") == "bg"


def test_pick_image_for_topic(tmp_path: Path):
    from pipeline.animatediff_bg import _pick_image_for_topic

    (tmp_path / "a.jpg").write_bytes(b"\xff\xd8\xff")
    (tmp_path / "cooking_recipe.png").write_bytes(b"\x89PNG")
    p = _pick_image_for_topic(tmp_path, "cooking tips")
    assert p is not None
    assert "cooking" in p.stem.lower()


def test_list_bg_images():
    from pipeline.animatediff_bg import _list_bg_images

    assert _list_bg_images(Path("/nonexistent")) == []


def test_generate_returns_none_when_disabled(monkeypatch):
    from pipeline import config as cfg

    monkeypatch.setattr(cfg, "ANIMATEDIFF_ENABLED", False)
    from pipeline.animatediff_bg import generate_motion_background

    assert generate_motion_background("topic") is None


def test_generate_script_uses_acquire_factory(monkeypatch, tmp_path: Path):
    """Внешний скрипт выполняется внутри контекста acquire_script_gpu."""
    from pipeline import animatediff_bg
    from pipeline import config as cfg

    entered: list[bool] = []

    @contextmanager
    def fake_acquire():
        entered.append(True)
        yield

    monkeypatch.setattr(cfg, "ANIMATEDIFF_ENABLED", True)
    monkeypatch.setattr(cfg, "ANIMATEDIFF_SCRIPT", "dummy_gen.sh")
    monkeypatch.setattr(cfg, "ANIMATEDIFF_FF_FALLBACK", False)

    def _fake_run_external(script: str, topic: str, out_path: Path) -> bool:
        assert script == "dummy_gen.sh"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"fake")
        return True

    monkeypatch.setattr(animatediff_bg, "_run_external_script", _fake_run_external)

    from pipeline.animatediff_bg import generate_motion_background

    out = generate_motion_background("my topic", acquire_script_gpu=fake_acquire)
    assert entered == [True]
    assert out is not None and out.exists()


def test_generate_ken_burns_writes_output(monkeypatch, tmp_path: Path):
    import subprocess

    from pipeline import config as cfg

    monkeypatch.setattr(cfg, "ANIMATEDIFF_ENABLED", True)
    monkeypatch.setattr(cfg, "ANIMATEDIFF_SCRIPT", "")
    monkeypatch.setattr(cfg, "ANIMATEDIFF_FF_FALLBACK", True)
    monkeypatch.setattr(cfg, "ANIMATEDIFF_DURATION_SEC", 1)
    monkeypatch.setattr(cfg, "ANIMATEDIFF_SIZE", "320:240")
    monkeypatch.setattr(cfg, "BASE_DIR", tmp_path)

    bg = tmp_path / "assets" / "backgrounds"
    bg.mkdir(parents=True)
    (bg / "test.jpg").write_bytes(b"\xff\xd8\xff")

    def _fake_run(cmd, capture_output, text, timeout):
        outp = Path(cmd[-1])
        outp.parent.mkdir(parents=True, exist_ok=True)
        outp.write_bytes(b"fakevideo")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    from pipeline.animatediff_bg import generate_motion_background

    with patch("pipeline.animatediff_bg.shutil.which", return_value="ffmpeg"), patch(
        "pipeline.animatediff_bg.subprocess.run", side_effect=_fake_run
    ):
        out = generate_motion_background("test topic")

    assert out is not None
    assert out.exists() and out.stat().st_size > 0
