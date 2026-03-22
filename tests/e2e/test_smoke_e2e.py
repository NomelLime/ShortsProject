"""
E2E / интеграционные смоки без моков внешних API (опционально).

Запуск:
  set RUN_E2E=1
  pytest tests/e2e -m e2e -v

Проверяет наличие ffmpeg и минимальный прогон утилит пайплайна на диске.
Не трогает соцсети и браузер.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.e2e


def _e2e_skip():
    if os.getenv("RUN_E2E", "").strip() != "1":
        pytest.skip("Установите RUN_E2E=1 для e2e-смоков")


def test_ffmpeg_on_path():
    _e2e_skip()
    assert shutil.which("ffmpeg"), "ffmpeg должен быть в PATH"


def test_ffprobe_version():
    _e2e_skip()
    kw = {}
    if sys.platform == "win32":
        kw["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    r = subprocess.run(
        ["ffprobe", "-version"],
        capture_output=True,
        text=True,
        timeout=10,
        **kw,
    )
    assert r.returncode == 0
    assert "ffprobe" in (r.stdout or "").lower()


def test_pipeline_utils_load_keywords_from_example():
    _e2e_skip()
    root = Path(__file__).resolve().parents[2]
    example = root / "examples" / "keywords.example.txt"
    if not example.is_file():
        pytest.skip("examples/keywords.example.txt отсутствует")

    from pipeline import config
    from pipeline.utils import load_keywords

    orig = config.KEYWORDS_FILE
    try:
        config.KEYWORDS_FILE = example
        kws = load_keywords()
    finally:
        config.KEYWORDS_FILE = orig

    assert len(kws) >= 1
    assert all(isinstance(x, str) and x for x in kws)
