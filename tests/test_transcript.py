"""
tests/test_transcript.py — Тесты модуля транскрипции (Сессия 11, ФИЧА 2).
"""
from __future__ import annotations
import importlib.util, sys, subprocess
from pathlib import Path
import pytest

_ROOT = Path(__file__).parent.parent

def _load(name: str):
    p = _ROOT / "pipeline" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"pipeline.{name}", p)
    m = importlib.util.module_from_spec(spec)
    sys.modules[f"pipeline.{name}"] = m
    spec.loader.exec_module(m)
    return m


class TestTranscriptCache:
    def test_returns_from_cache(self, tmp_path):
        """Если кеш существует — возвращает из него без ffmpeg/whisper."""
        tr = _load("transcript")
        video = tmp_path / "vid.mp4"; video.touch()
        (tmp_path / "vid.transcript_cache.txt").write_text("cached text", encoding="utf-8")
        assert tr.transcribe_for_metadata(video) == "cached text"

    def test_cache_truncated_to_500(self, tmp_path):
        tr = _load("transcript")
        video = tmp_path / "long.mp4"; video.touch()
        (tmp_path / "long.transcript_cache.txt").write_text("A" * 600, encoding="utf-8")
        result = tr.transcribe_for_metadata(video)
        assert len(result) == 500

    def test_cache_stripped(self, tmp_path):
        tr = _load("transcript")
        video = tmp_path / "ws.mp4"; video.touch()
        (tmp_path / "ws.transcript_cache.txt").write_text("  hello  \n", encoding="utf-8")
        assert tr.transcribe_for_metadata(video) == "hello"


class TestTranscriptGraceful:
    def test_nonexistent_video_returns_empty(self, tmp_path):
        """Нет видео, нет кеша → пустая строка."""
        tr = _load("transcript")
        assert tr.transcribe_for_metadata(tmp_path / "missing.mp4") == ""

    def test_ffmpeg_failure_returns_empty(self, tmp_path, monkeypatch):
        """Если ffmpeg не справился → пустая строка."""
        tr = _load("transcript")
        video = tmp_path / "fail.mp4"; video.touch()

        class _R:
            returncode = 1; stderr = b""
        monkeypatch.setattr(subprocess, "run", lambda *a, **kw: _R())

        assert tr.transcribe_for_metadata(video) == ""

    def test_whisper_not_installed_returns_empty(self, tmp_path, monkeypatch):
        """Если faster-whisper не установлен → пустая строка."""
        tr = _load("transcript")
        video = tmp_path / "nofw.mp4"; video.touch()

        class _R:
            returncode = 0; stderr = b""
        monkeypatch.setattr(subprocess, "run", lambda *a, **kw: _R())

        # Убираем faster_whisper из sys.modules
        monkeypatch.setitem(sys.modules, "faster_whisper", None)

        assert tr.transcribe_for_metadata(video) == ""

    def test_whisper_mock_writes_cache(self, tmp_path, monkeypatch):
        """Успешная транскрипция пишет кеш на диск."""
        tr = _load("transcript")
        video = tmp_path / "ok.mp4"; video.touch()
        cache = tmp_path / "ok.transcript_cache.txt"

        class _R:
            returncode = 0; stderr = b""
        monkeypatch.setattr(subprocess, "run", lambda *a, **kw: _R())

        class _Seg:
            text = "мок транскрипт"
        class _Model:
            def transcribe(self, *a, **kw): return [_Seg()], None

        mock_fw = type(sys)("faster_whisper")
        mock_fw.WhisperModel = lambda *a, **kw: _Model()
        monkeypatch.setitem(sys.modules, "faster_whisper", mock_fw)

        result = tr.transcribe_for_metadata(video)
        assert result == "мок транскрипт"
        assert cache.exists()
        assert cache.read_text(encoding="utf-8") == "мок транскрипт"
