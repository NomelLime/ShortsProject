"""Юнит-тесты pipeline/niche.py (без реального VL/Ollama)."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def test_extract_words_from_cache_list(tmp_path: Path):
    from pipeline import niche

    p = tmp_path / "cache.json"
    p.write_text(
        json.dumps(
            [{"title": "Amazing Fitness Workout", "hashtags": ["#gym", "#health"]}],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    words = niche._extract_words_from_cache(p)
    assert "amazing" in words
    assert "fitness" in words
    assert "workout" in words


def test_extract_words_from_cache_invalid(tmp_path: Path):
    from pipeline import niche

    p = tmp_path / "bad.json"
    p.write_text("not json", encoding="utf-8")
    assert niche._extract_words_from_cache(p) == []


def test_niche_from_upload_queue_none(tmp_path: Path):
    from pipeline import niche

    assert niche._niche_from_upload_queue(tmp_path) is None


def test_niche_from_upload_queue_b(tmp_path: Path):
    from pipeline import niche

    acc = tmp_path / "acc"
    q = acc / "upload_queue" / "youtube"
    q.mkdir(parents=True)
    (q / "x.ai_cache.json").write_text(
        json.dumps([{"title": "cooking pasta recipes", "hashtags": ["food"]}]),
        encoding="utf-8",
    )
    n = niche._niche_from_upload_queue(acc)
    assert n is not None
    assert "cooking" in n or "pasta" in n or "recipes" in n


def test_detect_and_cache_niche_existing():
    from pipeline import niche

    account = {
        "name": "t1",
        "dir": "/tmp",
        "config": {"niche": "manual niche"},
    }
    assert niche.detect_and_cache_niche(account) == "manual niche"


def test_detect_and_cache_niche_topic_key(tmp_path: Path):
    from pipeline import niche

    acc_dir = tmp_path / "acc"
    acc_dir.mkdir()
    cfg_path = acc_dir / "config.json"
    cfg_path.write_text(json.dumps({"platforms": ["youtube"]}), encoding="utf-8")
    account = {"name": "a", "dir": str(acc_dir), "config": {"topic": "crypto news"}}
    assert niche.detect_and_cache_niche(account) == "crypto news"


def test_detect_and_cache_niche_fallback_general(tmp_path: Path):
    from pipeline import niche

    acc_dir = tmp_path / "acc2"
    acc_dir.mkdir()
    cfg_path = acc_dir / "config.json"
    cfg_path.write_text(json.dumps({"platforms": ["youtube"]}), encoding="utf-8")
    account = {"name": "a", "dir": str(acc_dir), "config": {}}
    with patch.object(niche, "_niche_from_upload_queue", return_value=None), patch.object(
        niche, "_niche_from_video_vl", return_value=None
    ):
        n = niche.detect_and_cache_niche(account)
    assert n == "general content"
