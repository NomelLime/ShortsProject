"""Тесты прогрева заливки (upload_warmup), без полного импорта pipeline."""

from __future__ import annotations

import importlib.util
import json
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from unittest.mock import MagicMock


def _load_upload_warmup_with_config(cfg: types.SimpleNamespace):
    """Загружает pipeline/upload_warmup.py с подставным pipeline.config."""
    root = Path(__file__).resolve().parents[1]
    path = root / "pipeline" / "upload_warmup.py"

    # Без __path__ — иначе Python подтянет настоящий pipeline/__init__.py с тяжёлыми зависимостями.
    pkg = types.ModuleType("pipeline")
    cfg_mod = types.ModuleType("pipeline.config")
    for k, v in vars(cfg).items():
        if not k.startswith("_"):
            setattr(cfg_mod, k, v)

    sys.modules["pipeline"] = pkg
    sys.modules["pipeline.config"] = cfg_mod

    spec = importlib.util.spec_from_file_location("pipeline.upload_warmup", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["pipeline.upload_warmup"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def acc_setup(tmp_path):
    acc_dir = tmp_path / "acc1"
    acc_dir.mkdir()
    (acc_dir / "config.json").write_text("{}", encoding="utf-8")
    return acc_dir


def test_ensure_warmup_started_once(acc_setup):
    cfg = types.SimpleNamespace(
        UPLOAD_WARMUP_ENABLED=True,
        UPLOAD_WARMUP_MIN_DAYS=3,
        UPLOAD_WARMUP_MAX_DAYS=3,
    )
    uw = _load_upload_warmup_with_config(cfg)

    uw.ensure_warmup_started(acc_setup, "youtube", {})
    uw.ensure_warmup_started(acc_setup, "youtube", {})
    data = json.loads((acc_setup / uw.WARMUP_FILENAME).read_text(encoding="utf-8"))
    assert data["platforms"]["youtube"]["warmup_days"] == 3
    assert "upload_allowed_after" in data["platforms"]["youtube"]


def test_skip_upload_warmup_config(acc_setup):
    cfg = types.SimpleNamespace(
        UPLOAD_WARMUP_ENABLED=True,
        UPLOAD_WARMUP_MIN_DAYS=3,
        UPLOAD_WARMUP_MAX_DAYS=5,
    )
    uw = _load_upload_warmup_with_config(cfg)

    uw.ensure_warmup_started(acc_setup, "tiktok", {"skip_upload_warmup": True})
    assert not (acc_setup / uw.WARMUP_FILENAME).exists()


def test_is_upload_warmup_active_future(acc_setup):
    cfg = types.SimpleNamespace(UPLOAD_WARMUP_ENABLED=True)
    uw = _load_upload_warmup_with_config(cfg)

    until = (datetime.now(timezone.utc) + timedelta(days=2)).isoformat(timespec="seconds")
    (acc_setup / uw.WARMUP_FILENAME).write_text(
        json.dumps({"platforms": {"youtube": {"upload_allowed_after": until}}}),
        encoding="utf-8",
    )
    active, msg = uw.is_upload_warmup_active(acc_setup, "youtube", {})
    assert active is True
    assert "прогрев" in msg


def test_is_upload_warmup_active_expired(acc_setup):
    cfg = types.SimpleNamespace(UPLOAD_WARMUP_ENABLED=True)
    uw = _load_upload_warmup_with_config(cfg)

    until = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(timespec="seconds")
    (acc_setup / uw.WARMUP_FILENAME).write_text(
        json.dumps({"platforms": {"youtube": {"upload_allowed_after": until}}}),
        encoding="utf-8",
    )
    active, _ = uw.is_upload_warmup_active(acc_setup, "youtube", {})
    assert active is False


def test_is_upload_blocked_resolves_account(tmp_path):
    acc_dir = tmp_path / "acc1"
    acc_dir.mkdir()
    (acc_dir / "config.json").write_text("{}", encoding="utf-8")

    cfg = types.SimpleNamespace(
        ACCOUNTS_ROOT=str(tmp_path),
        UPLOAD_WARMUP_ENABLED=True,
    )
    uw = _load_upload_warmup_with_config(cfg)

    until = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(timespec="seconds")
    (acc_dir / uw.WARMUP_FILENAME).write_text(
        json.dumps({"platforms": {"youtube": {"upload_allowed_after": until}}}),
        encoding="utf-8",
    )
    blocked, _ = uw.is_upload_blocked("acc1", "youtube")
    assert blocked is True


def test_tracking_stem_ready_for_archive():
    cfg = types.SimpleNamespace(UPLOAD_WARMUP_ENABLED=True)
    uw = _load_upload_warmup_with_config(cfg)
    uw.all_accounts_warmup_for_platform = MagicMock(return_value=True)
    assert not uw.tracking_stem_ready_for_archive(
        {"youtube": False, "tiktok": False},
        {"youtube", "tiktok"},
    )
    assert uw.tracking_stem_ready_for_archive(
        {"youtube": True, "tiktok": False},
        {"youtube", "tiktok"},
    )


def test_all_accounts_warmup_true_when_all_accounts_in_warmup(tmp_path):
    future = (datetime.now(timezone.utc) + timedelta(days=4)).isoformat(timespec="seconds")
    for name in ("acc_a", "acc_b"):
        d = tmp_path / name
        d.mkdir()
        (d / "config.json").write_text(
            json.dumps({"platforms": ["youtube"]}),
            encoding="utf-8",
        )
    cfg = types.SimpleNamespace(
        ACCOUNTS_ROOT=str(tmp_path),
        UPLOAD_WARMUP_ENABLED=True,
    )
    uw = _load_upload_warmup_with_config(cfg)
    for name in ("acc_a", "acc_b"):
        (tmp_path / name / uw.WARMUP_FILENAME).write_text(
            json.dumps({"platforms": {"youtube": {"upload_allowed_after": future}}}),
            encoding="utf-8",
        )
    assert uw.all_accounts_warmup_for_platform("youtube") is True


def test_ensure_warmup_account_scope_writes_all_platforms(tmp_path):
    acc_dir = tmp_path / "acc_m"
    acc_dir.mkdir()
    (acc_dir / "config.json").write_text(
        json.dumps({"platforms": ["youtube", "tiktok"]}),
        encoding="utf-8",
    )
    cfg = types.SimpleNamespace(
        UPLOAD_WARMUP_ENABLED=True,
        UPLOAD_WARMUP_MIN_DAYS=4,
        UPLOAD_WARMUP_MAX_DAYS=4,
        UPLOAD_WARMUP_DEFAULT_SCOPE="account",
    )
    uw = _load_upload_warmup_with_config(cfg)
    uw.ensure_warmup_started(acc_dir, "youtube", {"platforms": ["youtube", "tiktok"]})
    data = json.loads((acc_dir / uw.WARMUP_FILENAME).read_text(encoding="utf-8"))
    assert "youtube" in data["platforms"]
    assert "tiktok" in data["platforms"]
    assert data["platforms"]["youtube"]["upload_allowed_after"] == data["platforms"]["tiktok"][
        "upload_allowed_after"
    ]


def test_all_accounts_warmup_false_when_one_account_ready(tmp_path):
    future = (datetime.now(timezone.utc) + timedelta(days=4)).isoformat(timespec="seconds")
    (tmp_path / "acc_w").mkdir()
    (tmp_path / "acc_ok").mkdir()
    (tmp_path / "acc_w" / "config.json").write_text(
        json.dumps({"platforms": ["youtube"]}),
        encoding="utf-8",
    )
    (tmp_path / "acc_ok" / "config.json").write_text(
        json.dumps({"platforms": ["youtube"], "skip_upload_warmup": True}),
        encoding="utf-8",
    )
    cfg = types.SimpleNamespace(
        ACCOUNTS_ROOT=str(tmp_path),
        UPLOAD_WARMUP_ENABLED=True,
    )
    uw = _load_upload_warmup_with_config(cfg)
    (tmp_path / "acc_w" / uw.WARMUP_FILENAME).write_text(
        json.dumps({"platforms": {"youtube": {"upload_allowed_after": future}}}),
        encoding="utf-8",
    )
    assert uw.all_accounts_warmup_for_platform("youtube") is False
