"""Ротация аккаунта пайплайна (LRU) для SCOUT."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipeline import pipeline_account_rotation as par
from pipeline import utils


def _mk_account(root: Path, name: str) -> None:
    d = root / name
    d.mkdir(parents=True)
    (d / "config.json").write_text("{}", encoding="utf-8")


def test_pick_lru_prefers_never_used(tmp_path, monkeypatch):
    monkeypatch.setattr(par, "_accounts_root", lambda: tmp_path)
    _mk_account(tmp_path, "a")
    _mk_account(tmp_path, "b")
    mem = MagicMock()
    mem.get = MagicMock(
        return_value={"a": "2026-01-15T12:00:00+00:00", "b": "2026-01-10T12:00:00+00:00"}
    )
    # Оба использованы; b старее → LRU = b
    assert par.pick_lru_pipeline_account(mem) == "b"

    mem.get = MagicMock(return_value={"a": "2026-01-15T12:00:00+00:00"})
    # b нет в карте → считается самым старым
    assert par.pick_lru_pipeline_account(mem) == "b"


def test_touch_updates_kv():
    mem = MagicMock()
    mem.get = MagicMock(return_value={"x": "2020-01-01T00:00:00+00:00"})
    par.touch_pipeline_account(mem, "y")
    assert mem.set.called
    args = mem.set.call_args[0]
    assert args[0] == par.KV_LAST_USED
    assert "y" in args[1]


def test_scout_context_binds_resolve(tmp_path, monkeypatch):
    monkeypatch.delenv("SHORTS_PIPELINE_ACCOUNT", raising=False)
    monkeypatch.delenv("YTDLP_COOKIES_ACCOUNT", raising=False)
    monkeypatch.setenv("PIPELINE_ACCOUNT_ROTATION", "1")
    monkeypatch.setattr(par, "_accounts_root", lambda: tmp_path)
    monkeypatch.setattr(utils.config, "ACCOUNTS_ROOT", str(tmp_path))
    _mk_account(tmp_path, "acc_rot")
    mem = MagicMock()
    mem.get = MagicMock(return_value={})

    assert utils.resolve_pipeline_account_name() is None
    with par.scout_pipeline_cycle_account(mem):
        assert utils.resolve_pipeline_account_name() == "acc_rot"
    assert utils.resolve_pipeline_account_name() is None
    assert mem.set.called


def test_trend_scout_cycle_touches_lru(tmp_path, monkeypatch):
    """TREND_SCOUT оборачивает сбор в scout_pipeline_cycle_account (как SCOUT)."""
    from pipeline.agents.trend_scout import TrendScout

    monkeypatch.delenv("SHORTS_PIPELINE_ACCOUNT", raising=False)
    monkeypatch.delenv("YTDLP_COOKIES_ACCOUNT", raising=False)
    monkeypatch.setenv("PIPELINE_ACCOUNT_ROTATION", "1")
    monkeypatch.setattr(par, "_accounts_root", lambda: tmp_path)
    _mk_account(tmp_path, "acc_ts")
    mem = MagicMock()
    mem.get = MagicMock(return_value={})

    ts = TrendScout(memory=mem)
    ts._fetch_sources = MagicMock()  # без сети

    ts._collect_trends()
    assert mem.set.called


def test_pinned_env_ignores_rotation(tmp_path, monkeypatch):
    monkeypatch.setenv("SHORTS_PIPELINE_ACCOUNT", "acc_pin")
    monkeypatch.setenv("PIPELINE_ACCOUNT_ROTATION", "1")
    monkeypatch.setattr(utils.config, "ACCOUNTS_ROOT", str(tmp_path))
    _mk_account(tmp_path, "acc_pin")
    _mk_account(tmp_path, "other")
    mem = MagicMock()
    mem.get = MagicMock(return_value={})

    with par.scout_pipeline_cycle_account(mem):
        assert utils.resolve_pipeline_account_name() == "acc_pin"
    # Не должны писать LRU при зафиксированном env
    mem.set.assert_not_called()
