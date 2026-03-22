"""Юнит-тесты pipeline/scheduler.py (без реального браузера)."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch


def test_in_activity_window_inside(monkeypatch):
    import pipeline.scheduler as sched

    class _DT:
        @staticmethod
        def now():
            return datetime(2026, 3, 22, 12, 0, 0)

    monkeypatch.setattr(sched, "datetime", _DT)
    monkeypatch.setattr(sched.config, "ACTIVITY_HOURS_START", 8)
    monkeypatch.setattr(sched.config, "ACTIVITY_HOURS_END", 23)
    assert sched._in_activity_window() is True


def test_in_activity_window_outside(monkeypatch):
    import pipeline.scheduler as sched

    class _DT:
        @staticmethod
        def now():
            return datetime(2026, 3, 22, 3, 0, 0)

    monkeypatch.setattr(sched, "datetime", _DT)
    monkeypatch.setattr(sched.config, "ACTIVITY_HOURS_START", 8)
    monkeypatch.setattr(sched.config, "ACTIVITY_HOURS_END", 23)
    assert sched._in_activity_window() is False


def test_account_activity_job_next_delay(tmp_path):
    from pipeline.scheduler import _AccountActivityJob

    acc = {"name": "a", "dir": str(tmp_path), "config": {}}
    job = _AccountActivityJob(acc, "youtube", interval_sec=3600, jitter_sec=0)
    d = job._next_delay()
    assert d >= 60.0


def test_activity_scheduler_disabled(monkeypatch):
    from pipeline.scheduler import ActivityScheduler

    monkeypatch.setattr("pipeline.scheduler.config.ACTIVITY_SCHEDULER_ENABLED", False)
    s = ActivityScheduler()
    s.start()
    assert s._started is False
    assert s._jobs == []


def test_activity_scheduler_start_stop(monkeypatch, tmp_path):
    from pipeline.scheduler import ActivityScheduler, _AccountActivityJob

    monkeypatch.setattr("pipeline.scheduler.config.ACTIVITY_SCHEDULER_ENABLED", True)
    monkeypatch.setattr("pipeline.scheduler.config.ACTIVITY_SCHEDULER_INTERVAL_MIN", 60)
    monkeypatch.setattr("pipeline.scheduler.config.ACTIVITY_SCHEDULER_JITTER_SEC", 0)

    fake_acc = {
        "name": "acc1",
        "dir": str(tmp_path),
        "platforms": ["youtube"],
        "config": {},
    }
    with patch("pipeline.scheduler.utils.get_all_accounts", return_value=[fake_acc]), patch.object(
        _AccountActivityJob, "start", MagicMock()
    ):
        s = ActivityScheduler()
        s.start()
        assert s._started is True
        assert len(s._jobs) == 1
        s.stop()
        assert s._started is False
        assert s._jobs == []


def test_activity_scheduler_no_accounts(monkeypatch):
    from pipeline.scheduler import ActivityScheduler

    monkeypatch.setattr("pipeline.scheduler.config.ACTIVITY_SCHEDULER_ENABLED", True)
    with patch("pipeline.scheduler.utils.get_all_accounts", return_value=[]):
        s = ActivityScheduler()
        s.start()
        assert s._jobs == []
