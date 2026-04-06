"""Тесты реестра exit-IP (моки сети и ротации)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline import proxy_ip_registry as pir
from pipeline import config


@pytest.fixture
def reg_path(tmp_path, monkeypatch):
    p = tmp_path / "proxy_ip_registry.json"
    lock = tmp_path / "proxy_ip_rotation.lock"
    monkeypatch.setattr(config, "PROXY_IP_REGISTRY_FILE", p)
    monkeypatch.setattr(config, "PROXY_IP_ROTATION_LOCK_FILE", lock)
    return p


@pytest.fixture
def proxy_cfg():
    return {"host": "p.example.com", "port": 8080, "username": "", "password": ""}


def test_account_id_from(tmp_path):
    d = tmp_path / "myacc"
    d.mkdir()
    assert pir.account_id_from(d, {"account_id": "x"}) == "x"
    assert pir.account_id_from(d, {"name": "n"}) == "n"
    assert pir.account_id_from(d, {}) == "myacc"


def test_is_ip_clean(reg_path):
    reg = {
        "accounts": {
            "a1": {"ip": "1.1.1.1"},
            "a2": {"ip": "2.2.2.2"},
        }
    }
    reg_path.write_text(json.dumps(reg), encoding="utf-8")
    assert pir._is_ip_clean_for_account("9.9.9.9", "a1", reg) is True
    assert pir._is_ip_clean_for_account("1.1.1.1", "a1", reg) is True
    assert pir._is_ip_clean_for_account("1.1.1.1", "a3", reg) is False


def test_first_clean_ip_assigned(reg_path, proxy_cfg, monkeypatch):
    monkeypatch.setattr(config, "SHORTS_PROXY_IP_REGISTRY", True)
    monkeypatch.setattr(config, "MOBILEPROXY_CHANGE_IP_URL", "https://change.test/rot")
    monkeypatch.setattr(
        "pipeline.proxy_ip_registry.mobileproxy_geo_enabled", lambda *a, **k: False
    )
    monkeypatch.setattr(config, "PROXY_IP_MAX_ROTATIONS", 20)
    monkeypatch.setattr(config, "PROXY_IP_MAX_STICKY_ATTEMPTS", 5)
    monkeypatch.setattr(config, "MOBILEPROXY_CHECK_SPAM", False)

    monkeypatch.setattr(
        pir.utils,
        "fetch_exit_ip_via_proxy",
        lambda p, *args, **kwargs: "5.5.5.5",
    )
    monkeypatch.setattr(pir.utils, "fetch_country_for_ip", lambda ip: "US")

    acc = {"country": "US"}
    with patch.object(pir, "_rotate_once", return_value=(True, "5.5.5.5")):
        pir._ensure_under_lock("acc1", acc, proxy_cfg, "https://change.test/rot")

    data = json.loads(reg_path.read_text(encoding="utf-8"))
    assert data["accounts"]["acc1"]["ip"] == "5.5.5.5"


def test_sticky_hits_remembered_after_rotate(reg_path, proxy_cfg, monkeypatch):
    reg_path.write_text(
        json.dumps({"accounts": {"acc1": {"ip": "7.7.7.7"}}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(config, "SHORTS_PROXY_IP_REGISTRY", True)
    monkeypatch.setattr(config, "MOBILEPROXY_CHANGE_IP_URL", "https://change.test/rot")
    monkeypatch.setattr(
        "pipeline.proxy_ip_registry.mobileproxy_geo_enabled", lambda *a, **k: False
    )
    monkeypatch.setattr(config, "PROXY_IP_MAX_ROTATIONS", 30)
    monkeypatch.setattr(config, "PROXY_IP_MAX_STICKY_ATTEMPTS", 10)
    monkeypatch.setattr(config, "MOBILEPROXY_CHECK_SPAM", False)

    calls = {"n": 0}

    def _ip(_p, *args, **kwargs):
        calls["n"] += 1
        return "1.1.1.1" if calls["n"] == 1 else "7.7.7.7"

    monkeypatch.setattr(pir.utils, "fetch_exit_ip_via_proxy", _ip)
    monkeypatch.setattr(pir.utils, "fetch_country_for_ip", lambda ip: "DE")

    acc = {"country": "DE"}

    with patch.object(pir, "_rotate_once", return_value=(True, "7.7.7.7")):
        pir._ensure_under_lock("acc1", acc, proxy_cfg, "https://change.test/rot")

    data = json.loads(reg_path.read_text(encoding="utf-8"))
    assert data["accounts"]["acc1"]["ip"] == "7.7.7.7"
