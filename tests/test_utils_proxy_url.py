"""Тесты proxy_cfg_to_http_url / load_proxy (без сети)."""

from __future__ import annotations

from unittest.mock import patch

from pipeline import utils


def test_proxy_cfg_to_http_url_no_auth():
    assert utils.proxy_cfg_to_http_url({"host": "1.2.3.4", "port": 8080}) == "http://1.2.3.4:8080"


def test_proxy_cfg_to_http_url_with_auth():
    u = utils.proxy_cfg_to_http_url(
        {"host": "p.example.com", "port": 5000, "username": "u1", "password": "p@ss"}
    )
    assert u.startswith("http://")
    assert "@p.example.com:5000" in u
    assert "u1" in u


def test_proxy_cfg_to_url_socks5():
    u = utils.proxy_cfg_to_url(
        {
            "host": "bproxy.site",
            "port": 14284,
            "username": "u1",
            "password": "p1",
            "scheme": "socks5h",
        }
    )
    assert u.startswith("socks5h://")
    assert "@bproxy.site:14284" in u


def test_proxy_url_to_cfg_socks5():
    cfg = utils.proxy_url_to_cfg("socks5h://u1:p1@bproxy.site:14284")
    assert cfg is not None
    assert cfg["scheme"] == "socks5h"
    assert cfg["host"] == "bproxy.site"
    assert cfg["port"] == 14284


def test_load_proxy_env_wins(monkeypatch):
    monkeypatch.setenv("PROXY", "http://override:9")
    assert utils.load_proxy() == "http://override:9"


def test_load_proxy_falls_back_to_mobileproxy(monkeypatch):
    monkeypatch.delenv("PROXY", raising=False)
    fake = {"host": "h.test", "port": 111, "username": "a", "password": "b"}
    with patch(
        "pipeline.mobileproxy_connection.fetch_mobileproxy_http_proxy",
        return_value=fake,
    ):
        assert utils.load_proxy() == utils.proxy_cfg_to_http_url(fake)


def test_load_proxy_none_when_no_env_no_mobileproxy(monkeypatch):
    monkeypatch.delenv("PROXY", raising=False)
    with patch(
        "pipeline.mobileproxy_connection.fetch_mobileproxy_http_proxy",
        return_value=None,
    ):
        assert utils.load_proxy() is None


def test_resolve_pipeline_account_name_requires_config(tmp_path, monkeypatch):
    monkeypatch.setenv("SHORTS_PIPELINE_ACCOUNT", "acc1")
    monkeypatch.setattr(utils.config, "ACCOUNTS_ROOT", str(tmp_path))
    assert utils.resolve_pipeline_account_name() is None
    (tmp_path / "acc1").mkdir()
    (tmp_path / "acc1" / "config.json").write_text("{}", encoding="utf-8")
    assert utils.resolve_pipeline_account_name() == "acc1"
