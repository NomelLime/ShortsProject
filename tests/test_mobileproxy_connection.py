"""Тесты mobileproxy_connection (без сети)."""

from __future__ import annotations

from pipeline.mobileproxy_connection import http_proxy_dict_from_my_proxy_row, invalidate_mobileproxy_http_cache


def test_http_proxy_dict_from_row_full():
    row = {
        "proxy_id": 123,
        "proxy_hostname": "s1.example.com",
        "proxy_http_port": 5000,
        "proxy_login": "u1",
        "proxy_pass": "p1",
    }
    d = http_proxy_dict_from_my_proxy_row(row)
    assert d == {
        "host": "s1.example.com",
        "port": 5000,
        "username": "u1",
        "password": "p1",
    }


def test_http_proxy_dict_fallback_ip_and_independent_port():
    row = {
        "proxy_host_ip": "1.2.3.4",
        "proxy_independent_port": 3128,
    }
    d = http_proxy_dict_from_my_proxy_row(row)
    assert d == {"host": "1.2.3.4", "port": 3128}


def test_http_proxy_dict_invalid():
    assert http_proxy_dict_from_my_proxy_row(None) is None
    assert http_proxy_dict_from_my_proxy_row({}) is None
    assert http_proxy_dict_from_my_proxy_row({"proxy_hostname": "x"}) is None


def test_invalidate_does_not_raise():
    invalidate_mobileproxy_http_cache()
