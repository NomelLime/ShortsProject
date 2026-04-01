"""Тесты mobileproxy_api (без реальной сети)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from pipeline import config
from pipeline import mobileproxy_api as mpa


def test_resolve_iso_manual(monkeypatch):
    monkeypatch.setattr(config, "MOBILEPROXY_API_KEY", "")
    monkeypatch.setattr(config, "MOBILEPROXY_ISO_TO_ID_JSON", '{"US": 99, "DE": 7}')
    mpa.invalidate_iso_to_id_country_cache()
    assert mpa.resolve_iso_to_id_country("US") == 99
    assert mpa.resolve_iso_to_id_country("de") == 7


def test_list_supported_iso2_codes_and_iso_supported(monkeypatch):
    monkeypatch.setattr(config, "MOBILEPROXY_API_KEY", "")
    monkeypatch.setattr(config, "MOBILEPROXY_ISO_TO_ID_JSON", '{"US": 99, "DE": 7}')
    mpa.invalidate_iso_to_id_country_cache()
    assert mpa.list_supported_iso2_codes() == ["DE", "US"]
    assert mpa.iso_supported_by_mobileproxy("US") is True
    assert mpa.iso_supported_by_mobileproxy("ZZ") is False


def test_iso_map_from_get_id_country_accepts_list():
    data = {
        "status": "ok",
        "id_country": [
            {"iso": "VN", "id_country": 42},
            {"iso": "US", "id_country": 1},
        ],
    }
    assert mpa._iso_map_from_get_id_country(data) == {"VN": 42, "US": 1}


def test_iso_map_from_get_id_country_accepts_uppercase_ISO_key():
    """Как в реальном ответе mobileproxy.space: поле «ISO», не «iso»."""
    data = {
        "status": "ok",
        "id_country": {
            "1": {"id_country": "1", "name": "Россия", "ISO": "RU"},
            "15": {"id_country": "15", "ISO": "AM"},
        },
    }
    assert mpa._iso_map_from_get_id_country(data) == {"RU": 1, "AM": 15}


def test_ensure_equipment_noop_when_current_matches(monkeypatch):
    monkeypatch.setattr(config, "MOBILEPROXY_API_KEY", "k")
    monkeypatch.setattr(config, "MOBILEPROXY_PROXY_ID", "1")
    monkeypatch.setattr(config, "MOBILEPROXY_ISO_TO_ID_JSON", '{"XX": 5}')
    mpa.invalidate_iso_to_id_country_cache()
    mpa.invalidate_my_proxy_cache()
    with patch.object(
        mpa,
        "get_my_proxy_row",
        return_value={"proxy_id": 1, "id_country": 5},
    ):
        mpa.ensure_equipment_country_for_iso("XX")  # no raise


def test_ensure_equipment_calls_change_when_diff(monkeypatch):
    monkeypatch.setattr(config, "MOBILEPROXY_API_KEY", "k")
    monkeypatch.setattr(config, "MOBILEPROXY_PROXY_ID", "1")
    monkeypatch.setattr(config, "MOBILEPROXY_ISO_TO_ID_JSON", '{"XX": 5}')
    monkeypatch.setattr(config, "MOBILEPROXY_POST_GEO_PAUSE_SEC", 0.0)
    monkeypatch.setattr(config, "MOBILEPROXY_CHANGE_EQUIPMENT_MAX_ATTEMPTS", 1)
    mpa.invalidate_iso_to_id_country_cache()
    mpa.invalidate_my_proxy_cache()
    with patch.object(
        mpa,
        "get_my_proxy_row",
        return_value={"proxy_id": 1, "id_country": 1},
    ), patch.object(mpa, "change_equipment_to_country", return_value=True) as chg:
        mpa.ensure_equipment_country_for_iso("XX")
        chg.assert_called_once()
        assert chg.call_args[0][0] == 5
        assert chg.call_args.kwargs.get("proxy_id") == 1


def test_spam_check_requires_rotation():
    from pipeline.mobileproxy_api import spam_check_requires_rotation

    assert spam_check_requires_rotation(None) is False
    assert spam_check_requires_rotation({"status": "fail"}) is False
    assert spam_check_requires_rotation(
        {"status": "ok", "ipguardian.net": {"spam": True}}
    ) is True
    assert spam_check_requires_rotation(
        {"status": "ok", "ipguardian.net": {"listed": "1"}}
    ) is True
    assert spam_check_requires_rotation(
        {"status": "ok", "ipguardian.net": {}}
    ) is False


def test_sort_accounts_by_country():
    from pipeline.utils import sort_accounts_by_country

    accs = [
        {"name": "z", "config": {"country": "US"}},
        {"name": "a", "config": {"country": "DE"}},
        {"name": "m", "config": {}},
    ]
    s = sort_accounts_by_country(accs)
    assert [x["name"] for x in s] == ["a", "z", "m"]
