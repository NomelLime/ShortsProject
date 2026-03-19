"""
tests/test_geo.py — Тесты GEO-справочника (Сессия 12, ФИЧА 1).
"""
from __future__ import annotations
import importlib.util, sys
from pathlib import Path
import pytest

_ROOT = Path(__file__).parent.parent

def _load(name: str):
    p = _ROOT / "pipeline" / Path(*name.split(".")).with_suffix(".py")
    spec = importlib.util.spec_from_file_location(f"pipeline.{name}", p)
    m = importlib.util.module_from_spec(spec)
    sys.modules[f"pipeline.{name}"] = m
    spec.loader.exec_module(m)
    return m

_geo = _load("fingerprint.geo")


class TestGetGeoParams:
    def test_known_country_returns_valid_tz(self):
        assert _geo.get_geo_params("US")["tz"] == "America/New_York"
        assert _geo.get_geo_params("DE")["tz"] == "Europe/Berlin"
        assert _geo.get_geo_params("BR")["tz"] == "America/Sao_Paulo"
        assert _geo.get_geo_params("JP")["tz"] == "Asia/Tokyo"
        assert _geo.get_geo_params("UA")["tz"] == "Europe/Kiev"

    def test_known_country_returns_valid_locale(self):
        assert _geo.get_geo_params("US")["locale"] == "en-US"
        assert _geo.get_geo_params("DE")["locale"] == "de-DE"
        assert _geo.get_geo_params("BR")["locale"] == "pt-BR"
        assert _geo.get_geo_params("RU")["locale"] == "ru-RU"

    def test_known_country_returns_languages_list(self):
        langs = _geo.get_geo_params("BR")["langs"]
        assert isinstance(langs, list)
        assert len(langs) >= 1
        assert "pt-BR" in langs

    def test_unknown_country_returns_default_us(self):
        result = _geo.get_geo_params("XX")
        assert result["tz"]     == "America/New_York"
        assert result["locale"] == "en-US"

    def test_case_insensitive(self):
        assert _geo.get_geo_params("de") == _geo.get_geo_params("DE")
        assert _geo.get_geo_params("br") == _geo.get_geo_params("BR")

    def test_all_countries_have_required_fields(self):
        for code in _geo.get_all_countries():
            params = _geo.get_geo_params(code)
            assert "tz"     in params, f"{code}: нет tz"
            assert "locale" in params, f"{code}: нет locale"
            assert "langs"  in params, f"{code}: нет langs"
            assert isinstance(params["langs"], list)
            assert len(params["langs"]) >= 1

    def test_all_countries_tz_non_empty(self):
        for code in _geo.get_all_countries():
            tz = _geo.get_geo_params(code)["tz"]
            assert "/" in tz, f"{code}: невалидный IANA timezone: {tz!r}"

    def test_get_all_countries_sorted(self):
        countries = _geo.get_all_countries()
        assert countries == sorted(countries)

    def test_minimum_country_coverage(self):
        """Покрытие ключевых GEO для арбитража трафика."""
        required = {"US", "BR", "DE", "GB", "FR", "IN", "TR", "UA", "PL", "MX"}
        for code in required:
            assert code in _geo.get_all_countries(), f"Страна {code} отсутствует"

    def test_returns_copy_not_reference(self):
        """get_geo_params возвращает копию, не ссылку на внутренний dict."""
        r1 = _geo.get_geo_params("US")
        r1["tz"] = "MODIFIED"
        r2 = _geo.get_geo_params("US")
        assert r2["tz"] == "America/New_York"
