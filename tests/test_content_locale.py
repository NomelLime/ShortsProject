"""Тесты разрешения локали контента по стране / аккаунту."""
from __future__ import annotations


class TestResolveFromCountry:
    def test_known_de(self):
        from pipeline.content_locale import resolve_content_locale_from_country

        assert resolve_content_locale_from_country("DE") == "de-DE"

    def test_unknown_iso_uses_en(self):
        from pipeline.content_locale import resolve_content_locale_from_country, FALLBACK_CONTENT_LOCALE

        assert resolve_content_locale_from_country("XX") == FALLBACK_CONTENT_LOCALE

    def test_empty_uses_en(self):
        from pipeline.content_locale import resolve_content_locale_from_country, FALLBACK_CONTENT_LOCALE

        assert resolve_content_locale_from_country("") == FALLBACK_CONTENT_LOCALE
        assert resolve_content_locale_from_country(None) == FALLBACK_CONTENT_LOCALE

    def test_latam_extra_es_419(self):
        from pipeline.content_locale import resolve_content_locale_from_country

        assert resolve_content_locale_from_country("BO") == "es-419"


class TestAccount:
    def test_manual_content_locale(self):
        from pipeline.content_locale import resolve_content_locale_for_account

        loc = resolve_content_locale_for_account({
            "content_locale": "de-DE",
            "country": "US",
        })
        assert loc == "de-DE"

    def test_country_over_proxy(self, monkeypatch):
        from pipeline.content_locale import resolve_content_locale_for_account

        monkeypatch.setattr(
            "pipeline.browser.get_proxy_country",
            lambda _p: "BR",
        )
        loc = resolve_content_locale_for_account({
            "country": "DE",
            "proxy": {"host": "1.2.3.4", "port": 8080},
        })
        assert loc == "de-DE"

    def test_proxy_when_no_country(self, monkeypatch):
        from pipeline.content_locale import resolve_content_locale_for_account

        monkeypatch.setattr(
            "pipeline.browser.get_proxy_country",
            lambda _p: "BR",
        )
        loc = resolve_content_locale_for_account({
            "proxy": {"host": "1.2.3.4", "port": 8080},
        })
        assert loc == "pt-BR"


def test_normalize_content_locale():
    from pipeline.content_locale import normalize_content_locale

    assert normalize_content_locale("en") == "en-US"
    assert normalize_content_locale("de_DE") == "de-DE"
