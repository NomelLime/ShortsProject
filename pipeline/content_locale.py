"""
Локаль контента (title/description/captions) по стране прокси / аккаунта.

Политика:
  - Язык задаётся по ISO-стране из конфига аккаунта или по гео активного прокси.
  - Неизвестная / невалидная страна → en-US.
  - Страна есть в fingerprint/geo.py → «местный» locale оттуда.
  - Известная латиноамериканская страна без отдельной строки в geo → es-419 (испанский по умолчанию).
  - Ручной override: content_locale в config.json аккаунта (BCP-47).
"""
from __future__ import annotations

import re
from typing import Optional

from pipeline.fingerprint.geo import get_all_countries, get_geo_params

# Страны Латинской Америки / Кариб с преобладанием испанского, которых может не быть в _GEO_MAP.
# BR, PT в Европе и т.п. не включать — для них действует geo или en-US.
_LATAM_ES_DEFAULT_ISO: frozenset[str] = frozenset({
    "BO", "CR", "CU", "DO", "EC", "GT", "HN", "NI", "PA", "PY", "SV", "UY",
    "PR", "GQ",
})

FALLBACK_CONTENT_LOCALE = "en-US"
_LATAM_ES_LOCALE = "es-419"

_geo_keys: frozenset[str] = frozenset(get_all_countries())


def resolve_content_locale_from_country(country_code: Optional[str]) -> str:
    """
    Возвращает BCP-47 локаль для генерации текстов по ISO-коду страны.

    Пустой / невалидный код → en-US.
    """
    if not country_code:
        return FALLBACK_CONTENT_LOCALE
    cc = country_code.upper().strip()
    if len(cc) != 2 or not cc.isalpha():
        return FALLBACK_CONTENT_LOCALE
    if cc in _geo_keys:
        return get_geo_params(cc)["locale"]
    if cc in _LATAM_ES_DEFAULT_ISO:
        return _LATAM_ES_LOCALE
    return FALLBACK_CONTENT_LOCALE


def resolve_content_locale_for_account(account_cfg: dict) -> str:
    """
    Локаль для аккаунта: ручной content_locale, иначе country, иначе гео прокси.
    """
    raw = (account_cfg.get("content_locale") or "").strip()
    if raw:
        if _looks_like_bcp47_locale(raw):
            return normalize_content_locale(raw)
        # некорректное значение — игнорируем, идём по автоматике
    country = (account_cfg.get("country") or "").upper().strip()
    if len(country) == 2 and country.isalpha():
        return resolve_content_locale_from_country(country)

    from pipeline.browser import get_proxy_country

    proxy = account_cfg.get("_active_proxy") or account_cfg.get("proxy") or {}
    if proxy.get("host"):
        detected = get_proxy_country(proxy)
        if detected:
            return resolve_content_locale_from_country(detected)

    return FALLBACK_CONTENT_LOCALE


def _looks_like_bcp47_locale(s: str) -> bool:
    s = s.strip()
    return bool(re.match(r"^[A-Za-z]{2,3}([_-][A-Za-z0-9]{2,8})*$", s))


def normalize_content_locale(s: str) -> str:
    """en_us → en-US; en → en-US."""
    s = (s or "").strip()
    if not s:
        return FALLBACK_CONTENT_LOCALE
    parts = re.split(r"[-_]", s, maxsplit=1)
    lang = parts[0].lower()
    if len(lang) < 2:
        return FALLBACK_CONTENT_LOCALE
    if len(parts) < 2 or not parts[1].strip():
        return f"{lang}-US" if lang == "en" else lang
    region = parts[1].upper()
    return f"{lang}-{region}"


def content_language_name_for_prompt(content_locale: str) -> str:
    """Короткое имя языка для инструкций к LLM (английский текст инструкции)."""
    base = (content_locale or FALLBACK_CONTENT_LOCALE).split("-")[0].lower()
    names = {
        "en": "English",
        "ru": "Russian",
        "de": "German",
        "fr": "French",
        "es": "Spanish",
        "pt": "Portuguese",
        "it": "Italian",
        "nl": "Dutch",
        "pl": "Polish",
        "uk": "Ukrainian",
        "ja": "Japanese",
        "ko": "Korean",
        "zh": "Chinese",
        "tr": "Turkish",
        "ar": "Arabic",
        "hi": "Hindi",
        "id": "Indonesian",
        "vi": "Vietnamese",
        "th": "Thai",
        "sv": "Swedish",
        "nb": "Norwegian",
        "da": "Danish",
        "fi": "Finnish",
        "cs": "Czech",
        "ro": "Romanian",
        "hu": "Hungarian",
        "el": "Greek",
        "he": "Hebrew",
        "ms": "Malay",
    }
    return names.get(base, f"the language of locale {content_locale}")


def platform_meta_hint_line(platform: str) -> str:
    """Одна строка про лимиты под YouTube / Instagram / TikTok (для промпта)."""
    p = (platform or "youtube").lower()
    if p == "instagram":
        return (
            "Platform: Instagram Reels — caption is the main text field; "
            "keep hook_text punchy; hashtags optional but can help discovery."
        )
    if p == "tiktok":
        return (
            "Platform: TikTok — very short on-screen text; "
            "hook_text and title should be extremely tight."
        )
    return (
        "Platform: YouTube Shorts — title and description support search; "
        "first line matters most."
    )
