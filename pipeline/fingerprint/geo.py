"""
pipeline/fingerprint/geo.py — GEO-согласование параметров браузера.

Timezone, locale и языки должны соответствовать IP прокси.
Несоответствие (например, US IP + московский timezone) — один из главных
сигналов антидетекта TikTok и Instagram.

Экспортирует:
    get_geo_params(country_code: str) → dict
    get_all_countries() → list[str]
"""
from __future__ import annotations

from typing import Dict, List

# 40+ стран — покрывает основные GEO для арбитража трафика
_GEO_MAP: Dict[str, Dict] = {
    # ── Северная Америка ──────────────────────────────────────────────────────
    "US": {"tz": "America/New_York",       "locale": "en-US", "langs": ["en-US", "en"]},
    "CA": {"tz": "America/Toronto",        "locale": "en-CA", "langs": ["en-CA", "en"]},
    "MX": {"tz": "America/Mexico_City",    "locale": "es-MX", "langs": ["es-MX", "es"]},

    # ── Латинская Америка ─────────────────────────────────────────────────────
    "BR": {"tz": "America/Sao_Paulo",      "locale": "pt-BR", "langs": ["pt-BR", "pt"]},
    "AR": {"tz": "America/Argentina/Buenos_Aires", "locale": "es-AR", "langs": ["es-AR", "es"]},
    "CO": {"tz": "America/Bogota",         "locale": "es-CO", "langs": ["es-CO", "es"]},
    "CL": {"tz": "America/Santiago",       "locale": "es-CL", "langs": ["es-CL", "es"]},
    "PE": {"tz": "America/Lima",           "locale": "es-PE", "langs": ["es-PE", "es"]},
    "VE": {"tz": "America/Caracas",        "locale": "es-VE", "langs": ["es-VE", "es"]},

    # ── Западная Европа ───────────────────────────────────────────────────────
    "GB": {"tz": "Europe/London",          "locale": "en-GB", "langs": ["en-GB", "en"]},
    "DE": {"tz": "Europe/Berlin",          "locale": "de-DE", "langs": ["de-DE", "de", "en"]},
    "FR": {"tz": "Europe/Paris",           "locale": "fr-FR", "langs": ["fr-FR", "fr"]},
    "IT": {"tz": "Europe/Rome",            "locale": "it-IT", "langs": ["it-IT", "it"]},
    "ES": {"tz": "Europe/Madrid",          "locale": "es-ES", "langs": ["es-ES", "es"]},
    "NL": {"tz": "Europe/Amsterdam",       "locale": "nl-NL", "langs": ["nl-NL", "nl", "en"]},
    "PT": {"tz": "Europe/Lisbon",          "locale": "pt-PT", "langs": ["pt-PT", "pt"]},
    "AT": {"tz": "Europe/Vienna",          "locale": "de-AT", "langs": ["de-AT", "de"]},
    "BE": {"tz": "Europe/Brussels",        "locale": "nl-BE", "langs": ["nl-BE", "fr-BE", "en"]},
    "CH": {"tz": "Europe/Zurich",          "locale": "de-CH", "langs": ["de-CH", "fr-CH", "en"]},

    # ── Северная Европа ───────────────────────────────────────────────────────
    "SE": {"tz": "Europe/Stockholm",       "locale": "sv-SE", "langs": ["sv-SE", "sv", "en"]},
    "NO": {"tz": "Europe/Oslo",            "locale": "nb-NO", "langs": ["nb-NO", "nb", "en"]},
    "FI": {"tz": "Europe/Helsinki",        "locale": "fi-FI", "langs": ["fi-FI", "fi", "en"]},
    "DK": {"tz": "Europe/Copenhagen",      "locale": "da-DK", "langs": ["da-DK", "da", "en"]},

    # ── Восточная Европа ──────────────────────────────────────────────────────
    "PL": {"tz": "Europe/Warsaw",          "locale": "pl-PL", "langs": ["pl-PL", "pl", "en"]},
    "UA": {"tz": "Europe/Kiev",            "locale": "uk-UA", "langs": ["uk-UA", "uk", "ru"]},
    "CZ": {"tz": "Europe/Prague",          "locale": "cs-CZ", "langs": ["cs-CZ", "cs"]},
    "RO": {"tz": "Europe/Bucharest",       "locale": "ro-RO", "langs": ["ro-RO", "ro"]},
    "HU": {"tz": "Europe/Budapest",        "locale": "hu-HU", "langs": ["hu-HU", "hu"]},
    "SK": {"tz": "Europe/Bratislava",      "locale": "sk-SK", "langs": ["sk-SK", "sk", "cs"]},
    "HR": {"tz": "Europe/Zagreb",          "locale": "hr-HR", "langs": ["hr-HR", "hr"]},
    "RS": {"tz": "Europe/Belgrade",        "locale": "sr-RS", "langs": ["sr-RS", "sr"]},
    "BG": {"tz": "Europe/Sofia",           "locale": "bg-BG", "langs": ["bg-BG", "bg"]},
    "GR": {"tz": "Europe/Athens",          "locale": "el-GR", "langs": ["el-GR", "el", "en"]},

    # ── СНГ ───────────────────────────────────────────────────────────────────
    "RU": {"tz": "Europe/Moscow",          "locale": "ru-RU", "langs": ["ru-RU", "ru"]},
    "KZ": {"tz": "Asia/Almaty",            "locale": "ru-KZ", "langs": ["ru-KZ", "kk-KZ", "ru"]},
    "BY": {"tz": "Europe/Minsk",           "locale": "ru-BY", "langs": ["ru-BY", "be-BY", "ru"]},

    # ── Азия ──────────────────────────────────────────────────────────────────
    "IN": {"tz": "Asia/Kolkata",           "locale": "en-IN", "langs": ["en-IN", "hi-IN", "en"]},
    "JP": {"tz": "Asia/Tokyo",             "locale": "ja-JP", "langs": ["ja-JP", "ja"]},
    "KR": {"tz": "Asia/Seoul",             "locale": "ko-KR", "langs": ["ko-KR", "ko"]},
    "TH": {"tz": "Asia/Bangkok",           "locale": "th-TH", "langs": ["th-TH", "th", "en"]},
    "ID": {"tz": "Asia/Jakarta",           "locale": "id-ID", "langs": ["id-ID", "id"]},
    "PH": {"tz": "Asia/Manila",            "locale": "en-PH", "langs": ["en-PH", "fil", "en"]},
    "VN": {"tz": "Asia/Ho_Chi_Minh",       "locale": "vi-VN", "langs": ["vi-VN", "vi"]},
    "TR": {"tz": "Europe/Istanbul",        "locale": "tr-TR", "langs": ["tr-TR", "tr"]},
    "AE": {"tz": "Asia/Dubai",             "locale": "ar-AE", "langs": ["ar-AE", "en"]},
    "IL": {"tz": "Asia/Jerusalem",         "locale": "he-IL", "langs": ["he-IL", "he", "en"]},
    "SG": {"tz": "Asia/Singapore",         "locale": "en-SG", "langs": ["en-SG", "zh-SG", "en"]},
    "MY": {"tz": "Asia/Kuala_Lumpur",      "locale": "ms-MY", "langs": ["ms-MY", "en-MY", "en"]},
    "PK": {"tz": "Asia/Karachi",           "locale": "ur-PK", "langs": ["ur-PK", "en-PK", "en"]},

    # ── Океания ───────────────────────────────────────────────────────────────
    "AU": {"tz": "Australia/Sydney",       "locale": "en-AU", "langs": ["en-AU", "en"]},
    "NZ": {"tz": "Pacific/Auckland",       "locale": "en-NZ", "langs": ["en-NZ", "en"]},

    # ── Африка ────────────────────────────────────────────────────────────────
    "ZA": {"tz": "Africa/Johannesburg",    "locale": "en-ZA", "langs": ["en-ZA", "en"]},
    "NG": {"tz": "Africa/Lagos",           "locale": "en-NG", "langs": ["en-NG", "en"]},
    "EG": {"tz": "Africa/Cairo",           "locale": "ar-EG", "langs": ["ar-EG", "ar"]},
}

_DEFAULT_GEO: Dict = {
    "tz": "America/New_York",
    "locale": "en-US",
    "langs": ["en-US", "en"],
}


def get_geo_params(country_code: str) -> Dict:
    """
    Возвращает GEO-параметры браузера для заданной страны.

    Args:
        country_code: ISO 3166-1 alpha-2 код страны (напр. "BR", "DE")

    Returns:
        dict с ключами:
            tz    — IANA timezone ID (напр. "America/Sao_Paulo")
            locale — BCP 47 locale (напр. "pt-BR")
            langs  — Accept-Language список (напр. ["pt-BR", "pt"])

    Если страна не найдена — возвращает дефолтные US параметры.
    """
    return _GEO_MAP.get(country_code.upper().strip(), _DEFAULT_GEO).copy()


def get_all_countries() -> List[str]:
    """Возвращает список поддерживаемых ISO кодов стран, отсортированный."""
    return sorted(_GEO_MAP.keys())
