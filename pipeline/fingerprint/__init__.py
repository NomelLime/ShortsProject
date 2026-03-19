"""
pipeline/fingerprint — Модуль уникализации browser fingerprint per account.

Основные компоненты:
    generator.py — генерация/хранение fingerprint-профиля
    geo.py       — GEO-согласование timezone/locale/languages
    devices.py   — банк устройств (мобильные/десктопные профили)
    injector.py  — применение fingerprint через JS add_init_script()

Быстрый старт:
    from pipeline.fingerprint.generator import ensure_fingerprint
    fp = ensure_fingerprint(acc_config, platform="tiktok", country="BR")
"""
from pipeline.fingerprint.generator import generate_fingerprint, ensure_fingerprint
from pipeline.fingerprint.geo import get_geo_params, get_all_countries

__all__ = [
    "generate_fingerprint",
    "ensure_fingerprint",
    "get_geo_params",
    "get_all_countries",
]
