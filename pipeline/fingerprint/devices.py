"""
pipeline/fingerprint/devices.py — Банк устройств для мобильной/десктопной эмуляции.

Реальные размеры экранов, DPR и touch points для популярных устройств.
Используется generator.py для генерации viewport/screen параметров.

Экспортирует:
    get_mobile_device(rng, preferred_name?) → dict
    get_desktop_screen(rng) → dict
"""
from __future__ import annotations

import random
from typing import Dict, List, Optional, Tuple

# (device_name, android_version, model_string, chrome_mobile_ua_fragment)
_MOBILE_DEVICES: List[Tuple[str, str, str]] = [
    ("Samsung Galaxy S23",   "13", "SM-S911B"),
    ("Samsung Galaxy S24",   "14", "SM-S921B"),
    ("Samsung Galaxy S22",   "12", "SM-S901B"),
    ("Samsung Galaxy A54",   "13", "SM-A546B"),
    ("Samsung Galaxy A34",   "13", "SM-A346B"),
    ("Google Pixel 7",       "13", "Pixel 7"),
    ("Google Pixel 8",       "14", "Pixel 8"),
    ("Google Pixel 7a",      "13", "Pixel 7a"),
    ("Xiaomi 13",            "13", "2211133G"),
    ("Xiaomi 13T",           "13", "23078PND5G"),
    ("Redmi Note 12",        "13", "2303CRA44A"),
    ("OnePlus 11",           "13", "CPH2449"),
    ("OnePlus 12",           "14", "CPH2583"),
    ("Realme 11 Pro",        "13", "RMX3771"),
    ("OPPO Find X6",         "13", "PGEM10"),
    ("Motorola Edge 40",     "13", "XT2303-2"),
]

# (device_name, screen_width, screen_height, dpr, touch_points)
_MOBILE_SCREENS: List[Tuple[str, int, int, float, int]] = [
    ("Samsung Galaxy S23",   360, 780,  3.0,   5),
    ("Samsung Galaxy S24",   360, 780,  3.0,   5),
    ("Samsung Galaxy S22",   360, 780,  3.0,   5),
    ("Samsung Galaxy A54",   360, 800,  2.0,   5),
    ("Samsung Galaxy A34",   360, 800,  2.0,   5),
    ("Google Pixel 7",       412, 915,  2.625, 5),
    ("Google Pixel 8",       412, 915,  2.625, 5),
    ("Google Pixel 7a",      390, 844,  2.625, 5),
    ("iPhone 14 Pro",        393, 852,  3.0,   5),
    ("iPhone 15",            393, 852,  3.0,   5),
    ("iPhone 13",            390, 844,  3.0,   5),
    ("Xiaomi 13",            393, 873,  2.75,  5),
    ("Redmi Note 12",        393, 873,  2.0,   5),
    ("OnePlus 11",           412, 919,  3.0,   5),
    ("Realme 11 Pro",        402, 874,  2.0,   5),
    ("Motorola Edge 40",     412, 915,  2.625, 5),
]

# (screen_width, screen_height, color_depth)
_DESKTOP_SCREENS: List[Tuple[int, int, int]] = [
    (1920, 1080, 24),
    (1366, 768,  24),
    (1536, 864,  24),
    (1440, 900,  24),
    (2560, 1440, 24),
    (1680, 1050, 24),
    (1280, 720,  24),
    (1600, 900,  24),
    (1280, 1024, 24),
    (1024, 768,  24),
]

# Viewport чуть меньше экрана (панели браузера)
_VIEWPORT_REDUCTION_DESKTOP = (0, 90)   # (w_reduce, h_reduce)
_VIEWPORT_REDUCTION_MOBILE  = (0, 56)   # адресная строка


def get_mobile_device(rng: random.Random) -> Dict:
    """
    Возвращает случайный мобильный профиль устройства.

    Args:
        rng: инициализированный random.Random (для воспроизводимости)

    Returns:
        dict с полями:
            device_name, android_version, model_string — для UA
            screen_width, screen_height, dpr, touch_points — для браузера
            viewport_width, viewport_height — немного меньше экрана
    """
    # Выбираем устройство и экран независимо (как в реальности)
    device = rng.choice(_MOBILE_DEVICES)
    screen = rng.choice(_MOBILE_SCREENS)

    vp_w = screen[1]
    vp_h = screen[2] - _VIEWPORT_REDUCTION_MOBILE[1]

    return {
        "device_name":      device[0],
        "android_version":  device[1],
        "model_string":     device[2],
        "screen_width":     screen[1],
        "screen_height":    screen[2],
        "dpr":              screen[3],
        "touch_points":     screen[4],
        "viewport_width":   vp_w,
        "viewport_height":  vp_h,
    }


def get_desktop_screen(rng: random.Random) -> Dict:
    """
    Возвращает случайный профиль десктопного экрана.

    Args:
        rng: инициализированный random.Random

    Returns:
        dict с полями:
            screen_width, screen_height, color_depth
            viewport_width, viewport_height
    """
    screen = rng.choice(_DESKTOP_SCREENS)
    w_reduce, h_reduce = _VIEWPORT_REDUCTION_DESKTOP

    return {
        "screen_width":   screen[0],
        "screen_height":  screen[1],
        "color_depth":    screen[2],
        "viewport_width":  screen[0] - w_reduce,
        "viewport_height": screen[1] - h_reduce,
    }
