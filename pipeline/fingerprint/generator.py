"""
pipeline/fingerprint/generator.py — Генерация уникального fingerprint-профиля.

Каждый аккаунт получает стабильный набор параметров, имитирующих
реальное устройство. Профиль генерируется ОДИН РАЗ при первом запуске
и переиспользуется — платформа видит «одно и то же устройство».

Экспортирует:
    generate_fingerprint(platform, country, seed) → dict
    ensure_fingerprint(acc_config, platform, country) → dict
"""
from __future__ import annotations

import hashlib
import random
import secrets
from typing import Dict, List, Optional

from pipeline.fingerprint.geo import get_geo_params
from pipeline.fingerprint.devices import get_mobile_device, get_desktop_screen

# ─────────────────────────────────────────────────────────────────────────────
# Банки версий Chrome
# ─────────────────────────────────────────────────────────────────────────────

_DESKTOP_CHROME_VERSIONS: List[str] = [
    "120.0.6099.199", "121.0.6167.85",  "122.0.6261.94",
    "123.0.6312.58",  "124.0.6367.91",  "125.0.6422.60",
    "126.0.6478.55",  "127.0.6533.72",  "128.0.6613.84",
    "129.0.6668.58",  "130.0.6723.58",  "131.0.6778.85",
]

_MOBILE_CHROME_VERSIONS: List[str] = [
    "120.0.6099.210", "121.0.6167.101", "122.0.6261.105",
    "123.0.6312.80",  "124.0.6367.113", "125.0.6422.72",
    "126.0.6478.71",  "127.0.6533.88",  "128.0.6613.99",
    "129.0.6668.70",  "130.0.6723.73",  "131.0.6778.104",
]

_DESKTOP_OS_VARIANTS: List[str] = [
    "Windows NT 10.0; Win64; x64",
    "Windows NT 10.0; Win64; x64",  # Windows x2 — наиболее частый
    "Macintosh; Intel Mac OS X 10_15_7",
    "Macintosh; Intel Mac OS X 14_0",
    "X11; Linux x86_64",
]

# ─────────────────────────────────────────────────────────────────────────────
# Банки WebGL-профилей
# ─────────────────────────────────────────────────────────────────────────────

_WEBGL_DESKTOP_PROFILES: List[Dict] = [
    {
        "vendor":            "Google Inc. (NVIDIA)",
        "renderer":          "ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0, D3D11)",
        "unmasked_vendor":   "NVIDIA Corporation",
        "unmasked_renderer": "NVIDIA GeForce RTX 3060/PCIe/SSE2",
    },
    {
        "vendor":            "Google Inc. (NVIDIA)",
        "renderer":          "ANGLE (NVIDIA, NVIDIA GeForce GTX 1660 SUPER Direct3D11 vs_5_0 ps_5_0, D3D11)",
        "unmasked_vendor":   "NVIDIA Corporation",
        "unmasked_renderer": "NVIDIA GeForce GTX 1660 SUPER/PCIe/SSE2",
    },
    {
        "vendor":            "Google Inc. (AMD)",
        "renderer":          "ANGLE (AMD, AMD Radeon RX 580 Direct3D11 vs_5_0 ps_5_0, D3D11)",
        "unmasked_vendor":   "ATI Technologies Inc.",
        "unmasked_renderer": "AMD Radeon RX 580",
    },
    {
        "vendor":            "Google Inc. (Intel)",
        "renderer":          "ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0, D3D11)",
        "unmasked_vendor":   "Intel Inc.",
        "unmasked_renderer": "Intel(R) UHD Graphics 630",
    },
    {
        "vendor":            "Google Inc. (Intel)",
        "renderer":          "ANGLE (Intel, Intel(R) Iris(R) Xe Graphics Direct3D11 vs_5_0 ps_5_0, D3D11)",
        "unmasked_vendor":   "Intel Inc.",
        "unmasked_renderer": "Intel(R) Iris(R) Xe Graphics",
    },
    {
        "vendor":            "Google Inc. (NVIDIA)",
        "renderer":          "ANGLE (NVIDIA, NVIDIA GeForce RTX 4060 Direct3D11 vs_5_0 ps_5_0, D3D11)",
        "unmasked_vendor":   "NVIDIA Corporation",
        "unmasked_renderer": "NVIDIA GeForce RTX 4060/PCIe/SSE2",
    },
]

_WEBGL_MOBILE_PROFILES: List[Dict] = [
    {
        "vendor":            "Qualcomm",
        "renderer":          "Adreno (TM) 740",
        "unmasked_vendor":   "Qualcomm",
        "unmasked_renderer": "Adreno (TM) 740",
    },
    {
        "vendor":            "Qualcomm",
        "renderer":          "Adreno (TM) 730",
        "unmasked_vendor":   "Qualcomm",
        "unmasked_renderer": "Adreno (TM) 730",
    },
    {
        "vendor":            "ARM",
        "renderer":          "Mali-G710 MC10",
        "unmasked_vendor":   "ARM",
        "unmasked_renderer": "Mali-G710 MC10",
    },
    {
        "vendor":            "ARM",
        "renderer":          "Mali-G715 MC10",
        "unmasked_vendor":   "ARM",
        "unmasked_renderer": "Mali-G715 MC10",
    },
    {
        "vendor":            "Qualcomm",
        "renderer":          "Adreno (TM) 660",
        "unmasked_vendor":   "Qualcomm",
        "unmasked_renderer": "Adreno (TM) 660",
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# Банк шрифтов
# ─────────────────────────────────────────────────────────────────────────────

_COMMON_FONTS: List[str] = [
    "Arial", "Verdana", "Helvetica", "Times New Roman", "Georgia",
    "Trebuchet MS", "Courier New", "Impact", "Comic Sans MS",
    "Lucida Console", "Tahoma", "Palatino Linotype", "Century Gothic",
    "Segoe UI", "Calibri", "Cambria", "Candara", "Consolas",
    "Franklin Gothic Medium", "Garamond", "Lucida Sans Unicode",
    "Arial Narrow", "Book Antiqua", "Copperplate Gothic Bold",
    "Courier", "Gill Sans", "Palatino", "Symbol", "Wingdings",
]


# ─────────────────────────────────────────────────────────────────────────────
# Генераторы вспомогательных значений
# ─────────────────────────────────────────────────────────────────────────────

def _make_rng(seed: str) -> random.Random:
    """Создаёт детерминированный RNG из строкового seed."""
    seed_int = int(hashlib.sha256(seed.encode()).hexdigest(), 16) % (2**32)
    return random.Random(seed_int)


def _generate_ua(platform: str, rng: random.Random) -> tuple[str, str, bool]:
    """
    Генерирует User-Agent, имя устройства и флаг is_mobile.

    Returns:
        (user_agent, device_name, is_mobile)
    """
    if platform in ("tiktok", "instagram", "youtube"):
        # Мобильный UA для всех платформ (единая мобильная стратегия)
        from pipeline.fingerprint.devices import _MOBILE_DEVICES
        device = rng.choice(_MOBILE_DEVICES)
        version = rng.choice(_MOBILE_CHROME_VERSIONS)
        ua = (
            f"Mozilla/5.0 (Linux; Android {device[1]}; {device[2]}) "
            f"AppleWebKit/537.36 (KHTML, like Gecko) "
            f"Chrome/{version} Mobile Safari/537.36"
        )
        return ua, device[0], True
    else:
        # Десктопный UA для YouTube и остальных
        version = rng.choice(_DESKTOP_CHROME_VERSIONS)
        os_str  = rng.choice(_DESKTOP_OS_VARIANTS)
        ua = (
            f"Mozilla/5.0 ({os_str}) "
            f"AppleWebKit/537.36 (KHTML, like Gecko) "
            f"Chrome/{version} Safari/537.36"
        )
        device_name = os_str.split(";")[0].strip()
        return ua, device_name, False


def _generate_font_subset(rng: random.Random, is_mobile: bool) -> List[str]:
    """
    Генерирует подмножество системных шрифтов.

    Мобильные устройства имеют меньше шрифтов чем десктопные.
    """
    base  = ["Arial", "Verdana", "Times New Roman", "Courier New"]
    pool  = [f for f in _COMMON_FONTS if f not in base]
    extra_count = rng.randint(2, 4) if is_mobile else rng.randint(5, 10)
    extra = rng.sample(pool, k=min(extra_count, len(pool)))
    return sorted(base + extra)


# ─────────────────────────────────────────────────────────────────────────────
# Публичный API
# ─────────────────────────────────────────────────────────────────────────────

def generate_fingerprint(
    platform: str = "youtube",
    country: str = "US",
    seed: Optional[str] = None,
) -> Dict:
    """
    Генерирует полный fingerprint-профиль для аккаунта.

    Один seed = один fingerprint (воспроизводимость). Если seed не задан —
    генерируется случайно (первичное создание аккаунта).

    Args:
        platform: целевая платформа — "youtube" | "tiktok" | "instagram"
        country:  ISO 3166-1 alpha-2 код страны прокси (напр. "BR")
        seed:     строка-seed для воспроизводимости (None = случайный)

    Returns:
        dict со всеми параметрами fingerprint:
            fp_seed, user_agent, viewport, screen, platform_nav,
            hardware_concurrency, device_memory, max_touch_points,
            canvas_noise_seed, webgl_vendor, webgl_renderer,
            webgl_unmasked_vendor, webgl_unmasked_renderer,
            fonts, audio_context_noise, timezone_id, locale,
            languages, color_depth, pixel_ratio, do_not_track,
            is_mobile, device_name
    """
    fp_seed = seed or secrets.token_hex(8)
    rng = _make_rng(fp_seed)

    # User-Agent + is_mobile
    ua, device_name, is_mobile = _generate_ua(platform, rng)

    # GEO-согласование
    geo = get_geo_params(country)

    # Устройство (viewport, screen, dpr)
    if is_mobile:
        dev = get_mobile_device(rng)
        viewport   = {"width": dev["viewport_width"], "height": dev["viewport_height"]}
        screen     = {"width": dev["screen_width"],   "height": dev["screen_height"],   "depth": 24}
        pixel_ratio    = dev["dpr"]
        max_touch      = dev["touch_points"]
        color_depth    = 24
        platform_nav   = "Linux armv8l"
        hw_concurrency = rng.choice([4, 6, 8])
        device_memory  = rng.choice([4, 6, 8])
        device_name    = dev["device_name"]
    else:
        dev = get_desktop_screen(rng)
        viewport   = {"width": dev["viewport_width"], "height": dev["viewport_height"]}
        screen     = {"width": dev["screen_width"],   "height": dev["screen_height"],   "depth": dev["color_depth"]}
        pixel_ratio    = rng.choice([1.0, 1.25, 1.5, 2.0])
        max_touch      = 0
        color_depth    = dev["color_depth"]
        platform_nav   = rng.choice(["Win32", "Win32", "MacIntel", "Linux x86_64"])
        hw_concurrency = rng.choice([4, 6, 8, 12, 16])
        device_memory  = rng.choice([4, 8, 16])

    # WebGL профиль
    webgl_pool = _WEBGL_MOBILE_PROFILES if is_mobile else _WEBGL_DESKTOP_PROFILES
    webgl = rng.choice(webgl_pool)

    # Canvas noise seed — уникальный per account
    canvas_noise_seed = rng.randint(1, 2**32 - 1)

    # AudioContext noise — субпиксельный сдвиг
    audio_noise = rng.uniform(-1.0, 1.0)

    # Шрифты
    fonts = _generate_font_subset(rng, is_mobile)

    # Do Not Track: ~30% включают
    do_not_track: Optional[str] = "1" if rng.random() < 0.3 else None

    return {
        "fp_seed":                fp_seed,
        "user_agent":             ua,
        "viewport":               viewport,
        "screen":                 screen,
        "platform_nav":           platform_nav,
        "hardware_concurrency":   hw_concurrency,
        "device_memory":          device_memory,
        "max_touch_points":       max_touch,
        "canvas_noise_seed":      canvas_noise_seed,
        "webgl_vendor":           webgl["vendor"],
        "webgl_renderer":         webgl["renderer"],
        "webgl_unmasked_vendor":  webgl["unmasked_vendor"],
        "webgl_unmasked_renderer": webgl["unmasked_renderer"],
        "fonts":                  fonts,
        "audio_context_noise":    audio_noise,
        "timezone_id":            geo["tz"],
        "locale":                 geo["locale"],
        "languages":              geo["langs"],
        "color_depth":            color_depth,
        "pixel_ratio":            pixel_ratio,
        "do_not_track":           do_not_track,
        "is_mobile":              is_mobile,
        "device_name":            device_name,
    }


def ensure_fingerprint(
    acc_config: dict,
    platform: str,
    country: str = "",
) -> Dict:
    """
    Возвращает существующий fingerprint или генерирует новый (ленивая инициализация).

    Fingerprint'ы хранятся per-platform в acc_config["fingerprint"][platform].
    Первый вызов → генерация + сохранение в acc_config (caller должен записать на диск).
    Последующие вызовы → возвращает существующий без изменений.

    Args:
        acc_config: dict конфигурации аккаунта (account/config.json)
        platform:   "youtube" | "tiktok" | "instagram"
        country:    ISO код страны для GEO-согласования (из acc_config["country"])

    Returns:
        dict fingerprint-профиля для данной платформы.
    """
    all_fp: dict = acc_config.setdefault("fingerprint", {})

    if platform in all_fp:
        return all_fp[platform]

    # Генерируем новый fingerprint
    fp = generate_fingerprint(
        platform=platform,
        country=(country or "US").upper(),
    )
    all_fp[platform] = fp
    return fp
