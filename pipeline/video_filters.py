"""
pipeline/video_filters.py — Библиотека визуальных ffmpeg-фильтров.

Каждый фильтр — строка, вставляемая в filter_complex ПОСЛЕ масштабирования
и ДО наложения баннера/текста.

Использование:
    from pipeline.video_filters import get_filter, get_random_filter, AVAILABLE_FILTERS

    filter_str = get_filter("warm")  # → "eq=brightness=0.04:saturation=1.2,..."
    filter_str = get_filter("none")  # → ""  (без фильтра)
"""

from __future__ import annotations

import random
from typing import Dict, List, Optional

# Словарь: имя → ffmpeg filter_complex фрагмент.
# Каждый фильтр: один вход → один выход. Не содержит ';' (используется в цепочке, не граф).
FILTER_REGISTRY: Dict[str, str] = {
    # ── Цветовая температура ──────────────────────────────────────────────────
    "warm":        "eq=brightness=0.04:saturation=1.2,colortemperature=6500",
    "cold":        "eq=brightness=0.02:saturation=1.1,colortemperature=4500",

    # ── Насыщенность / контраст ───────────────────────────────────────────────
    "vibrant":       "eq=saturation=1.5:contrast=1.1",
    "muted":         "eq=saturation=0.7:brightness=0.03",
    "high_contrast": "eq=contrast=1.4:brightness=-0.05",

    # ── Стилевые / художественные ─────────────────────────────────────────────
    "sepia":    "colorchannelmixer=.393:.769:.189:0:.349:.686:.168:0:.272:.534:.131",
    "grayscale": "hue=s=0",
    "vintage":  "curves=vintage",
    "vhs":      "noise=alls=20:allf=t+u,eq=saturation=1.3,hue=h=5",
    "film_grain": "noise=alls=12:allf=t",

    # ── Виньетка ──────────────────────────────────────────────────────────────
    "vignette":        "vignette=PI/4",
    "vignette_strong": "vignette=PI/3",

    # ── Резкость / мягкость ───────────────────────────────────────────────────
    "sharpen": "unsharp=5:5:1.0",
    "soft":    "gblur=sigma=0.8",

    # ── Комбо-пресеты ─────────────────────────────────────────────────────────
    "cinematic": "eq=contrast=1.2:brightness=-0.03:saturation=1.1,vignette=PI/4",
    "dreamy":    "gblur=sigma=1.2,eq=brightness=0.06:saturation=0.8",
    "neon":      "eq=saturation=2.0:contrast=1.3,hue=h=15",
    "moody":     "eq=brightness=-0.08:saturation=0.9:contrast=1.2,vignette=PI/3",

    # ── Без фильтра (явное значение) ──────────────────────────────────────────
    "none": "",
}

# Публичный список доступных фильтров (для config.json аккаунта и для LLM-промпта)
AVAILABLE_FILTERS: List[str] = sorted(FILTER_REGISTRY.keys())


def get_filter(name: str) -> str:
    """
    Возвращает ffmpeg filter string по имени.

    Возвращает пустую строку если name == 'none', неизвестен или фильтр пустой.

    Args:
        name: имя фильтра (см. FILTER_REGISTRY) или 'none'

    Returns:
        ffmpeg filter chain fragment (без входного/выходного label)
    """
    return FILTER_REGISTRY.get(name, "")


def get_random_filter(exclude: Optional[List[str]] = None) -> str:
    """
    Возвращает имя случайного непустого фильтра.

    Args:
        exclude: список имён фильтров для исключения

    Returns:
        Имя фильтра (str). Если все исключены — возвращает 'none'.
    """
    excluded = set(exclude or []) | {"none"}
    candidates = [k for k in FILTER_REGISTRY if k not in excluded and FILTER_REGISTRY[k]]
    return random.choice(candidates) if candidates else "none"
