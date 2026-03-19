"""
tests/test_fingerprint.py — Тесты генератора fingerprint (Сессия 12, ФИЧА 1).

Прямой импорт через importlib — минуя pipeline/__init__.py.
"""
from __future__ import annotations
import importlib.util, sys
from pathlib import Path
import pytest

_ROOT = Path(__file__).parent.parent

def _load(name: str):
    parts = name.split(".")
    path  = _ROOT / "pipeline" / Path(*parts).with_suffix(".py")
    fqn   = f"pipeline.{name}"
    spec  = importlib.util.spec_from_file_location(fqn, path)
    m     = importlib.util.module_from_spec(spec)
    sys.modules[fqn] = m
    spec.loader.exec_module(m)
    return m

# Загружаем нужные модули
_geo     = _load("fingerprint.geo")
_devices = _load("fingerprint.devices")

# Для generator нужны зависимости из geo и devices
sys.modules["pipeline.fingerprint.geo"]     = _geo
sys.modules["pipeline.fingerprint.devices"] = _devices
_gen = _load("fingerprint.generator")


class TestGenerateFingerprint:
    def test_all_required_fields_present(self):
        """Все ожидаемые поля присутствуют в профиле."""
        fp = _gen.generate_fingerprint(platform="youtube")
        required = [
            "fp_seed", "user_agent", "viewport", "screen", "platform_nav",
            "hardware_concurrency", "device_memory", "max_touch_points",
            "canvas_noise_seed", "webgl_vendor", "webgl_renderer",
            "webgl_unmasked_vendor", "webgl_unmasked_renderer",
            "fonts", "audio_context_noise", "timezone_id", "locale",
            "languages", "color_depth", "pixel_ratio", "is_mobile", "device_name",
        ]
        for field in required:
            assert field in fp, f"Поле '{field}' отсутствует"

    def test_youtube_is_desktop(self):
        """YouTube — всегда десктоп."""
        fp = _gen.generate_fingerprint(platform="youtube")
        assert fp["is_mobile"] is False
        assert fp["max_touch_points"] == 0
        assert "Mobile" not in fp["user_agent"]

    def test_tiktok_is_mobile(self):
        """TikTok — всегда мобильный."""
        fp = _gen.generate_fingerprint(platform="tiktok")
        assert fp["is_mobile"] is True
        assert fp["max_touch_points"] > 0
        assert "Mobile" in fp["user_agent"]
        assert "Android" in fp["user_agent"]

    def test_instagram_is_mobile(self):
        """Instagram — мобильный (Reels приоритет)."""
        fp = _gen.generate_fingerprint(platform="instagram")
        assert fp["is_mobile"] is True
        assert "Mobile" in fp["user_agent"]

    def test_geo_consistency_brazil(self):
        """country=BR → бразильские GEO-параметры."""
        fp = _gen.generate_fingerprint(platform="tiktok", country="BR")
        assert fp["timezone_id"] == "America/Sao_Paulo"
        assert fp["locale"] == "pt-BR"
        assert "pt-BR" in fp["languages"]

    def test_geo_consistency_germany(self):
        """country=DE → немецкие параметры."""
        fp = _gen.generate_fingerprint(platform="youtube", country="DE")
        assert fp["timezone_id"] == "Europe/Berlin"
        assert fp["locale"] == "de-DE"

    def test_geo_consistency_ukraine(self):
        """country=UA → украинские параметры."""
        fp = _gen.generate_fingerprint(platform="youtube", country="UA")
        assert fp["timezone_id"] == "Europe/Kiev"
        assert "uk-UA" in fp["languages"]

    def test_fingerprints_unique_by_seed(self):
        """Два разных seed → разные fingerprints."""
        fp1 = _gen.generate_fingerprint(platform="tiktok", seed="seed_aaa")
        fp2 = _gen.generate_fingerprint(platform="tiktok", seed="seed_bbb")
        assert fp1["canvas_noise_seed"] != fp2["canvas_noise_seed"]
        # Хотя бы одно из полей должно отличаться
        differs = any(
            fp1.get(k) != fp2.get(k)
            for k in ("canvas_noise_seed", "webgl_renderer", "audio_context_noise",
                      "hardware_concurrency", "device_memory")
        )
        assert differs

    def test_fingerprint_reproducible_by_seed(self):
        """Один и тот же seed → одинаковый fingerprint."""
        fp1 = _gen.generate_fingerprint(platform="tiktok", seed="fixed_seed_42")
        fp2 = _gen.generate_fingerprint(platform="tiktok", seed="fixed_seed_42")
        assert fp1["canvas_noise_seed"] == fp2["canvas_noise_seed"]
        assert fp1["user_agent"]        == fp2["user_agent"]
        assert fp1["webgl_renderer"]    == fp2["webgl_renderer"]

    def test_viewport_smaller_than_screen(self):
        """Viewport меньше экрана (панели браузера)."""
        fp = _gen.generate_fingerprint(platform="youtube")
        assert fp["viewport"]["width"]  <= fp["screen"]["width"]
        assert fp["viewport"]["height"] <= fp["screen"]["height"]

    def test_fonts_non_empty(self):
        """Список шрифтов непустой."""
        fp = _gen.generate_fingerprint(platform="youtube")
        assert isinstance(fp["fonts"], list)
        assert len(fp["fonts"]) >= 4
        assert "Arial" in fp["fonts"]  # базовый шрифт всегда присутствует

    def test_mobile_fewer_fonts_than_desktop(self):
        """Мобильные профили обычно имеют меньше шрифтов."""
        # Запускаем несколько раз для статистики
        mobile_counts  = [len(_gen.generate_fingerprint("tiktok", seed=f"m{i}")["fonts"]) for i in range(5)]
        desktop_counts = [len(_gen.generate_fingerprint("youtube", seed=f"d{i}")["fonts"]) for i in range(5)]
        assert max(mobile_counts) <= max(desktop_counts)

    def test_webgl_profiles_realistic(self):
        """WebGL vendor содержит реалистичные значения."""
        known_vendors = {"Google Inc. (NVIDIA)", "Google Inc. (AMD)", "Google Inc. (Intel)",
                         "Qualcomm", "ARM"}
        for platform in ("youtube", "tiktok", "instagram"):
            fp = _gen.generate_fingerprint(platform=platform)
            assert fp["webgl_vendor"] in known_vendors, \
                f"Нереалистичный WebGL vendor: {fp['webgl_vendor']}"


class TestEnsureFingerprint:
    def test_generates_if_absent(self):
        """ensure_fingerprint генерирует новый если нет в config."""
        config = {"platforms": ["tiktok"]}
        fp = _gen.ensure_fingerprint(config, "tiktok", "US")
        assert "fp_seed" in fp
        assert config["fingerprint"]["tiktok"] is fp

    def test_idempotent(self):
        """Повторный вызов возвращает тот же fingerprint."""
        config = {"platforms": ["tiktok"]}
        fp1 = _gen.ensure_fingerprint(config, "tiktok", "US")
        fp2 = _gen.ensure_fingerprint(config, "tiktok", "US")
        assert fp1["canvas_noise_seed"] == fp2["canvas_noise_seed"]
        assert fp1 is fp2  # тот же объект

    def test_per_platform_storage(self):
        """Fingerprint хранится per-platform."""
        config = {"platforms": ["youtube", "tiktok"]}
        fp_yt = _gen.ensure_fingerprint(config, "youtube", "US")
        fp_tt = _gen.ensure_fingerprint(config, "tiktok",  "US")
        # Должны быть разные объекты
        assert fp_yt is not fp_tt
        # YouTube — десктоп, TikTok — мобильный
        assert fp_yt["is_mobile"] is False
        assert fp_tt["is_mobile"] is True
