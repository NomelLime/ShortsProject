"""
tests/test_video_filters.py — Тесты библиотеки видеофильтров (Сессия 11, ФИЧА 3).
"""
from __future__ import annotations
import importlib.util, sys
from pathlib import Path
import pytest

_ROOT = Path(__file__).parent.parent

def _load(name: str):
    p = _ROOT / "pipeline" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"pipeline.{name}", p)
    m = importlib.util.module_from_spec(spec)
    sys.modules[f"pipeline.{name}"] = m
    spec.loader.exec_module(m)
    return m

_vf = _load("video_filters")


class TestFilterRegistry:
    def test_registry_not_empty(self):
        assert len(_vf.FILTER_REGISTRY) > 0

    def test_none_returns_empty_string(self):
        assert _vf.get_filter("none") == ""

    def test_unknown_returns_empty_string(self):
        assert _vf.get_filter("nonexistent_xyz") == ""

    def test_known_filter_nonempty(self):
        assert _vf.get_filter("cinematic") != ""
        assert _vf.get_filter("warm") != ""
        assert _vf.get_filter("vhs") != ""

    def test_all_filters_no_semicolon(self):
        for name, fstr in _vf.FILTER_REGISTRY.items():
            if fstr:
                assert ";" not in fstr, f"Фильтр '{name}' содержит ';'"

    def test_available_filters_includes_none(self):
        assert "none" in _vf.AVAILABLE_FILTERS

    def test_available_filters_sorted(self):
        assert _vf.AVAILABLE_FILTERS == sorted(_vf.AVAILABLE_FILTERS)

    def test_required_filters_present(self):
        required = {"cinematic","warm","cold","vibrant","muted","vhs",
                    "sepia","grayscale","vintage","moody","dreamy"}
        for name in required:
            assert name in _vf.FILTER_REGISTRY, f"Фильтр '{name}' отсутствует"


class TestGetFilter:
    def test_cinematic_eq(self):     assert "eq=" in _vf.get_filter("cinematic")
    def test_cinematic_vignette(self): assert "vignette" in _vf.get_filter("cinematic")
    def test_grayscale(self):        assert "hue=s=0" in _vf.get_filter("grayscale")
    def test_vhs_noise(self):        assert "noise=" in _vf.get_filter("vhs")
    def test_sepia(self):            assert "colorchannelmixer" in _vf.get_filter("sepia")


class TestGetRandomFilter:
    def test_not_none(self):
        for _ in range(20):
            name = _vf.get_random_filter()
            assert name != "none"
            assert _vf.FILTER_REGISTRY.get(name, "") != ""

    def test_respects_exclude(self):
        all_except = [k for k in _vf.FILTER_REGISTRY if k not in ("none","cinematic")]
        for _ in range(20):
            assert _vf.get_random_filter(exclude=all_except) == "cinematic"

    def test_all_excluded_returns_none(self):
        assert _vf.get_random_filter(exclude=list(_vf.FILTER_REGISTRY.keys())) == "none"
