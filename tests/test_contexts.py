"""
tests/test_contexts.py — Тесты платформенных контекстов (Сессия 12, ФИЧА 3).
"""
from __future__ import annotations
import importlib.util, sys, types
from pathlib import Path
import pytest

_ROOT = Path(__file__).parent.parent

def _load(name: str):
    parts = name.split(".")
    p     = _ROOT / "pipeline" / Path(*parts).with_suffix(".py")
    fqn   = f"pipeline.{name}"
    spec  = importlib.util.spec_from_file_location(fqn, p)
    m     = importlib.util.module_from_spec(spec)
    sys.modules[fqn] = m
    spec.loader.exec_module(m)
    return m

# Загружаем зависимости цепочкой
_geo = _load("fingerprint.geo")
_dev = _load("fingerprint.devices")
sys.modules["pipeline.fingerprint.geo"]     = _geo
sys.modules["pipeline.fingerprint.devices"] = _dev
_gen = _load("fingerprint.generator")
sys.modules["pipeline.fingerprint.generator"] = _gen

# Мок injector (не нужен BrowserContext в unit-тестах)
_inj = types.ModuleType("pipeline.fingerprint.injector")
_inj.apply_fingerprint = lambda ctx, fp: None
sys.modules["pipeline.fingerprint.injector"] = _inj

# Мок playwright_stealth
_stealth_mod = types.ModuleType("playwright_stealth")
class _MockStealth:
    def apply_stealth_sync(self, page): pass
_stealth_mod.Stealth = _MockStealth
sys.modules["playwright_stealth"] = _stealth_mod

# rebrowser_playwright — tests/conftest.py (не перезаписывать: нужны Page и sync_playwright).

_base = _load("contexts.base")
sys.modules["pipeline.contexts.base"] = _base
_yt   = _load("contexts.youtube")
_tt   = _load("contexts.tiktok")
_ig   = _load("contexts.instagram")


def _make_fp(platform="youtube", country="US"):
    """Создаёт тестовый fingerprint через генератор."""
    return _gen.generate_fingerprint(platform=platform, country=country, seed="test_seed")


class TestYouTubeContext:
    def setup_method(self):
        self.ctx = _yt.YouTubeContext()
        self.fp  = _make_fp("youtube")

    def test_platform_name(self):
        assert self.ctx.platform_name == "youtube"

    def test_desktop_mode(self):
        kwargs = self.ctx.build_launch_kwargs(Path("/tmp"), self.fp, None)
        assert kwargs["is_mobile"] is False
        assert kwargs["has_touch"] is False

    def test_user_agent_in_kwargs(self):
        kwargs = self.ctx.build_launch_kwargs(Path("/tmp"), self.fp, None)
        assert kwargs["user_agent"] == self.fp["user_agent"]

    def test_proxy_added_when_provided(self):
        proxy = {"server": "http://1.2.3.4:8080"}
        kwargs = self.ctx.build_launch_kwargs(Path("/tmp"), self.fp, proxy)
        assert kwargs["proxy"] == proxy

    def test_no_proxy_when_none(self):
        kwargs = self.ctx.build_launch_kwargs(Path("/tmp"), self.fp, None)
        assert "proxy" not in kwargs

    def test_login_url_google(self):
        assert "google.com" in self.ctx.get_login_url()

    def test_session_check_url_studio(self):
        assert "studio.youtube.com" in self.ctx.get_session_check_url()


class TestTikTokContext:
    def setup_method(self):
        self.ctx = _tt.TikTokContext()
        self.fp  = _make_fp("tiktok")

    def test_platform_name(self):
        assert self.ctx.platform_name == "tiktok"

    def test_is_mobile(self):
        kwargs = self.ctx.build_launch_kwargs(Path("/tmp"), self.fp, None)
        assert kwargs["is_mobile"] is True
        assert kwargs["has_touch"] is True

    def test_device_scale_factor_from_fp(self):
        kwargs = self.ctx.build_launch_kwargs(Path("/tmp"), self.fp, None)
        assert kwargs["device_scale_factor"] == self.fp["pixel_ratio"]

    def test_timezone_from_fp(self):
        kwargs = self.ctx.build_launch_kwargs(Path("/tmp"), self.fp, None)
        assert kwargs["timezone_id"] == self.fp["timezone_id"]

    def test_login_url_tiktok(self):
        assert "tiktok.com" in self.ctx.get_login_url()

    def test_redirect_markers_non_empty(self):
        assert len(self.ctx.get_redirect_markers()) > 0


class TestInstagramContext:
    def setup_method(self):
        self.ctx = _ig.InstagramContext()
        self.fp  = _make_fp("instagram")

    def test_platform_name(self):
        assert self.ctx.platform_name == "instagram"

    def test_is_mobile(self):
        kwargs = self.ctx.build_launch_kwargs(Path("/tmp"), self.fp, None)
        assert kwargs["is_mobile"] is True
        assert kwargs["has_touch"] is True

    def test_login_url_instagram(self):
        assert "instagram.com" in self.ctx.get_login_url()


class TestPlatformSpecificFingerprints:
    def test_tiktok_and_youtube_are_mobile(self):
        """TikTok и YouTube используют мобильные fingerprint-профили."""
        fp_yt = _make_fp("youtube")
        fp_tt = _make_fp("tiktok")
        assert fp_tt["is_mobile"] is True
        assert fp_yt["is_mobile"] is True

    def test_tiktok_webgl_from_mobile_pool(self):
        """TikTok использует мобильные WebGL профили."""
        fp = _make_fp("tiktok")
        mobile_vendors = {"Qualcomm", "ARM"}
        assert fp["webgl_vendor"] in mobile_vendors

    def test_youtube_webgl_from_mobile_pool(self):
        """YouTube использует мобильные WebGL профили (единая mobile-стратегия)."""
        fp = _make_fp("youtube")
        mobile_vendors = {"Qualcomm", "ARM"}
        assert fp["webgl_vendor"] in mobile_vendors
