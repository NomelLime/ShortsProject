"""
tests/test_profile_manager.py — Тесты profile_manager (Сессия 12B).

Прямой импорт через importlib. Все платформенные вызовы замоканы.
"""
from __future__ import annotations
import importlib.util, sys, types
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import pytest

_ROOT = Path(__file__).parent.parent


def _load(name: str):
    p    = _ROOT / "pipeline" / f"{name}.py"
    fqn  = f"pipeline.{name}"
    spec = importlib.util.spec_from_file_location(fqn, p)
    m    = importlib.util.module_from_spec(spec)
    sys.modules[fqn] = m
    spec.loader.exec_module(m)
    return m


# Мок yt_dlp (нужен для pipeline.__init__ chain)
_ytdlp = types.ModuleType("yt_dlp")
_ytdlp.YoutubeDL = object
sys.modules["yt_dlp"] = _ytdlp

# Мок browser (launch_browser/close_browser)
_browser = types.ModuleType("pipeline.browser")
_browser.launch_browser = MagicMock(return_value=(MagicMock(), MagicMock()))
_browser.close_browser  = MagicMock()
sys.modules["pipeline.browser"] = _browser

# Мок rebrowser_playwright
_pw = types.ModuleType("rebrowser_playwright")
_pw.sync_api = types.ModuleType("rebrowser_playwright.sync_api")
_pw.sync_api.BrowserContext = object
_pw.sync_api.Page           = object
sys.modules["rebrowser_playwright"]          = _pw
sys.modules["rebrowser_playwright.sync_api"] = _pw.sync_api

# Мок pipeline.ai (полный — чтобы patch() не пытался импортировать pipeline.__init__)
_ai = types.ModuleType("pipeline.ai")
_ai.OLLAMA_MODEL                 = "test-model"
_ai.ollama_generate_with_timeout = MagicMock(return_value={"response": "YES"})
_ai.generate_video_metadata      = MagicMock(return_value=[])
_ai.check_ollama                 = MagicMock(return_value=True)
_ai.load_trending_hashtags       = MagicMock(return_value=[])
_ai.extract_frames               = MagicMock(return_value=[])
sys.modules["pipeline.ai"] = _ai

# Мок pipeline.utils (тянется из pipeline.__init__)
_utils = types.ModuleType("pipeline.utils")
_utils.get_all_accounts     = MagicMock(return_value=[])
_utils.get_upload_queue     = MagicMock(return_value=[])
_utils.mark_uploaded        = MagicMock()
_utils.get_uploads_today    = MagicMock(return_value=0)
_utils.increment_upload_count = MagicMock()
_utils.is_daily_limit_reached = MagicMock(return_value=False)
_utils.create_sample_account  = MagicMock()
_utils.get_logger             = MagicMock(return_value=MagicMock())
_utils.human_sleep            = MagicMock()
_utils.type_humanlike         = MagicMock()
_utils.probe_video            = MagicMock()
_utils.check_video_integrity  = MagicMock()
_utils.detect_encoder         = MagicMock()
_utils.get_random_asset       = MagicMock()
sys.modules["pipeline.utils"] = _utils

_pm = _load("profile_manager")


@pytest.fixture(autouse=True)
def reset_ai_mock():
    """Сбрасывает mock VL между тестами чтобы избежать state bleed."""
    _ai.ollama_generate_with_timeout.reset_mock()
    _ai.ollama_generate_with_timeout.return_value = {"response": "YES"}
    _ai.ollama_generate_with_timeout.side_effect  = None
    yield
    _ai.ollama_generate_with_timeout.reset_mock()


# ─────────────────────────────────────────────────────────────────────────────
# setup_profile_link
# ─────────────────────────────────────────────────────────────────────────────

class TestSetupProfileLink:
    def test_empty_url_returns_false(self):
        """Пустой prelend_url → False без открытия страницы."""
        ctx = MagicMock()
        result = _pm.setup_profile_link(ctx, "youtube", "")
        assert result is False
        ctx.new_page.assert_not_called()

    def test_unknown_platform_returns_false(self):
        """Неизвестная платформа → False."""
        ctx = MagicMock()
        result = _pm.setup_profile_link(ctx, "vkontakte", "https://example.com")
        assert result is False
        ctx.new_page.assert_not_called()

    def test_page_closed_on_success(self):
        """Страница закрывается даже при успешном выполнении."""
        ctx  = MagicMock()
        page = MagicMock()
        ctx.new_page.return_value = page

        # Инжектируем мок хендлер напрямую в registry
        orig = _pm._SETUP_HANDLERS.get("youtube")
        _pm._SETUP_HANDLERS["youtube"] = MagicMock(return_value=True)
        try:
            _pm.setup_profile_link(ctx, "youtube", "https://example.com")
        finally:
            _pm._SETUP_HANDLERS["youtube"] = orig

        page.close.assert_called_once()

    def test_page_closed_on_exception(self):
        """Страница закрывается даже при исключении."""
        ctx  = MagicMock()
        page = MagicMock()
        ctx.new_page.return_value = page

        orig = _pm._SETUP_HANDLERS.get("youtube")
        _pm._SETUP_HANDLERS["youtube"] = MagicMock(side_effect=RuntimeError("fail"))
        try:
            _pm.setup_profile_link(ctx, "youtube", "https://example.com")
        except Exception:
            pass
        finally:
            _pm._SETUP_HANDLERS["youtube"] = orig

        page.close.assert_called_once()

    def test_dispatches_to_correct_handler(self):
        """Диспетчер содержит хендлеры для всех платформ и вызывает правильный."""
        # Реестр содержит все платформы
        assert "youtube"   in _pm._SETUP_HANDLERS
        assert "tiktok"    in _pm._SETUP_HANDLERS
        assert "instagram" in _pm._SETUP_HANDLERS

        # Пустой url → False для всех платформ (без открытия страницы)
        ctx = MagicMock()
        assert _pm.setup_profile_link(ctx, "youtube",   "") is False
        assert _pm.setup_profile_link(ctx, "tiktok",    "") is False
        assert _pm.setup_profile_link(ctx, "instagram", "") is False
        ctx.new_page.assert_not_called()

        # Неизвестная платформа → False
        assert _pm.setup_profile_link(ctx, "snapchat", "https://x.com") is False


    def test_unknown_platform_returns_true(self):
        """Неизвестная платформа не блокирует — возвращает True."""
        ctx = MagicMock()
        result = _pm.verify_profile_link(ctx, "vkontakte", "https://example.com")
        assert result is True
        ctx.new_page.assert_not_called()

    def test_dispatches_to_youtube_handler(self):
        ctx  = MagicMock()
        page = MagicMock()
        ctx.new_page.return_value = page

        mock_h = MagicMock(return_value=True)
        orig = _pm._VERIFY_HANDLERS.get("youtube")
        _pm._VERIFY_HANDLERS["youtube"] = mock_h
        try:
            result = _pm.verify_profile_link(ctx, "youtube", "https://example.com")
            mock_h.assert_called_once_with(page, "https://example.com")
            assert result is True
        finally:
            _pm._VERIFY_HANDLERS["youtube"] = orig

    def test_dispatches_to_tiktok_handler(self):
        ctx  = MagicMock()
        page = MagicMock()
        ctx.new_page.return_value = page

        mock_h = MagicMock(return_value=False)
        orig = _pm._VERIFY_HANDLERS.get("tiktok")
        _pm._VERIFY_HANDLERS["tiktok"] = mock_h
        try:
            result = _pm.verify_profile_link(ctx, "tiktok", "https://example.com")
            assert result is False
        finally:
            _pm._VERIFY_HANDLERS["tiktok"] = orig


# ─────────────────────────────────────────────────────────────────────────────
# verify_all_links / setup_all_links
# ─────────────────────────────────────────────────────────────────────────────

class TestVerifyAllLinks:
    def test_empty_prelend_url_returns_empty_dict(self, tmp_path):
        """Нет prelend_url → пустой dict без запуска браузера."""
        cfg = {"platforms": ["youtube"]}
        result = _pm.verify_all_links(cfg, tmp_path)
        assert result == {}

    def test_returns_per_platform_dict(self, tmp_path):
        """verify_all_links возвращает dict per platform."""
        cfg = {"prelend_url": "https://example.com", "platforms": ["youtube", "instagram"]}
        pw_mock  = MagicMock()
        ctx_mock = MagicMock()
        page_mock = MagicMock()
        ctx_mock.new_page.return_value = page_mock

        # Патчим через sys.modules (browser уже в sys.modules через _browser)
        orig_yt = _pm._VERIFY_HANDLERS.get("youtube")
        orig_ig = _pm._VERIFY_HANDLERS.get("instagram")
        _pm._VERIFY_HANDLERS["youtube"]   = MagicMock(return_value=True)
        _pm._VERIFY_HANDLERS["instagram"] = MagicMock(return_value=False)
        _browser.launch_browser.return_value = (pw_mock, ctx_mock)
        try:
            result = _pm.verify_all_links(cfg, tmp_path)
        finally:
            _pm._VERIFY_HANDLERS["youtube"]   = orig_yt
            _pm._VERIFY_HANDLERS["instagram"] = orig_ig

        assert result == {"youtube": True, "instagram": False}

    def test_browser_error_returns_all_false(self, tmp_path):
        """Если браузер не запустился — все платформы False."""
        cfg = {"prelend_url": "https://example.com", "platforms": ["youtube", "tiktok"]}

        orig_lb = _browser.launch_browser
        _browser.launch_browser = MagicMock(side_effect=RuntimeError("proxy down"))
        try:
            result = _pm.verify_all_links(cfg, tmp_path)
        finally:
            _browser.launch_browser = orig_lb

        assert result == {"youtube": False, "tiktok": False}


class TestSetupAllLinks:
    def test_empty_prelend_url_returns_empty_dict(self, tmp_path):
        cfg = {"platforms": ["youtube"]}
        result = _pm.setup_all_links(cfg, tmp_path)
        assert result == {}

    def test_per_platform_bio_used(self, tmp_path):
        """Bio per-platform (bio_text_tiktok) передаётся в хендлер."""
        cfg = {
            "prelend_url":        "https://example.com",
            "platforms":          ["tiktok"],
            "bio_text":           "general bio",
            "bio_text_tiktok":    "tiktok bio",
        }
        pw_mock  = MagicMock()
        ctx_mock = MagicMock()
        page_mock = MagicMock()
        ctx_mock.new_page.return_value = page_mock

        mock_handler = MagicMock(return_value=True)
        orig = _pm._SETUP_HANDLERS.get("tiktok")
        _pm._SETUP_HANDLERS["tiktok"] = mock_handler
        _browser.launch_browser.return_value = (pw_mock, ctx_mock)
        try:
            _pm.setup_all_links(cfg, tmp_path)
        finally:
            _pm._SETUP_HANDLERS["tiktok"] = orig

        mock_handler.assert_called_once_with(page_mock, "https://example.com", "tiktok bio")

    def test_browser_closed_on_success(self, tmp_path):
        """close_browser вызывается даже при успехе."""
        cfg = {"prelend_url": "https://example.com", "platforms": ["instagram"]}
        pw_mock  = MagicMock()
        ctx_mock = MagicMock()
        ctx_mock.new_page.return_value = MagicMock()

        mock_handler = MagicMock(return_value=True)
        orig = _pm._SETUP_HANDLERS.get("instagram")
        _pm._SETUP_HANDLERS["instagram"] = mock_handler
        _browser.launch_browser.return_value = (pw_mock, ctx_mock)
        _browser.close_browser.reset_mock()
        try:
            _pm.setup_all_links(cfg, tmp_path)
        finally:
            _pm._SETUP_HANDLERS["instagram"] = orig

        _browser.close_browser.assert_called_once_with(pw_mock, ctx_mock)


# ─────────────────────────────────────────────────────────────────────────────
# VL-fallback: _find_element_with_fallback
# ─────────────────────────────────────────────────────────────────────────────

class TestFindElementWithFallback:
    def test_css_hit_no_vl_call(self):
        """Если CSS нашёл — VL не вызывается."""
        page   = MagicMock()
        mock_el = MagicMock()
        mock_el.is_visible.return_value = True
        page.locator.return_value.first = mock_el

        result = _pm._find_element_with_fallback(
            page,
            css_selectors=["input#test"],
            vl_prompt="find field",
            description="test",
        )

        assert result is mock_el
        # screenshot не вызывался
        page.screenshot.assert_not_called()

    def test_css_miss_calls_screenshot(self):
        """CSS не нашёл → делается скриншот."""
        page = MagicMock()
        page.locator.return_value.first.is_visible.return_value = False
        page.screenshot.return_value = b"jpeg"
        _ai.ollama_generate_with_timeout.return_value = {"response": "NOT_FOUND"}

        _pm._find_element_with_fallback(
            page,
            css_selectors=["#nope"],
            vl_prompt="test",
            description="test_field",
        )

        page.screenshot.assert_called_once()

    def test_vl_not_found_returns_none(self):
        """VL ответил NOT_FOUND → None."""
        page = MagicMock()
        page.locator.return_value.first.is_visible.return_value = False
        page.screenshot.return_value = b"jpeg"
        _ai.ollama_generate_with_timeout.return_value = {"response": "NOT_FOUND"}

        result = _pm._find_element_with_fallback(
            page, css_selectors=["#nope"], vl_prompt="test", description="x",
        )
        assert result is None

    def test_vl_returns_coordinates_clicks(self):
        """VL вернул координаты → mouse.click вызван."""
        page = MagicMock()
        page.locator.return_value.first.is_visible.return_value = False
        page.screenshot.return_value = b"jpeg"
        # Убеждаемся что наш мок ai актуален (test_activity_vl может перезаписать sys.modules)
        sys.modules["pipeline.ai"] = _ai
        _ai.ollama_generate_with_timeout.return_value = {"response": "250, 400"}

        focus_mock = MagicMock()
        # locator(":focus") возвращает focus_mock
        def loc_side(sel):
            m = MagicMock()
            m.first.is_visible.return_value = False
            if sel == ":focus":
                m.first = focus_mock
            return m
        page.locator.side_effect = loc_side

        result = _pm._find_element_with_fallback(
            page, css_selectors=["#nope"], vl_prompt="test", description="x",
        )

        page.mouse.click.assert_called_once_with(250, 400)
        assert result is focus_mock

    def test_vl_error_returns_none_graceful(self):
        """Если Ollama упал → None без краша пайплайна."""
        page = MagicMock()
        page.locator.return_value.first.is_visible.return_value = False
        page.screenshot.side_effect = Exception("GPU busy")

        result = _pm._find_element_with_fallback(
            page, css_selectors=["#nope"], vl_prompt="test", description="x",
        )
        assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# _verify_page_context
# ─────────────────────────────────────────────────────────────────────────────

class TestVerifyPageContext:
    def test_vl_yes_returns_true(self):
        page = MagicMock()
        page.screenshot.return_value = b"jpeg"
        _ai.ollama_generate_with_timeout.return_value = {"response": "YES"}
        assert _pm._verify_page_context(page, "Instagram", "edit profile") is True

    def test_vl_no_returns_false(self):
        page = MagicMock()
        page.screenshot.return_value = b"jpeg"
        _ai.ollama_generate_with_timeout.return_value = {"response": "NO, wrong page"}
        assert _pm._verify_page_context(page, "Instagram", "edit profile") is False

    def test_vl_error_returns_true_graceful(self):
        """Если VL упал → True (graceful, не блокируем CSS-only режим)."""
        page = MagicMock()
        page.screenshot.side_effect = Exception("GPU busy")
        assert _pm._verify_page_context(page, "Instagram", "edit profile") is True

    def test_vl_yes_case_insensitive(self):
        page = MagicMock()
        page.screenshot.return_value = b"jpeg"
        _ai.ollama_generate_with_timeout.return_value = {"response": "yes, this is the edit profile page"}
        assert _pm._verify_page_context(page, "TikTok", "edit profile") is True
