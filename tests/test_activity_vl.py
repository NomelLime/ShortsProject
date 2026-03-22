"""
tests/test_activity_vl.py — Тесты FIX#V3-1 (_sanitize_comment) и
FIX#V3-3 (_validate_vl_result) из Code Review v3 (19.03.2026).

Импортирует модуль напрямую через importlib, минуя pipeline/__init__.py.
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

# rebrowser_playwright — общая заглушка из tests/conftest.py (Page, sync_playwright).

# Мок pipeline.config
import types as _types
_mock_cfg = _types.ModuleType("pipeline.config")
_mock_cfg.OLLAMA_MODEL = "test"
_mock_cfg.VL_SCROLL_ROUNDS = 3
_mock_cfg.VL_FEED_SCREENSHOTS = 1
_mock_cfg.VL_LIKE_BUDGET_MIN = 1
_mock_cfg.VL_LIKE_BUDGET_MAX = 3
_mock_cfg.VL_COMMENT_CHANCE = 0.5
_mock_cfg.SP_ACCOUNTS_DIR = Path("/tmp")
_mock_cfg.ACTIVITY_DURATION_MIN_SEC = 30
_mock_cfg.ACTIVITY_DURATION_MAX_SEC = 120
_mock_cfg.WATCH_TIME_MIN_SEC = 5
_mock_cfg.WATCH_TIME_MAX_SEC = 30
_mock_cfg.CLICK_DELAY_MIN_SEC = 0.5
_mock_cfg.CLICK_DELAY_MAX_SEC = 2.0
_mock_cfg.PLATFORM_URLS = {}
_mock_cfg.CAPTCHA_WAIT_TIMEOUT_SEC = 60
sys.modules["pipeline.config"] = _mock_cfg

# Мок остальных зависимостей
for mod in ["pipeline.utils", "pipeline.notifications", "pipeline.ai",
            "pipeline.shared_gpu_lock", "pipeline.niche"]:
    m = _types.ModuleType(mod)
    m.human_sleep = lambda *a, **kw: None
    m.send_telegram_alert = lambda *a, **kw: None
    m.check_and_handle_captcha = lambda *a, **kw: False
    m.ollama_generate_with_timeout = lambda *a, **kw: {}
    m.acquire_gpu_lock = lambda **kw: __import__('contextlib').nullcontext()
    m.detect_and_cache_niche = lambda *a, **kw: "test"
    sys.modules[mod] = m

_vl = _load("activity_vl")

# Заглушки нужны только на время exec_module(activity_vl). Иначе остальные тесты
# получают пустые pipeline.utils / pipeline.notifications и падают.
for _mod in (
    "pipeline.config",
    "pipeline.utils",
    "pipeline.notifications",
    "pipeline.ai",
    "pipeline.shared_gpu_lock",
    "pipeline.niche",
):
    sys.modules.pop(_mod, None)


class TestSanitizeComment:
    """FIX#V3-1: VL comment injection prevention."""

    def test_removes_https_url(self):
        result = _vl._sanitize_comment("check this https://evil.com site")
        assert "https://" not in result
        assert "check this" in result

    def test_removes_http_url(self):
        result = _vl._sanitize_comment("visit http://spam.ru now")
        assert "http://" not in result

    def test_removes_www_url(self):
        result = _vl._sanitize_comment("go to www.spam.com")
        assert "www." not in result

    def test_removes_mentions(self):
        result = _vl._sanitize_comment("nice video @spambot @another")
        assert "@" not in result
        assert "nice video" in result

    def test_removes_html_tags(self):
        result = _vl._sanitize_comment("<script>alert(1)</script>hello")
        assert "<script>" not in result
        assert "hello" in result

    def test_removes_html_injection(self):
        result = _vl._sanitize_comment('<img src=x onerror=alert(1)>nice')
        assert "<img" not in result
        assert "nice" in result

    def test_short_comment_returns_empty(self):
        """Текст < 3 символов после очистки → пустая строка."""
        assert _vl._sanitize_comment("hi") == ""
        assert _vl._sanitize_comment("  ") == ""
        assert _vl._sanitize_comment("") == ""

    def test_max_length_enforced(self):
        result = _vl._sanitize_comment("a" * 200, max_len=100)
        assert len(result) == 100

    def test_normal_comment_passes_through(self):
        text = "great content love it"
        assert _vl._sanitize_comment(text) == text

    def test_collapses_whitespace(self):
        result = _vl._sanitize_comment("too   many    spaces")
        assert "  " not in result
        assert result == "too many spaces"


class TestValidateVlResult:
    """FIX#V3-3: VL JSON structure validation."""

    def test_valid_like_action_passes(self):
        raw = {"interactions": [{"action": "like", "rank": 1, "comment": ""}],
               "captcha_detected": False, "search_query": None}
        result = _vl._validate_vl_result(raw)
        assert result["interactions"][0]["action"] == "like"

    def test_invalid_action_replaced_with_skip(self):
        raw = {"interactions": [{"action": "delete_account", "rank": 1}]}
        result = _vl._validate_vl_result(raw)
        assert result["interactions"][0]["action"] == "skip"

    def test_rank_clamped_to_bounds(self):
        """rank ограничен диапазоном [1, 10]."""
        raw = {"interactions": [
            {"action": "like", "rank": 999},
            {"action": "like", "rank": -5},
        ]}
        result = _vl._validate_vl_result(raw)
        assert result["interactions"][0]["rank"] == 10
        assert result["interactions"][1]["rank"] == 1

    def test_max_5_interactions(self):
        """LLM не может вернуть более 5 действий."""
        raw = {"interactions": [{"action": "like", "rank": i} for i in range(1, 10)]}
        result = _vl._validate_vl_result(raw)
        assert len(result["interactions"]) == 5

    def test_comment_truncated_to_150(self):
        raw = {"interactions": [{"action": "comment", "rank": 1,
                                  "comment": "x" * 200}]}
        result = _vl._validate_vl_result(raw)
        assert len(result["interactions"][0]["comment"]) == 150

    def test_search_query_truncated_to_50(self):
        raw = {"search_query": "q" * 100, "interactions": []}
        result = _vl._validate_vl_result(raw)
        assert len(result["search_query"]) == 50

    def test_short_search_query_rejected(self):
        """search_query < 2 символов → None."""
        raw = {"search_query": "x", "interactions": []}
        result = _vl._validate_vl_result(raw)
        assert result["search_query"] is None

    def test_captcha_coerced_to_bool(self):
        raw = {"captcha_detected": 1, "interactions": []}
        result = _vl._validate_vl_result(raw)
        assert result["captcha_detected"] is True

    def test_non_list_interactions_returns_empty(self):
        raw = {"interactions": "like everything", "captcha_detected": False}
        result = _vl._validate_vl_result(raw)
        assert result["interactions"] == []

    def test_non_dict_items_in_interactions_skipped(self):
        raw = {"interactions": ["like", {"action": "like", "rank": 1}, None]}
        result = _vl._validate_vl_result(raw)
        assert len(result["interactions"]) == 1


class TestSanitizeCommentAndValidateIntegration:
    """Проверяем что _sanitize_comment и _validate_vl_result работают вместе."""

    def test_validate_preserves_comment_for_sanitize(self):
        """После validate comment идёт на sanitize — результат безопасен."""
        raw = {"interactions": [
            {"action": "comment", "rank": 1,
             "comment": "great video @check https://spam.com"}
        ]}
        validated = _vl._validate_vl_result(raw)
        comment = validated["interactions"][0]["comment"]
        sanitized = _vl._sanitize_comment(comment)
        assert "@" not in sanitized
        assert "https://" not in sanitized
