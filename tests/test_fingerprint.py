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

    def test_youtube_is_mobile(self):
        """YouTube — мобильный (единая mobile-стратегия)."""
        fp = _gen.generate_fingerprint(platform="youtube")
        assert fp["is_mobile"] is True
        assert fp["max_touch_points"] > 0
        assert "Mobile" in fp["user_agent"]

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
        # YouTube и TikTok — мобильные в текущей стратегии
        assert fp_yt["is_mobile"] is True
        assert fp_tt["is_mobile"] is True


class TestInjectorSafety:
    """FIX#1: Проверяет безопасное экранирование строковых fp-полей в JS-инъекции."""

    def test_safe_js_string_basic(self):
        """_safe_js_string возвращает корректный JSON string."""
        import importlib.util, sys
        from pathlib import Path
        _ROOT = Path(__file__).parent.parent
        p = _ROOT / "pipeline" / "fingerprint" / "injector.py"
        spec = importlib.util.spec_from_file_location("pipeline.fingerprint.injector", p)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)

        assert m._safe_js_string("Win32") == '"Win32"'
        # Результат должен быть валидным JSON-строкой
        import json
        assert json.loads(m._safe_js_string("Win32")) == "Win32"

    def test_safe_js_string_escapes_single_quote(self):
        import importlib.util, sys, json
        from pathlib import Path
        _ROOT = Path(__file__).parent.parent
        p = _ROOT / "pipeline" / "fingerprint" / "injector.py"
        spec = importlib.util.spec_from_file_location("pipeline.fingerprint.injector_q", p)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)

        result = m._safe_js_string("test'quote")
        assert json.loads(result) == "test'quote"

    def test_safe_js_string_blocks_js_injection(self):
        """Malicious payload экранируется — не вырывается из JS строки."""
        import importlib.util, sys, json
        from pathlib import Path
        _ROOT = Path(__file__).parent.parent
        p = _ROOT / "pipeline" / "fingerprint" / "injector.py"
        spec = importlib.util.spec_from_file_location("pipeline.fingerprint.injector_inj", p)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)

        malicious = "'); document.cookie; //"
        safe = m._safe_js_string(malicious)
        # Должен начинаться и заканчиваться кавычкой
        assert safe.startswith('"') and safe.endswith('"')
        # Последовательность '); экранирована внутри JSON строки (не разрывает контекст)
        # json.dumps("');...") → "'\');..." — одинарная кавычка НЕ экранируется json
        # Но двойные кавычки обёртывают — невозможно закрыть строку через '
        # Главное: результат парсится обратно в исходную строку
        assert json.loads(safe) == malicious
        # Должен быть валидным JSON
        assert json.loads(safe) == malicious

    def test_injector_no_raw_string_interp_in_webgl(self):
        """_inject_webgl не содержит ручного replace — использует _safe_js_string."""
        from pathlib import Path
        src = (Path(__file__).parent.parent / "pipeline" / "fingerprint" / "injector.py"
               ).read_text(encoding="utf-8")
        # Старый небезопасный паттерн не должен присутствовать
        assert ".replace(\"'\", \"\\\\'\")" not in src


class TestProfileLock:
    """FIX#2: Проверяет _profile_lock file locking."""

    def test_lock_acquired_when_no_contention(self, tmp_path):
        """Если профиль не занят — lock получен (yields True)."""
        import importlib.util, sys
        from pathlib import Path

        _ROOT = Path(__file__).parent.parent
        # Мокируем зависимости profile_manager перед загрузкой
        import types
        for mod in ["rebrowser_playwright", "rebrowser_playwright.sync_api"]:
            if mod not in sys.modules:
                m = types.ModuleType(mod)
                m.BrowserContext = object
                m.Page = object
                sys.modules[mod] = m

        p = _ROOT / "pipeline" / "profile_manager.py"
        spec = importlib.util.spec_from_file_location("pipeline.profile_manager_lock", p)
        pm = importlib.util.module_from_spec(spec)

        # Мок pipeline.ai
        ai_mock = types.ModuleType("pipeline.ai")
        ai_mock.OLLAMA_MODEL = "test"
        ai_mock.ollama_generate_with_timeout = lambda *a, **kw: {"response": "YES"}
        _prev_ai = sys.modules.pop("pipeline.ai", None)
        sys.modules["pipeline.ai"] = ai_mock
        try:
            spec.loader.exec_module(pm)

            profile_dir = tmp_path / "browser_profile"
            with pm._profile_lock(profile_dir) as acquired:
                # If portalocker is installed — lock file created; otherwise graceful fallback
                assert acquired is True
        finally:
            sys.modules.pop("pipeline.ai", None)
            if _prev_ai is not None:
                sys.modules["pipeline.ai"] = _prev_ai

    def test_lock_released_after_context(self, tmp_path):
        """Блокировка снимается после выхода из контекста."""
        import importlib.util, sys, types
        from pathlib import Path

        _ROOT = Path(__file__).parent.parent
        p = _ROOT / "pipeline" / "profile_manager.py"
        spec = importlib.util.spec_from_file_location("pipeline.profile_manager_lock2", p)
        pm = importlib.util.module_from_spec(spec)

        ai_mock = types.ModuleType("pipeline.ai")
        ai_mock.OLLAMA_MODEL = "test"
        ai_mock.ollama_generate_with_timeout = lambda *a, **kw: {"response": "YES"}
        _prev_ai = sys.modules.pop("pipeline.ai", None)
        sys.modules["pipeline.ai"] = ai_mock
        try:
            spec.loader.exec_module(pm)

            profile_dir = tmp_path / "profile2"
            with pm._profile_lock(profile_dir):
                pass  # снимается при выходе

            # Повторная блокировка должна сработать (предыдущая снята)
            with pm._profile_lock(profile_dir) as acquired2:
                assert acquired2 is True
        finally:
            sys.modules.pop("pipeline.ai", None)
            if _prev_ai is not None:
                sys.modules["pipeline.ai"] = _prev_ai
