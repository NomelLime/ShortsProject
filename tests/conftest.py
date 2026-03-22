"""
tests/conftest.py
Общие фикстуры и настройка окружения для тестов.
"""

import importlib
import sys
import types
from pathlib import Path

# Добавляем корень проекта в sys.path чтобы импорты pipeline.* работали
ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Не выполнять pipeline/__init__.py (yt-dlp, downloader, …): подмодули грузятся по запросу.
if "pipeline" not in sys.modules:
    _pipeline_pkg = types.ModuleType("pipeline")
    _pipeline_pkg.__path__ = [str(ROOT / "pipeline")]
    sys.modules["pipeline"] = _pipeline_pkg

# CI / минимальное окружение: pipeline.utils тянет rebrowser_playwright
try:
    import rebrowser_playwright.sync_api  # noqa: F401
except ImportError:
    _pw = types.ModuleType("rebrowser_playwright")
    _pw.sync_api = types.ModuleType("rebrowser_playwright.sync_api")
    _pw.sync_api.BrowserContext = object
    _pw.sync_api.Page = object
    _pw.sync_api.Playwright = object

    class _DummySyncPlaywrightCM:
        def __enter__(self):
            return (object(), object())

        def __exit__(self, *args):
            return None

    def _sync_playwright():
        return _DummySyncPlaywrightCM()

    _pw.sync_api.sync_playwright = _sync_playwright
    sys.modules["rebrowser_playwright"] = _pw
    sys.modules["rebrowser_playwright.sync_api"] = _pw.sync_api

try:
    import playwright_stealth  # noqa: F401
except ImportError:
    _pls = types.ModuleType("playwright_stealth")

    class Stealth:
        def __init__(self, *args, **kwargs):
            pass

        def apply_stealth_sync(self, *args, **kwargs):
            pass

    _pls.Stealth = Stealth
    sys.modules["playwright_stealth"] = _pls

try:
    import requests  # noqa: F401
except ImportError:
    _rq = types.ModuleType("requests")
    _rq_exc = types.ModuleType("requests.exceptions")

    class RequestException(Exception):
        pass

    class ConnectionError(RequestException):
        pass

    _rq_exc.RequestException = RequestException
    _rq_exc.ConnectionError = ConnectionError
    _rq.exceptions = _rq_exc
    _rq.get = lambda *a, **k: None
    _rq.post = lambda *a, **k: None
    sys.modules["requests"] = _rq
    sys.modules["requests.exceptions"] = _rq_exc

try:
    import cv2  # noqa: F401
except ImportError:
    sys.modules["cv2"] = types.ModuleType("cv2")

try:
    import ollama  # noqa: F401
except ImportError:
    sys.modules["ollama"] = types.ModuleType("ollama")

try:
    import tqdm  # noqa: F401
except ImportError:
    _tqdm_mod = types.ModuleType("tqdm")

    def _tqdm_iter(it, *args, **kwargs):
        return it

    _tqdm_mod.tqdm = _tqdm_iter
    sys.modules["tqdm"] = _tqdm_mod

try:
    import yt_dlp  # noqa: F401
except ImportError:
    _ytd = types.ModuleType("yt_dlp")

    class _FakeYoutubeDL:
        pass

    _ytd.YoutubeDL = _FakeYoutubeDL
    sys.modules["yt_dlp"] = _ytd

# patch("pipeline.quarantine.…") — подмодуль должен быть в sys.modules и на пакете pipeline
for _sub in ("pipeline.quarantine", "pipeline.upload_warmup"):
    importlib.import_module(_sub)

try:
    importlib.import_module("pipeline.downloader")
except ImportError:
    pass
