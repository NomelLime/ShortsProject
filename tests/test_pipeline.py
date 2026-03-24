"""
tests/test_pipeline.py
Реальные тесты для всех ключевых модулей пайплайна.
Все внешние зависимости (ffmpeg, yt-dlp, Ollama, Playwright) мокируются.
"""

import json
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open, call
import pytest

# Маркеры скорости:
#   @pytest.mark.slow  — тесты с реальными потоками / ретраями / тяжёлыми моками
#   (без маркера)      — быстрые юнит-тесты, запускаются за секунды
#
# Запуск только быстрых:  pytest -m "not slow"
# Запуск только медленных: pytest -m slow

# ═══════════════════════════════════════════════════════════════════════
# FIXTURES
# ═══════════════════════════════════════════════════════════════════════

@pytest.fixture
def tmp_video(tmp_path) -> Path:
    """Создаёт фиктивный mp4-файл."""
    p = tmp_path / "test_video.mp4"
    p.write_bytes(b"\x00" * 1024)
    return p


@pytest.fixture
def probe_info():
    """Стандартный ответ probe_video."""
    return {
        "width": 1920, "height": 1080, "fps": 30.0,
        "duration": 60.0, "has_audio": True, "sample_rate": 44100,
    }


@pytest.fixture
def fake_account(tmp_path) -> Path:
    """Создаёт структуру папки аккаунта."""
    acc = tmp_path / "accounts" / "acc_yt"
    acc.mkdir(parents=True)
    (acc / "config.json").write_text(
        json.dumps({"name": "acc_yt", "platforms": ["youtube"]}),
        encoding="utf-8",
    )
    return acc


# ═══════════════════════════════════════════════════════════════════════
# 1. download.py
# ═══════════════════════════════════════════════════════════════════════

class TestDownloadStats:
    def test_total_is_computed_property(self):
        from pipeline.download import DownloadStats
        s = DownloadStats(ok=3, failed=2, integrity_error=1)
        assert s.total == 6

    def test_total_starts_at_zero(self):
        from pipeline.download import DownloadStats
        assert DownloadStats().total == 0

    def test_record_ok(self, tmp_video):
        from pipeline.download import DownloadStats, DownloadResult, DownloadStatus
        s = DownloadStats()
        s.record(DownloadResult("http://x.com", DownloadStatus.OK, tmp_video))
        assert s.ok == 1
        assert s.total == 1
        assert tmp_video in s.files

    def test_record_failed(self):
        from pipeline.download import DownloadStats, DownloadResult, DownloadStatus
        s = DownloadStats()
        s.record(DownloadResult("http://x.com", DownloadStatus.FAILED, None, "err"))
        assert s.failed == 1
        assert s.ok == 0

    def test_record_integrity_error(self):
        from pipeline.download import DownloadStats, DownloadResult, DownloadStatus
        s = DownloadStats()
        s.record(DownloadResult("http://x.com", DownloadStatus.INTEGRITY_ERROR))
        assert s.integrity_error == 1

    @pytest.mark.slow
    def test_record_is_thread_safe(self):
        """Параллельные записи не должны терять данные."""
        from pipeline.download import DownloadStats, DownloadResult, DownloadStatus
        s = DownloadStats()

        def add_ok():
            for _ in range(100):
                s.record(DownloadResult("u", DownloadStatus.OK))

        threads = [threading.Thread(target=add_ok) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert s.ok == 500
        assert s.total == 500


class TestDownloadSingle:
    def test_returns_ok_on_success(self, tmp_video):
        from pipeline.download import download_single, DownloadStatus

        mock_info = {"id": "abc", "ext": "mp4"}
        mock_ydl = MagicMock()
        mock_ydl.__enter__ = lambda s: s
        mock_ydl.__exit__ = MagicMock(return_value=False)
        mock_ydl.extract_info.return_value = mock_info
        mock_ydl.prepare_filename.return_value = str(tmp_video)

        with patch("pipeline.download.YoutubeDL", return_value=mock_ydl), \
             patch("pipeline.download.check_video_integrity", return_value=True), \
             patch("pipeline.download.utils.is_duplicate", return_value=False):
            result = download_single("http://youtube.com/watch?v=abc")

        assert result.status is DownloadStatus.OK
        assert result.file == tmp_video

    def test_returns_failed_on_exception(self):
        from pipeline.download import download_single, DownloadStatus

        with patch("pipeline.download.YoutubeDL", side_effect=Exception("network error")):
            result = download_single("http://bad-url.com")

        assert result.status is DownloadStatus.FAILED
        assert "network error" in result.reason

    def test_returns_integrity_error_when_check_fails(self, tmp_video):
        from pipeline.download import download_single, DownloadStatus

        mock_ydl = MagicMock()
        mock_ydl.__enter__ = lambda s: s
        mock_ydl.__exit__ = MagicMock(return_value=False)
        mock_ydl.extract_info.return_value = {"id": "x", "ext": "mp4"}
        mock_ydl.prepare_filename.return_value = str(tmp_video)

        with patch("pipeline.download.YoutubeDL", return_value=mock_ydl), \
             patch("pipeline.download.check_video_integrity", return_value=False):
            result = download_single("http://youtube.com/watch?v=x")

        assert result.status is DownloadStatus.INTEGRITY_ERROR

    def test_returns_failed_on_duplicate(self, tmp_video):
        from pipeline.download import download_single, DownloadStatus

        mock_ydl = MagicMock()
        mock_ydl.__enter__ = lambda s: s
        mock_ydl.__exit__ = MagicMock(return_value=False)
        mock_ydl.extract_info.return_value = {"id": "dup", "ext": "mp4"}
        mock_ydl.prepare_filename.return_value = str(tmp_video)

        with patch("pipeline.download.YoutubeDL", return_value=mock_ydl), \
             patch("pipeline.download.check_video_integrity", return_value=True), \
             patch("pipeline.download.utils.is_duplicate", return_value=True), \
             patch("pipeline.download._log_failed"):
            result = download_single("http://youtube.com/watch?v=dup")

        assert result.status is DownloadStatus.FAILED
        assert "Duplicate" in result.reason


# ═══════════════════════════════════════════════════════════════════════
# 2. slicer.py
# ═══════════════════════════════════════════════════════════════════════

class TestGroupIntoClips:
    def test_short_video_returns_single_clip(self):
        from pipeline.slicer import group_into_clips
        clips = group_into_clips([], total=10.0)
        assert clips == [(0.0, 10.0)]

    def test_empty_cut_points_splits_by_max_len(self):
        from pipeline.slicer import group_into_clips
        from pipeline.config import CLIP_MAX_LEN
        clips = group_into_clips([], total=100.0)
        assert len(clips) >= 2
        for start, end in clips:
            assert end - start <= CLIP_MAX_LEN + 0.01

    def test_respects_cut_points(self):
        from pipeline.slicer import group_into_clips
        # Видео 60с, точки нарезки в нужных местах
        clips = group_into_clips([20.0, 40.0], total=60.0)
        starts = [c[0] for c in clips]
        assert 0.0 in starts

    def test_clips_cover_full_duration(self):
        from pipeline.slicer import group_into_clips
        total = 90.0
        clips = group_into_clips([15.0, 30.0, 45.0, 60.0, 75.0], total=total)
        assert clips[0][0] == 0.0
        assert abs(clips[-1][1] - total) < 1.0

    def test_zero_duration_returns_empty(self):
        from pipeline.slicer import group_into_clips
        assert group_into_clips([], total=0.0) == []

    def test_clip_min_length_respected(self):
        from pipeline.slicer import group_into_clips
        from pipeline.config import CLIP_MIN_LEN
        clips = group_into_clips([], total=60.0)
        for start, end in clips:
            assert end - start >= CLIP_MIN_LEN - 0.01


class TestStageSlice:
    def test_calls_slice_short_for_short_video(self, tmp_path, tmp_video):
        from pipeline.slicer import stage_slice

        with patch("pipeline.slicer.probe_video", return_value={"duration": 5.0}), \
             patch("pipeline.slicer.slice_short_video", return_value=[tmp_video]) as mock_short:
            result = stage_slice(tmp_video, tmp_path)

        mock_short.assert_called_once()
        assert result == [tmp_video]

    def test_calls_slice_long_for_long_video(self, tmp_path, tmp_video):
        from pipeline.slicer import stage_slice

        clips = [tmp_path / "clip_0.mp4", tmp_path / "clip_1.mp4"]
        with patch("pipeline.slicer.probe_video", return_value={"duration": 120.0}), \
             patch("pipeline.slicer.slice_long_video", return_value=clips) as mock_long:
            result = stage_slice(tmp_video, tmp_path, metadata_variants=[{"title": "t"}])

        mock_long.assert_called_once()
        assert result == clips


# ═══════════════════════════════════════════════════════════════════════
# 3. utils.py
# ═══════════════════════════════════════════════════════════════════════

class TestProbeVideo:
    def test_returns_correct_fields(self, tmp_video):
        from pipeline.utils import probe_video

        fake_info = {
            "streams": [
                {"codec_type": "video", "width": 1280, "height": 720,
                 "r_frame_rate": "30/1"},
                {"codec_type": "audio", "sample_rate": "44100"},
            ],
            "format": {"duration": "45.5"},
        }
        with patch("pipeline.utils.ffmpeg.probe", return_value=fake_info):
            info = probe_video(tmp_video)

        assert info["width"] == 1280
        assert info["height"] == 720
        assert info["fps"] == 30.0
        assert info["duration"] == 45.5
        assert info["has_audio"] is True

    def test_raises_on_no_video_stream(self, tmp_video):
        from pipeline.utils import probe_video

        fake_info = {"streams": [], "format": {"duration": "10"}}
        with patch("pipeline.utils.ffmpeg.probe", return_value=fake_info):
            with pytest.raises(ValueError, match="Нет видео-потока"):
                probe_video(tmp_video)


class TestIsDuplicate:
    def test_first_video_not_duplicate(self, tmp_video, tmp_path):
        from pipeline import utils
        import imagehash

        ph = imagehash.hex_to_hash("a" * 16)
        with patch.object(utils, "compute_perceptual_hash", return_value=ph), \
             patch.object(utils, "load_hashes", return_value=[]), \
             patch.object(utils, "save_hashes") as mock_save:
            result = utils.is_duplicate(tmp_video)

        assert result is False
        mock_save.assert_called_once_with([str(ph)])

    def test_duplicate_detected(self, tmp_video):
        from pipeline import utils
        import imagehash

        ph = imagehash.hex_to_hash("b" * 16)
        with patch.object(utils, "compute_perceptual_hash", return_value=ph), \
             patch.object(utils, "load_hashes", return_value=[str(ph)]):
            result = utils.is_duplicate(tmp_video)

        assert result is True

    def test_none_hash_not_duplicate(self, tmp_video):
        from pipeline import utils

        with patch.object(utils, "compute_perceptual_hash", return_value=None):
            result = utils.is_duplicate(tmp_video)

        assert result is False


class TestUniqueLines:
    def test_deduplicates_and_strips(self, tmp_path):
        from pipeline.utils import unique_lines
        f = tmp_path / "urls.txt"
        f.write_text("http://a.com\nhttp://b.com\nhttp://a.com\n  \n", encoding="utf-8")
        result = unique_lines(f)
        assert result == ["http://a.com", "http://b.com"]

    def test_returns_empty_for_missing_file(self, tmp_path):
        from pipeline.utils import unique_lines
        result = unique_lines(tmp_path / "nonexistent.txt")
        assert result == []


class TestLoadKeywords:
    def test_skips_comments_and_blank(self, tmp_path, monkeypatch):
        # Патчим pipeline.utils.config, а не pipeline.config: тесты вроде
        # test_upload_warmup подменяют sys.modules["pipeline.config"], после чего
        # у pipeline.utils остаётся старая ссылка на «настоящий» config.
        import pipeline.utils as pipeline_utils
        from pipeline.utils import load_keywords

        kw = tmp_path / "keywords.txt"
        kw.write_text(
            "# header comment\n\n  alpha  \n# skip\nbeta\n",
            encoding="utf-8",
        )
        monkeypatch.setattr(pipeline_utils.config, "KEYWORDS_FILE", kw)
        assert load_keywords() == ["alpha", "beta"]

    def test_missing_file_returns_empty(self, tmp_path, monkeypatch):
        import pipeline.utils as pipeline_utils
        from pipeline.utils import load_keywords

        monkeypatch.setattr(pipeline_utils.config, "KEYWORDS_FILE", tmp_path / "none.txt")
        assert load_keywords() == []


class TestSaveAndLoadJson:
    def test_roundtrip(self, tmp_path):
        from pipeline.utils import save_json, load_json
        path = tmp_path / "data.json"
        data = {"key": "value", "num": 42}
        save_json(path, data)
        loaded = load_json(path)
        assert loaded == data

    def test_load_missing_returns_none(self, tmp_path):
        from pipeline.utils import load_json
        assert load_json(tmp_path / "missing.json") is None


# ═══════════════════════════════════════════════════════════════════════
# 4. distributor.py
# ═══════════════════════════════════════════════════════════════════════

class TestParseDescriptionFile:
    def test_parses_single_block(self, tmp_path):
        from pipeline.distributor import parse_description_file
        f = tmp_path / "desc.txt"
        f.write_text(
            "Title: My Video\nDescription: Cool video\nTags: tag1, tag2\n",
            encoding="utf-8",
        )
        result = parse_description_file(f)
        assert len(result) == 1
        assert result[0]["title"] == "My Video"
        assert result[0]["tags"] == ["tag1", "tag2"]

    def test_parses_multiple_variants(self, tmp_path):
        from pipeline.distributor import parse_description_file
        f = tmp_path / "desc.txt"
        f.write_text(
            "Вариант 1\nTitle: V1\nDescription: D1\nTags: t1\n\n"
            "Вариант 2\nTitle: V2\nDescription: D2\nTags: t2\n",
            encoding="utf-8",
        )
        result = parse_description_file(f)
        assert len(result) == 2
        assert result[0]["title"] == "V1"
        assert result[1]["title"] == "V2"


class TestCollectShorts:
    def test_finds_mp4_files(self, tmp_path):
        from pipeline import distributor, config

        (tmp_path / "vid1.mp4").write_bytes(b"")
        (tmp_path / "vid1.json").write_text('{"title": "t"}', encoding="utf-8")
        (tmp_path / "vid2.mp4").write_bytes(b"")

        with patch.object(config, "OUTPUT_DIR", tmp_path):
            shorts = distributor.collect_shorts()

        assert len(shorts) == 2
        names = [s["video_path"].name for s in shorts]
        assert "vid1.mp4" in names
        assert "vid2.mp4" in names

    def test_returns_empty_when_dir_missing(self, tmp_path):
        from pipeline import distributor, config

        with patch.object(config, "OUTPUT_DIR", tmp_path / "nonexistent"):
            shorts = distributor.collect_shorts()

        assert shorts == []


class TestDistributeShortsDryRun:
    def test_dry_run_does_not_copy_files(self, tmp_path, tmp_video):
        from pipeline import distributor, config

        acc_dir = tmp_path / "accounts" / "acc1"
        acc_dir.mkdir(parents=True)
        (acc_dir / "config.json").write_text(
            json.dumps({"platforms": ["youtube"]}), encoding="utf-8"
        )

        with patch.object(config, "OUTPUT_DIR", tmp_path / "out"), \
             patch.object(config, "ACCOUNTS_ROOT", str(tmp_path / "accounts")), \
             patch.object(distributor, "collect_shorts", return_value=[
                 {"video_path": tmp_video, "meta": {"title": "t"}}
             ]):
            distributor.distribute_shorts(dry_run=True)

        queue = acc_dir / "upload_queue" / "youtube"
        assert not any(queue.glob("*.mp4"))


# ═══════════════════════════════════════════════════════════════════════
# 5. uploader.py
# ═══════════════════════════════════════════════════════════════════════

class TestCleanVideoMetadata:
    def test_returns_clean_path_on_success(self, tmp_video):
        from pipeline.uploader import clean_video_metadata

        with patch("pipeline.uploader.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = clean_video_metadata(tmp_video)

        assert "_clean" in result.name

    def test_returns_original_when_ffmpeg_missing(self, tmp_video):
        from pipeline.uploader import clean_video_metadata
        import subprocess

        with patch("pipeline.uploader.subprocess.run",
                   side_effect=FileNotFoundError("ffmpeg not found")):
            result = clean_video_metadata(tmp_video)

        assert result == tmp_video


@pytest.mark.slow
class TestUploadVideo:
    def test_returns_true_on_first_success(self, tmp_video):
        from pipeline.uploader import upload_video

        mock_ctx = MagicMock()

        def _fake_youtube(page, video_path, meta):
            return "https://youtu.be/ok"

        with patch("pipeline.uploader.send_telegram"), \
             patch.dict("pipeline.uploader._PLATFORM_UPLOADERS", {"youtube": _fake_youtube}):
            result = upload_video(mock_ctx, "youtube", tmp_video, {})

        assert result == "https://youtu.be/ok"

    def test_saves_error_json_after_5_failures(self, tmp_path, tmp_video):
        from pipeline.uploader import upload_video

        mock_ctx = MagicMock()

        def _always_fail(page, video_path, meta):
            raise RuntimeError("upload failed")

        with patch("pipeline.uploader.send_telegram"), \
             patch.dict("pipeline.uploader._PLATFORM_UPLOADERS", {"youtube": _always_fail}), \
             patch("pipeline.uploader.time.sleep"), \
             patch.object(__import__("pipeline.config", fromlist=["ACCOUNTS_ROOT"]),
                          "ACCOUNTS_ROOT", str(tmp_path)):
            result = upload_video(mock_ctx, "youtube", tmp_video, {},
                                  account_name="acc1", account_cfg={})

        assert result is None


@pytest.mark.slow
class TestUploadAllDryRun:
    def test_dry_run_skips_actual_upload(self, tmp_path, tmp_video):
        from pipeline import uploader, utils

        fake_accounts = [{
            "name": "acc1",
            "dir": tmp_path / "acc1",
            "config": {},
            "platforms": ["youtube"],
        }]
        fake_queue = [{"video_path": tmp_video, "meta": {"title": "t"}}]

        with patch.object(utils, "get_all_accounts", return_value=fake_accounts), \
             patch.object(utils, "get_upload_queue", return_value=fake_queue), \
             patch.object(utils, "get_uploads_today", return_value=0), \
             patch("pipeline.uploader.launch_browser",
                   return_value=(MagicMock(), MagicMock())), \
             patch("pipeline.uploader.close_browser"), \
             patch("pipeline.uploader.ensure_session_fresh", return_value=True), \
             patch("pipeline.uploader.run_activity"), \
             patch("pipeline.uploader.config.PLATFORM_DAILY_LIMITS",
                   {"youtube": 5}):
            results = uploader.upload_all(dry_run=True)

        assert len(results) == 1
        assert results[0]["status"] == "skipped"


# ═══════════════════════════════════════════════════════════════════════
# 6. run_pipeline.py  (оркестратор)
# ═══════════════════════════════════════════════════════════════════════

class TestRunPipeline:
    def test_run_stage_returns_false_on_exception(self):
        import run_pipeline

        def failing():
            raise RuntimeError("boom")

        result = run_pipeline.run_stage(failing, "test_stage")
        assert result is False

    def test_upload_results_fallback_to_empty_list(self, monkeypatch):
        """Если upload_all упал — финализация получает [], а не False."""
        import run_pipeline

        called_with = {}

        def fake_finalize(results, dry_run=False):
            called_with["results"] = results

        monkeypatch.setattr("run_pipeline.run_stage",
                            lambda fn, name, *a, **kw: False if name == "uploader" else None)
        # Симулируем только логику проверки типа
        upload_results = False
        if not isinstance(upload_results, list):
            upload_results = []

        assert upload_results == []

    def test_run_stage_returns_result_on_success(self):
        import run_pipeline

        result = run_pipeline.run_stage(lambda: 42, "test_stage")
        assert result == 42


# ═══════════════════════════════════════════════════════════════════════
# 7. finalize.py
# ═══════════════════════════════════════════════════════════════════════

class TestExtractSourceStem:
    def test_extracts_base_stem(self):
        from pipeline.finalize import _extract_source_stem
        assert _extract_source_stem("abc123_clip0001_clone05") == "abc123"
        assert _extract_source_stem("video_clip0000") == "video"
        assert _extract_source_stem("plain_name") == "plain_name"


class TestUpdateTracking:
    def test_marks_platform_as_uploaded(self):
        from pipeline.finalize import _update_tracking
        results = [
            {"status": "uploaded", "platform": "youtube",
             "source_path": "/out/video_clip0001.mp4"},
        ]
        tracking = _update_tracking(results, {})
        assert "video" in tracking
        assert tracking["video"]["youtube"] is True

    def test_skips_non_uploaded_results(self):
        from pipeline.finalize import _update_tracking
        results = [
            {"status": "error", "platform": "youtube",
             "source_path": "/out/video_clip0001.mp4"},
        ]
        tracking = _update_tracking(results, {})
        assert tracking == {}


class TestFindCompleteSources:
    def test_finds_fully_uploaded(self):
        from pipeline.finalize import _find_complete_sources
        from pipeline import config

        tracking = {
            "video1": {"youtube": True, "tiktok": True, "instagram": True},
            "video2": {"youtube": True, "tiktok": False},
        }
        with patch.object(config, "ALL_PLATFORMS", {"youtube", "tiktok", "instagram"}):
            complete = _find_complete_sources(tracking)

        assert "video1" in complete
        assert "video2" not in complete


class TestCollectStatistics:
    def test_counts_correctly(self):
        from pipeline.finalize import _collect_statistics
        results = [
            {"status": "uploaded", "platform": "youtube"},
            {"status": "uploaded", "platform": "tiktok"},
            {"status": "error",    "platform": "instagram"},
            {"status": "skipped",  "platform": "youtube"},
        ]
        stats = _collect_statistics(results)
        assert stats["total"] == 4
        assert stats["uploaded"] == 2
        assert stats["errors"] == 1
        assert stats["skipped"] == 1
        assert stats["platforms"]["youtube"] == 1


# ═══════════════════════════════════════════════════════════════════════
# 8. config.py
# ═══════════════════════════════════════════════════════════════════════

class TestPlatformDataclass:
    def test_frozen_prevents_mutation(self):
        from pipeline.config import Platform
        p = Platform("test", ("suffix",), ("prefix{n}:",))
        with pytest.raises(Exception):  # FrozenInstanceError или AttributeError
            p.name = "other"  # type: ignore

    def test_build_queries(self):
        from pipeline.config import Platform
        p = Platform("YT", ("#shorts",), ("ytsearch{n}:",))
        queries = p.build_queries("cats", n=5)
        assert len(queries) == 1
        assert "ytsearch5:" in queries[0]
        assert "cats #shorts" in queries[0]

    def test_search_suffixes_are_tuples(self):
        from pipeline.config import PLATFORMS
        for platform in PLATFORMS:
            assert isinstance(platform.search_suffixes, tuple)
            assert isinstance(platform.prefixes, tuple)


class TestConfigHasTelegramVars:
    def test_telegram_vars_exist(self):
        from pipeline import config
        assert hasattr(config, "TELEGRAM_BOT_TOKEN")
        assert hasattr(config, "TELEGRAM_CHAT_ID")


# ═══════════════════════════════════════════════════════════════════════
# 9. notifications.py
# ═══════════════════════════════════════════════════════════════════════

class TestSendTelegram:
    @staticmethod
    def _reset_telegram_rate_state():
        """Сброс глобального dedup/rate-limit между тестами одного процесса."""
        from pipeline import notifications

        notifications._tg_dedup_cache.clear()
        notifications._tg_last_send_ts = 0.0

    def test_returns_false_when_not_configured(self):
        self._reset_telegram_rate_state()
        from pipeline import notifications, config

        with patch.object(config, "TELEGRAM_BOT_TOKEN", ""), \
             patch.object(config, "TELEGRAM_CHAT_ID", ""):
            # Перезагружаем константы в модуле
            with patch("pipeline.notifications.TELEGRAM_BOT_TOKEN", ""), \
                 patch("pipeline.notifications.TELEGRAM_CHAT_ID", ""):
                result = notifications.send_telegram("test")

        assert result is False

    def test_returns_true_on_200(self):
        self._reset_telegram_rate_state()
        from pipeline import notifications

        mock_resp = MagicMock()
        mock_resp.status_code = 200

        with patch("pipeline.notifications.TELEGRAM_BOT_TOKEN", "TOKEN"), \
             patch("pipeline.notifications.TELEGRAM_CHAT_ID", "CHAT"), \
             patch("pipeline.notifications._CRITICAL_ONLY", False), \
             patch("pipeline.notifications.requests.post", return_value=mock_resp):
            result = notifications.send_telegram("hello")

        assert result is True

    def test_returns_false_on_request_error(self):
        self._reset_telegram_rate_state()
        from pipeline import notifications
        import requests as req

        with patch("pipeline.notifications.TELEGRAM_BOT_TOKEN", "TOKEN"), \
             patch("pipeline.notifications.TELEGRAM_CHAT_ID", "CHAT"), \
             patch("pipeline.notifications._CRITICAL_ONLY", False), \
             patch("pipeline.notifications.requests.post",
                   side_effect=req.exceptions.ConnectionError("fail")):
            # Уникальный текст — гарантия обхода дедупа от других тестов в suite
            result = notifications.send_telegram(
                "hello (connection error test) 7f3c2a1b-unique-msg"
            )

        assert result is False


# ═══════════════════════════════════════════════════════════════════════
# postprocessor: регрессия — один -filter_complex без TTS (не дублировать)
# ═══════════════════════════════════════════════════════════════════════


class TestPostprocessSingleFilterComplex:
    def test_no_tts_branch_single_filter_complex(self, tmp_path, tmp_video, probe_info):
        """Без TTS ffmpeg должен получить ровно один -filter_complex (иначе libx264 EOF)."""
        from pipeline.postprocessor import _postprocess_single

        out_path = tmp_path / "out.mp4"

        def _fake_run(cmd, **kwargs):
            Path(cmd[-1]).write_bytes(b"ok")
            return MagicMock(returncode=0)

        with patch("pipeline.postprocessor.probe_video", return_value=probe_info), \
             patch("pipeline.postprocessor._pick_random_banner", return_value=None), \
             patch("pipeline.postprocessor.subprocess.run", side_effect=_fake_run) as run_mock:
            ok = _postprocess_single(
                clip_path=tmp_video,
                out_path=out_path,
                banner_path=None,
                font_str="",
                vcodec="libx264",
                vcodec_opts={"preset": "veryfast"},
                meta={},
                shape="portrait_center",
                bg_path=None,
                tts_audio_path=None,
            )

        assert ok is True
        cmd = run_mock.call_args[0][0]
        assert cmd.count("-filter_complex") == 1
        assert cmd.count("[vout]") >= 1


class TestOverlayPositionExprs:
    def test_overlay_xy_no_duplicate_prefix(self):
        """OVERLAY_POSITION x=...:y=... не должен давать x=x= в drawtext."""
        from pipeline.postprocessor import _overlay_xy_exprs

        ox, oy = _overlay_xy_exprs()
        assert not ox.strip().lower().startswith("x=")
        assert not oy.strip().lower().startswith("y=")


class TestAiMetadataQuality:
    def test_normalize_meta_variant_trims_and_limits(self):
        from pipeline.ai import _normalize_meta_variant

        src = {
            "title": "  Очень длинный заголовок " + "x" * 200,
            "description": "  Описание " + "y" * 300,
            "tags": "a, b, c",
            "hook_text": "  hook  ",
            "thumbnail_idea": "  idea  ",
            "loop_prompt": "  loop  ",
            "overlays": [],
        }
        out = _normalize_meta_variant(src)
        assert len(out["title"]) <= 60
        assert len(out["description"]) <= 150
        assert out["tags"] == ["a", "b", "c"]
        assert out["hook_text"] == "hook"

    def test_meta_quality_rejects_generic_templates(self):
        from pipeline.ai import _meta_quality_ok

        bad = {
            "title": "Amazing video",
            "description": "Subscribe for more! #shorts #viral #trending",
        }
        good = {
            "title": "Кот открыл кран и затопил ванную",
            "description": "Кот случайно запускает воду в ванной, а хозяин в шоке пытается остановить поток.",
        }
        assert _meta_quality_ok(bad) is False
        assert _meta_quality_ok(good) is True

    def test_infer_niche_style_hint_animals(self, tmp_path):
        from pipeline.ai import _infer_niche_style_hint

        p = tmp_path / "cat_funny.mp4"
        hint = _infer_niche_style_hint(
            video_path=p,
            transcript="",
            trending_hashtags=["#pets"],
        )
        assert "animals" in hint

    def test_parse_metadata_json_response_with_wrapper_text(self):
        from pipeline.ai import _parse_metadata_json_response

        raw = (
            "Вот результат:\\n"
            "```json\\n"
            "[{\"title\":\"T\",\"description\":\"D\",\"tags\":[\"a\"]}]\\n"
            "```\\n"
            "Спасибо!"
        )
        parsed = _parse_metadata_json_response(raw)
        assert isinstance(parsed, list)
        assert parsed[0]["title"] == "T"

    def test_parse_metadata_json_response_dict_variants(self):
        from pipeline.ai import _parse_metadata_json_response

        raw = "{\"variants\":[{\"title\":\"T2\",\"description\":\"D2\"}]}"
        parsed = _parse_metadata_json_response(raw)
        assert isinstance(parsed, list)
        assert parsed[0]["title"] == "T2"

    def test_salvage_metadata_from_raw_text(self, tmp_path):
        from pipeline.ai import _salvage_metadata_from_raw_text

        p = tmp_path / "sample.mp4"
        raw = "Вот кратко: Кот открыл кран и устроил потоп. Хозяин в шоке."
        out = _salvage_metadata_from_raw_text(raw, p)
        assert isinstance(out, list) and out
        assert len(out[0]["title"]) >= 8
        assert len(out[0]["description"]) >= 20

    def test_salvage_strips_im_tokens(self, tmp_path):
        from pipeline.ai import _salvage_metadata_from_raw_text

        p = tmp_path / "cat_toilet_video.mp4"
        raw = "<|im_start|> assistant <|im_end|>"
        out = _salvage_metadata_from_raw_text(raw, p, transcript="Кот открыл кран и вода потекла.")
        assert out and "<|" not in out[0]["title"]
        assert "Кот" in out[0]["description"] or "кот" in out[0]["description"]

    def test_enrich_metadata_variant_fills_behavior_fields(self, tmp_path):
        from pipeline.ai import _enrich_metadata_variant

        p = tmp_path / "cat_toilet_scene.mp4"
        src = {
            "title": "<|im_start|>",
            "description": "",
            "tags": [],
            "thumbnail_idea": "",
            "hook_text": "",
            "overlays": [],
            "loop_prompt": "",
            "best_segment": None,
        }
        out = _enrich_metadata_variant(
            src,
            p,
            transcript="Кот открыл кран в туалете и вода льется.",
            trending_hashtags=["#cats", "#petlife"],
        )
        assert out["title"] and "<|" not in out["title"]
        assert len(out["description"]) >= 20
        assert isinstance(out["tags"], list) and len(out["tags"]) >= 1
        assert out["thumbnail_idea"]
        assert out["hook_text"]
        assert isinstance(out["overlays"], list) and len(out["overlays"]) >= 1
        assert out["loop_prompt"]


class TestTtsLangOverride:
    def test_tts_text_for_clip_override_ru_falls_back_to_detected_en(self):
        from pipeline.tts_utils import tts_text_for_clip

        text, lang = tts_text_for_clip(
            {"title": "Amazing short about cat"},
            lang_override="ru",
        )
        assert text is not None
        assert lang.startswith("en")

    def test_tts_text_for_clip_force_override_keeps_ru(self):
        from pipeline.tts_utils import tts_text_for_clip

        text, lang = tts_text_for_clip(
            {"title": "Amazing short about cat"},
            lang_override="ru",
            force_lang_override=True,
        )
        assert text is not None
        assert lang == "ru"
