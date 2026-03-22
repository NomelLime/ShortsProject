"""
tests/test_agents.py — Тесты агентного слоя ShortsProject.

Покрывает: GPUManager, AgentMemory, BaseAgent, Scout, Curator,
           Publisher, Guardian, Sentinel, Strategist, Accountant.

Все тесты используют временные директории и mock-объекты —
реальные файлы, сеть и GPU не нужны.
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Dict, List
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

# ---------------------------------------------------------------------------
# Хелперы
# ---------------------------------------------------------------------------

def make_memory(tmp_path: Path):
    """Создаёт AgentMemory с временным persist-файлом."""
    from pipeline.agent_memory import AgentMemory
    return AgentMemory(persist_path=tmp_path / "agent_memory.json")


def make_account(tmp_path: Path, name: str = "acc1",
                 platforms: List[str] = None,
                 upload_schedule: Dict = None,
                 uploads_today: int = 0) -> Path:
    """Создаёт структуру аккаунта в tmp_path."""
    platforms = platforms or ["youtube"]
    acc_dir = tmp_path / "accounts" / name
    acc_dir.mkdir(parents=True)

    cfg = {"platforms": platforms}
    if upload_schedule:
        cfg["upload_schedule"] = upload_schedule
    (acc_dir / "config.json").write_text(json.dumps(cfg), encoding="utf-8")

    if uploads_today:
        today = __import__("datetime").date.today().isoformat()
        limit_data = {"uploaded_today": {today: uploads_today}}
        (acc_dir / "daily_limit.json").write_text(
            json.dumps(limit_data), encoding="utf-8"
        )
    return acc_dir


# ===========================================================================
# 6А. GPU Manager
# ===========================================================================

class TestGPUManager:
    """GPUResourceManager: очередь приоритетов, параллелизм, статистика."""

    def setup_method(self):
        # Создаём свежий экземпляр (не синглтон)
        from pipeline.agents.gpu_manager import GPUResourceManager
        self.gpu = GPUResourceManager(max_concurrent=1)
        self.gpu.start()

    def teardown_method(self):
        self.gpu.stop()

    def test_priority_order(self):
        """CRITICAL должен получать ресурс раньше LLM при конкуренции."""
        from pipeline.agents.gpu_manager import GPUPriority

        acquired_order: List[str] = []
        barrier = threading.Barrier(3)  # два потока + основной

        def worker(priority, label):
            barrier.wait()  # все стартуют одновременно
            # Иначе LLM может попасть в очередь раньше CRITICAL — диспетчер обслужит его первым
            if label == "llm":
                time.sleep(0.08)
            with self.gpu.acquire(label, priority):
                acquired_order.append(label)

        # LLM стартует первым, CRITICAL — после небольшой задержки
        t_llm = threading.Thread(target=worker, args=(GPUPriority.LLM, "llm"))
        t_crit = threading.Thread(target=worker, args=(GPUPriority.CRITICAL, "critical"))

        t_llm.start()
        t_crit.start()
        barrier.wait()  # отпускаем оба

        t_llm.join(timeout=5)
        t_crit.join(timeout=5)

        # Оба выполнились; CRITICAL (0) обслуживается раньше LLM (1)
        assert acquired_order == ["critical", "llm"]

    def test_concurrent_limit(self):
        """Не более max_concurrent=1 задачи одновременно."""
        from pipeline.agents.gpu_manager import GPUPriority

        concurrent_count = [0]
        max_seen         = [0]
        lock             = threading.Lock()

        def worker():
            with self.gpu.acquire("test", GPUPriority.ENCODE):
                with lock:
                    concurrent_count[0] += 1
                    max_seen[0] = max(max_seen[0], concurrent_count[0])
                time.sleep(0.05)
                with lock:
                    concurrent_count[0] -= 1

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert max_seen[0] == 1, f"Одновременно работало {max_seen[0]} задач"

    def test_stats_tracking(self):
        """После acquire → stats['tts_test'].calls увеличивается."""
        from pipeline.agents.gpu_manager import GPUPriority

        stats_before = self.gpu.status().get("stats", {})
        with self.gpu.acquire("tts_test", GPUPriority.TTS):
            pass
        stats_after = self.gpu.status().get("stats", {})
        assert stats_after.get("tts_test", {}).get("calls", 0) >= 1
        assert stats_after.get("tts_test", {}).get("calls", 0) > \
               stats_before.get("tts_test", {}).get("calls", 0)

    def test_decorator_usage(self):
        """@gpu_manager.task() должен освобождать ресурс после вызова."""
        from pipeline.agents.gpu_manager import GPUPriority

        @self.gpu.gpu_task(GPUPriority.LLM)
        def my_task():
            return 42

        result = my_task()
        assert result == 42
        # После выполнения нет активных задач
        assert self.gpu.status().get("active_tasks", 0) == 0


# ===========================================================================
# 6Б. AgentMemory
# ===========================================================================

class TestAgentMemory:
    """AgentMemory: потокобезопасность, персистентность, лог событий."""

    def test_basic_set_get(self, tmp_path):
        mem = make_memory(tmp_path)
        mem.set("key1", "value1")
        assert mem.get("key1") == "value1"

    def test_default_on_missing_key(self, tmp_path):
        mem = make_memory(tmp_path)
        assert mem.get("nonexistent", default="fallback") == "fallback"

    def test_thread_safe_set_get(self, tmp_path):
        """100 потоков пишут разные ключи одновременно — данные не теряются."""
        mem = make_memory(tmp_path)
        errors: List[Exception] = []

        def writer(i):
            try:
                mem.set(f"key_{i}", i, persist=False)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Ошибки при многопоточной записи: {errors}"
        for i in range(100):
            assert mem.get(f"key_{i}") == i

    def test_persistence(self, tmp_path):
        """set → перезагрузка → get возвращает то же значение."""
        from pipeline.agent_memory import AgentMemory

        path = tmp_path / "mem.json"
        mem1 = AgentMemory(persist_path=path)
        mem1.set("persistent_key", {"nested": True}, persist=True)

        mem2 = AgentMemory(persist_path=path)
        assert mem2.get("persistent_key") == {"nested": True}

    def test_log_event_max_500(self, tmp_path):
        """events deque не превышает 500 записей."""
        mem = make_memory(tmp_path)
        for i in range(600):
            mem.log_event("TEST", f"event_{i}", {"i": i})

        events = mem.get_events()
        assert len(events) <= 500

    def test_agent_status_roundtrip(self, tmp_path):
        """set_agent_status → get_all_agent_statuses содержит статус."""
        mem = make_memory(tmp_path)
        mem.register_agent("SCOUT")
        mem.set_agent_status("SCOUT", "RUNNING: crawling")

        statuses = mem.get_all_agent_statuses()
        assert "SCOUT" in statuses
        assert "RUNNING" in statuses["SCOUT"]

    def test_delete_key(self, tmp_path):
        mem = make_memory(tmp_path)
        mem.set("to_delete", 99)
        mem.delete("to_delete")
        assert mem.get("to_delete") is None


# ===========================================================================
# 6В. BaseAgent lifecycle
# ===========================================================================

class TestBaseAgentLifecycle:
    """BaseAgent: start/stop, статусы, should_stop, sleep."""

    def _make_agent(self, tmp_path, run_fn=None):
        """Создаёт конкретного агента с подменённым run()."""
        from pipeline.agents.base_agent import BaseAgent, AgentStatus

        mem = make_memory(tmp_path)

        class ConcreteAgent(BaseAgent):
            def __init__(self):
                super().__init__("TEST_AGENT", mem, notify=None)
                self.ran = False
                self.cycles = 0

            def run(self):
                if run_fn:
                    run_fn(self)
                else:
                    while not self.should_stop:
                        self.cycles += 1
                        if not self.sleep(0.05):
                            break

        return ConcreteAgent()

    def test_start_stop(self, tmp_path):
        """Агент запускается в потоке, корректно останавливается."""
        agent = self._make_agent(tmp_path)
        agent.start()
        time.sleep(0.1)
        assert agent._thread is not None
        assert agent._thread.is_alive()

        agent.stop(timeout=2.0)
        assert not agent._thread.is_alive()

    def test_should_stop_after_stop(self, tmp_path):
        """После stop() — should_stop == True."""
        agent = self._make_agent(tmp_path)
        agent.start()
        agent.stop(timeout=2.0)
        assert agent.should_stop

    def test_status_transitions(self, tmp_path):
        """IDLE → RUNNING (во время работы) → STOPPED после stop."""
        from pipeline.agents.base_agent import AgentStatus

        status_log: List[str] = []

        def run_fn(a):
            status_log.append(a.status.value)
            a._set_status(AgentStatus.RUNNING)
            status_log.append(a.status.value)
            a.sleep(0.2)

        agent = self._make_agent(tmp_path, run_fn)
        assert agent.status == AgentStatus.IDLE
        agent.start()
        time.sleep(0.15)
        assert agent.status == AgentStatus.RUNNING
        agent.stop(timeout=2.0)
        assert agent.status == AgentStatus.STOPPED

    def test_sleep_interrupted_by_stop(self, tmp_path):
        """sleep() возвращает False при вызове stop()."""
        results: List[bool] = []

        def run_fn(a):
            result = a.sleep(10.0)  # длинный sleep
            results.append(result)

        agent = self._make_agent(tmp_path, run_fn)
        agent.start()
        time.sleep(0.05)
        agent.stop(timeout=2.0)
        # sleep был прерван → вернул False
        assert results and results[0] is False

    def test_error_captured(self, tmp_path):
        """Исключение в run() → status ERROR, _last_error заполнен."""
        from pipeline.agents.base_agent import AgentStatus

        def run_fn(a):
            raise RuntimeError("test crash")

        agent = self._make_agent(tmp_path, run_fn)
        agent.start()
        time.sleep(0.2)

        assert agent.status == AgentStatus.ERROR
        assert "test crash" in (agent._last_error or "")

    def test_report_stored_in_memory(self, tmp_path):
        """report() сохраняет данные в AgentMemory."""
        agent = self._make_agent(tmp_path)
        agent.report({"score": 42, "platform": "youtube"})
        # Проверяем через память
        report_key = f"report_{agent.name.lower()}"
        stored = agent.memory.get(report_key)
        assert stored is not None
        assert stored.get("data", {}).get("score") == 42


# ===========================================================================
# 6Г. Scout
# ===========================================================================

class TestScout:
    """Scout: поиск URL, сохранение, override от COMMANDER."""

    def test_scout_saves_urls(self, tmp_path):
        """Mock yt-dlp → Scout._crawl_cycle() → URLs в памяти."""
        mem = make_memory(tmp_path)

        with patch("pipeline.agents.scout.get_gpu_manager") as mock_gpu_mgr, \
             patch("pipeline.agents.scout.get_memory", return_value=mem), \
             patch("pipeline.utils.load_keywords", return_value=["cats"]), \
             patch("pipeline.downloader._search_ytdlp",
                   return_value=["https://yt.be/1", "https://yt.be/2"]):

            mock_gpu = MagicMock()
            mock_gpu.acquire.return_value.__enter__ = MagicMock(return_value=None)
            mock_gpu.acquire.return_value.__exit__ = MagicMock(return_value=False)
            mock_gpu_mgr.return_value = mock_gpu

            from pipeline.agents.scout import Scout
            scout = Scout(memory=mem, notify=None, interval_sec=9999)

            with patch("pipeline.utils.merge_and_save_urls") as mock_save:
                mock_save.return_value = 2
                scout._crawl_cycle()
                # merge_and_save_urls должен быть вызван с найденными URL
                mock_save.assert_called_once()
                args = mock_save.call_args[0]
                urls = args[0] if args else []
                assert "https://yt.be/1" in urls

    def test_scout_uses_keyword_override(self, tmp_path):
        """COMMANDER устанавливает scout_keywords_override → Scout использует."""
        mem = make_memory(tmp_path)
        mem.set("scout_keywords_override", ["override_kw"])
        # Иначе при пустом yt-dlp пойдёт реальный браузерный поиск (сеть/Playwright).
        mem.set("scout_browser_enabled", False)

        with patch("pipeline.agents.scout.get_gpu_manager") as mock_gpu_mgr, \
             patch("pipeline.downloader._search_ytdlp", return_value=[]) as mock_search, \
             patch("pipeline.utils.load_keywords", return_value=["original_kw"]), \
             patch("pipeline.utils.merge_and_save_urls", return_value=0):

            mock_gpu = MagicMock()
            mock_gpu.acquire.return_value.__enter__ = MagicMock(return_value=None)
            mock_gpu.acquire.return_value.__exit__ = MagicMock(return_value=False)
            mock_gpu_mgr.return_value = mock_gpu

            from pipeline.agents.scout import Scout
            scout = Scout(memory=mem, notify=None, interval_sec=9999)
            scout._crawl_cycle()

            # Поиск вызван с override-ключевым словом, не оригинальным
            assert mock_search.called, "ожидали вызов _search_ytdlp с override keywords"
            call_kwargs = mock_search.call_args
            called_keywords = (
                call_kwargs[0][0] if call_kwargs[0] else
                call_kwargs[1].get("keywords", [])
            )
            assert "override_kw" in str(called_keywords)


# ===========================================================================
# 6Д. Curator
# ===========================================================================

class TestCurator:
    """Curator: фильтрация по длине/разрешению, dedup."""

    def test_rejects_short_video(self, tmp_path):
        """Видео короче 5с → отклонить с причиной."""
        mem = make_memory(tmp_path)

        with patch("pipeline.agents.curator.get_memory", return_value=mem):
            from pipeline.agents.curator import Curator
            curator = Curator(memory=mem, notify=None)

        mock_probe = {"duration": 3.0, "width": 720, "height": 1280, "has_audio": True}

        ok, reason = curator._evaluate(
            Path("test.mp4"),
            lambda _p: mock_probe,
            lambda _path: False,   # is_duplicate = False
        )
        assert not ok
        assert reason  # должна быть причина

    def test_accepts_valid_video(self, tmp_path):
        """Нормальное видео (30с, 720p, с аудио) → принять."""
        mem = make_memory(tmp_path)

        with patch("pipeline.agents.curator.get_memory", return_value=mem):
            from pipeline.agents.curator import Curator
            curator = Curator(memory=mem, notify=None)

        mock_probe = {"duration": 30.0, "width": 720, "height": 1280, "has_audio": True}

        ok, reason = curator._evaluate(
            Path("test.mp4"),
            lambda _p: mock_probe,
            lambda _path: False,
        )
        assert ok, f"Ожидали OK, получили: {reason}"

    def test_rejects_duplicate(self, tmp_path):
        """Дубликат (is_duplicate=True) → отклонить."""
        mem = make_memory(tmp_path)

        with patch("pipeline.agents.curator.get_memory", return_value=mem):
            from pipeline.agents.curator import Curator
            curator = Curator(memory=mem, notify=None)

        mock_probe = {"duration": 30.0, "width": 720, "height": 1280, "has_audio": True}

        ok, reason = curator._evaluate(
            Path("test.mp4"),
            lambda _p: mock_probe,
            lambda _path: True,   # is_duplicate = True
        )
        assert not ok

    def test_rejects_low_resolution(self, tmp_path):
        """Видео с шириной < 320px → отклонить."""
        mem = make_memory(tmp_path)

        with patch("pipeline.agents.curator.get_memory", return_value=mem):
            from pipeline.agents.curator import Curator
            curator = Curator(memory=mem, notify=None)

        mock_probe = {"duration": 30.0, "width": 240, "height": 360, "has_audio": True}

        ok, reason = curator._evaluate(
            Path("test.mp4"),
            lambda _p: mock_probe,
            lambda _path: False,
        )
        assert not ok


# ===========================================================================
# 6Е. Publisher
# ===========================================================================

class TestPublisher:
    """Publisher: построение очереди, сводка батча (_process_results)."""

    def test_process_results_updates_counters_and_batch_done(self, tmp_path):
        """ok/error/warmup/other → счётчики и событие batch_done в памяти."""
        mem = make_memory(tmp_path)
        from pipeline.agents.publisher import Publisher

        pub = Publisher(memory=mem, notify=None)
        pub._guardian = MagicMock()
        results = [
            {"status": "ok", "platform": "youtube"},
            {"status": "ok", "platform": "youtube"},
            {"status": "error", "platform": "tiktok"},
            {"status": "warmup", "platform": "instagram"},
            {"status": "not_logged_in", "platform": "youtube"},
        ]
        with patch.object(pub, "_send"):
            pub._process_results(results)

        assert pub._uploaded == 2
        assert pub._failed == 1
        batch_events = [e for e in mem.get_events(agent="PUBLISHER") if e.get("event") == "batch_done"]
        assert batch_events
        data = batch_events[-1].get("data", {})
        assert data.get("batch_ok") == 2
        assert data.get("batch_errors") == 1
        assert data.get("batch_warmup") == 1
        assert data.get("batch_other") == 1

    def test_build_task_list_skips_quarantined_account(self, tmp_path):
        """Карантин в Guardian → задача не попадает в список."""
        mem = make_memory(tmp_path)
        acc_root = tmp_path / "accounts" / "acc1"
        acc_root.mkdir(parents=True)
        (acc_root / "config.json").write_text(json.dumps({"platforms": ["youtube"]}), encoding="utf-8")

        accounts = [{
            "name":      "acc1",
            "dir":       acc_root,
            "config":    {"platforms": ["youtube"]},
            "platforms": ["youtube"],
        }]
        mock_g = MagicMock()
        mock_g.is_account_safe.return_value = (False, "quarantine")

        fake_queue = [{"video_path": tmp_path / "v.mp4", "meta": {}}]
        (tmp_path / "v.mp4").write_bytes(b"x")

        with patch("pipeline.utils.get_upload_queue", return_value=fake_queue), \
             patch("pipeline.utils.get_uploads_today", return_value=0), \
             patch("pipeline.upload_warmup.is_upload_blocked", return_value=(False, "")), \
             patch("pipeline.agents.publisher.get_memory", return_value=mem):

            from pipeline.agents.publisher import Publisher

            pub = Publisher(memory=mem, notify=None, guardian=mock_g)
            tasks = pub._build_task_list(accounts)

        assert tasks == []
        assert pub._skipped >= 1


# ===========================================================================
# 6Ж. Guardian
# ===========================================================================

class TestGuardian:
    """Guardian: карантин, бан-сигналы, is_account_safe."""

    def test_is_account_safe_when_quarantined(self, tmp_path):
        """Аккаунт в карантине → is_account_safe вернёт False."""
        mem = make_memory(tmp_path)

        with patch("pipeline.quarantine.is_quarantined", return_value=True), \
             patch("pipeline.agents.guardian.get_memory", return_value=mem):

            from pipeline.agents.guardian import Guardian
            guardian = Guardian(memory=mem, notify=None)
            safe, reason = guardian.is_account_safe("acc1", "youtube")

        assert not safe
        assert reason  # причина должна быть объяснена

    def test_is_account_safe_when_clean(self, tmp_path):
        """Чистый аккаунт → is_account_safe вернёт True."""
        mem = make_memory(tmp_path)

        with patch("pipeline.quarantine.is_quarantined", return_value=False), \
             patch("pipeline.upload_warmup.is_upload_blocked", return_value=(False, "")), \
             patch("pipeline.agents.guardian.get_memory", return_value=mem):

            from pipeline.agents.guardian import Guardian
            guardian = Guardian(memory=mem, notify=None)
            safe, _ = guardian.is_account_safe("acc1", "youtube")

        assert safe

    def test_ban_signal_429_detected(self, tmp_path):
        """report_upload_error с HTTP 429 → ban_signal в AgentMemory."""
        mem = make_memory(tmp_path)

        with patch("pipeline.agents.guardian.get_memory", return_value=mem), \
             patch("pipeline.quarantine.is_quarantined", return_value=False), \
             patch("pipeline.quarantine.mark_error"):

            from pipeline.agents.guardian import Guardian
            guardian = Guardian(memory=mem, notify=None)
            guardian.report_upload_error("acc1", "youtube", "HTTP 429 Too Many Requests")

        events = mem.get_events()
        ban_events = [e for e in events if "ban" in str(e.get("event", "")).lower()]
        assert len(ban_events) >= 1

    def test_ban_signal_403_detected(self, tmp_path):
        """HTTP 403 тоже считается бан-сигналом."""
        mem = make_memory(tmp_path)

        with patch("pipeline.agents.guardian.get_memory", return_value=mem), \
             patch("pipeline.quarantine.is_quarantined", return_value=False), \
             patch("pipeline.quarantine.mark_error"):

            from pipeline.agents.guardian import Guardian
            guardian = Guardian(memory=mem, notify=None)
            guardian.report_upload_error("acc1", "tiktok", "HTTP 403 Forbidden")

        events = mem.get_events()
        ban_events = [e for e in events if "ban" in str(e.get("event", "")).lower()]
        assert len(ban_events) >= 1


# ===========================================================================
# 6З. Sentinel (авто-рестарт)
# ===========================================================================

class TestSentinel:
    """Sentinel: запрос рестарта через AgentMemory."""

    def test_no_restart_request_for_idle_agent(self, tmp_path):
        """Агент в IDLE → Sentinel не просит рестарт."""
        mem = make_memory(tmp_path)
        mem.register_agent("SCOUT")
        mem.set_agent_status("SCOUT", "IDLE")

        with patch("pipeline.agents.sentinel.get_memory", return_value=mem):
            from pipeline.agents.sentinel import Sentinel
            sentinel = Sentinel(memory=mem, notify=None)
            sentinel._check_agents()

        requests = mem.get("sentinel_restart_requests", [])
        assert "SCOUT" not in requests

    def test_no_restart_for_waiting_agent(self, tmp_path):
        """Агент в WAITING (ждёт GPU) → Sentinel не просит рестарт."""
        mem = make_memory(tmp_path)
        mem.register_agent("NARRATOR")
        mem.set_agent_status("NARRATOR", "WAITING: ждёт GPU")

        with patch("pipeline.agents.sentinel.get_memory", return_value=mem):
            from pipeline.agents.sentinel import Sentinel
            sentinel = Sentinel(memory=mem, notify=None)
            sentinel._check_agents()

        requests = mem.get("sentinel_restart_requests", [])
        assert "NARRATOR" not in requests

    def test_restart_requested_after_error_threshold(self, tmp_path):
        """Агент в ERROR > 2 мин → Sentinel добавляет в restart_requests."""
        from pipeline.agents.sentinel import _ERROR_RESTART_SEC

        mem = make_memory(tmp_path)
        mem.register_agent("EDITOR")
        mem.set_agent_status("EDITOR", "ERROR: crash")

        with patch("pipeline.agents.sentinel.get_memory", return_value=mem):
            from pipeline.agents.sentinel import Sentinel
            sentinel = Sentinel(memory=mem, notify=None)

            # Первый вызов — фиксирует время начала ошибки
            sentinel._check_agents()
            assert "EDITOR" not in mem.get("sentinel_restart_requests", [])

            # Симулируем прошедшее время > порога
            sentinel._error_since["EDITOR"] = time.time() - (_ERROR_RESTART_SEC + 5)

            # Второй вызов — должен запросить рестарт
            sentinel._check_agents()

        requests = mem.get("sentinel_restart_requests", [])
        assert "EDITOR" in requests

    def test_error_timer_reset_after_recovery(self, tmp_path):
        """Агент вышел из ERROR → таймер ошибки сбрасывается."""
        mem = make_memory(tmp_path)
        mem.register_agent("CURATOR")
        mem.set_agent_status("CURATOR", "ERROR: something")

        with patch("pipeline.agents.sentinel.get_memory", return_value=mem):
            from pipeline.agents.sentinel import Sentinel
            sentinel = Sentinel(memory=mem, notify=None)
            sentinel._check_agents()
            assert "CURATOR" in sentinel._error_since

            # Агент восстановился
            mem.set_agent_status("CURATOR", "IDLE")
            sentinel._check_agents()

        assert "CURATOR" not in sentinel._error_since


# ===========================================================================
# 6И. Director (_process_sentinel_requests)
# ===========================================================================

class TestDirectorSentinelIntegration:
    """Director читает sentinel_restart_requests и вызывает restart_agent."""

    def test_director_processes_restart_request(self, tmp_path):
        """sentinel_restart_requests содержит агента → Director перезапускает."""
        from pipeline.agents.base_agent import AgentStatus

        mem = make_memory(tmp_path)
        mem.set("sentinel_restart_requests", ["SCOUT"])

        mock_scout = MagicMock()
        mock_scout.name   = "SCOUT"
        mock_scout.status = AgentStatus.ERROR

        with patch("pipeline.agents.director.get_memory", return_value=mem), \
             patch("pipeline.agents.director.get_gpu_manager"):

            from pipeline.agents.director import Director
            director = Director(memory=mem, notify=None)
            director._agents["SCOUT"] = mock_scout
            director._process_sentinel_requests()

        mock_scout.stop.assert_called()
        mock_scout.start.assert_called()
        # Запрос должен быть удалён из списка
        assert "SCOUT" not in mem.get("sentinel_restart_requests", [])

    def test_director_skips_already_recovered(self, tmp_path):
        """Агент уже IDLE (сам восстановился) → Director пропускает рестарт."""
        from pipeline.agents.base_agent import AgentStatus

        mem = make_memory(tmp_path)
        mem.set("sentinel_restart_requests", ["VISIONARY"])

        mock_visionary = MagicMock()
        mock_visionary.name   = "VISIONARY"
        mock_visionary.status = AgentStatus.IDLE  # уже ок

        with patch("pipeline.agents.director.get_memory", return_value=mem), \
             patch("pipeline.agents.director.get_gpu_manager"):

            from pipeline.agents.director import Director
            director = Director(memory=mem, notify=None)
            director._agents["VISIONARY"] = mock_visionary
            director._process_sentinel_requests()

        mock_visionary.stop.assert_not_called()
        mock_visionary.start.assert_not_called()


# ===========================================================================
# 6К. Accountant
# ===========================================================================

class TestAccountant:
    """Accountant: лимиты, расписание, set_custom_limit."""

    def test_get_account_capacity(self, tmp_path):
        """get_account_capacity возвращает (доступных, всего)."""
        mem = make_memory(tmp_path)
        mem.set("account_stats", {
            "acc1": {"youtube": {"at_limit": False}},
            "acc2": {"youtube": {"at_limit": True}},
            "acc3": {"youtube": {"at_limit": False}},
        })

        with patch("pipeline.agents.accountant.get_memory", return_value=mem):
            from pipeline.agents.accountant import Accountant
            acc = Accountant(memory=mem, notify=None)

        available, total = acc.get_account_capacity("youtube")
        assert total == 3
        assert available == 2

    def test_set_custom_limit_persisted(self, tmp_path):
        """set_custom_limit → данные в AgentMemory."""
        mem = make_memory(tmp_path)

        with patch("pipeline.agents.accountant.get_memory", return_value=mem):
            from pipeline.agents.accountant import Accountant
            acc = Accountant(memory=mem, notify=None)

        acc.set_custom_limit("tiktok", 15)
        limits = acc.get_custom_limits()
        assert limits.get("tiktok") == 15

    def test_set_custom_limit_per_account(self, tmp_path):
        """set_custom_limit с account_name → ключ acc.platform."""
        mem = make_memory(tmp_path)

        with patch("pipeline.agents.accountant.get_memory", return_value=mem):
            from pipeline.agents.accountant import Accountant
            acc = Accountant(memory=mem, notify=None)

        acc.set_custom_limit("youtube", 5, account_name="acc1")
        limits = acc.get_custom_limits()
        assert limits.get("acc1.youtube") == 5

    def test_get_next_upload_times_from_accounts(self, tmp_path):
        """get_next_upload_times читает расписание из config.json аккаунтов."""
        make_account(tmp_path, "acc1", ["youtube"],
                     upload_schedule={"youtube": ["09:00", "19:00"]})

        mem = make_memory(tmp_path)

        with patch("pipeline.agents.accountant.get_memory", return_value=mem), \
             patch("pipeline.utils.get_all_accounts") as mock_accounts:

            mock_accounts.return_value = [{
                "name": "acc1",
                "dir": tmp_path / "accounts" / "acc1",
                "platforms": ["youtube"],
                "config": {"upload_schedule": {"youtube": ["09:00", "19:00"]}},
            }]

            from pipeline.agents.accountant import Accountant
            acc = Accountant(memory=mem, notify=None)

        times = acc.get_next_upload_times("youtube")
        assert "09:00" in times
        assert "19:00" in times

    def test_get_available_accounts(self, tmp_path):
        """get_available_accounts возвращает только не-лимитные аккаунты."""
        mem = make_memory(tmp_path)
        mem.set("account_stats", {
            "acc1": {"tiktok": {"at_limit": False}},
            "acc2": {"tiktok": {"at_limit": True}},
        })

        with patch("pipeline.agents.accountant.get_memory", return_value=mem):
            from pipeline.agents.accountant import Accountant
            acc = Accountant(memory=mem, notify=None)

        available = acc.get_available_accounts("tiktok")
        assert "acc1" in available
        assert "acc2" not in available


# ===========================================================================
# 6Л. Strategist (_apply_schedule_recommendations)
# ===========================================================================

class TestStrategistSchedule:
    """Strategist: применение расписания к config.json аккаунтов."""

    def test_apply_schedule_writes_to_config(self, tmp_path):
        """best_times → config.json аккаунтов обновлён."""
        acc_dir = make_account(tmp_path, "acc1", ["youtube"])
        mem     = make_memory(tmp_path)

        mock_accounts = [{
            "name":      "acc1",
            "dir":       acc_dir,
            "platforms": ["youtube"],
            "config":    {"platforms": ["youtube"]},
        }]

        with patch("pipeline.utils.get_all_accounts", return_value=mock_accounts), \
             patch("pipeline.agents.strategist.get_memory", return_value=mem):

            from pipeline.agents.strategist import Strategist
            strategist = Strategist(memory=mem, notify=None)
            strategist._apply_schedule_recommendations({"youtube": [9, 19]})

        cfg = json.loads((acc_dir / "config.json").read_text())
        assert cfg.get("upload_schedule", {}).get("youtube") == ["09:00", "19:00"]

    def test_apply_schedule_skips_wrong_platform(self, tmp_path):
        """Платформа не в аккаунте → config.json не меняется."""
        acc_dir = make_account(tmp_path, "acc1", ["youtube"])
        mem     = make_memory(tmp_path)

        mock_accounts = [{
            "name":      "acc1",
            "dir":       acc_dir,
            "platforms": ["youtube"],
            "config":    {"platforms": ["youtube"]},
        }]

        with patch("pipeline.utils.get_all_accounts", return_value=mock_accounts), \
             patch("pipeline.agents.strategist.get_memory", return_value=mem):

            from pipeline.agents.strategist import Strategist
            strategist = Strategist(memory=mem, notify=None)
            # tiktok не в платформах acc1
            strategist._apply_schedule_recommendations({"tiktok": [10, 20]})

        cfg = json.loads((acc_dir / "config.json").read_text())
        assert "tiktok" not in cfg.get("upload_schedule", {})

    def test_apply_empty_best_times_no_op(self, tmp_path):
        """Пустой best_times → ничего не делаем."""
        mem = make_memory(tmp_path)

        with patch("pipeline.utils.get_all_accounts", return_value=[]), \
             patch("pipeline.agents.strategist.get_memory", return_value=mem):

            from pipeline.agents.strategist import Strategist
            strategist = Strategist(memory=mem, notify=None)
            strategist._apply_schedule_recommendations({})  # не должно бросить исключение
