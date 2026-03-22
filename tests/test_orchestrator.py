"""
tests/test_orchestrator.py — Director, Commander, ShortsProjectCrew (smoke).

Без тяжёлых зависимостей: моки агентов при тесте crew; Director/Commander — изолированно.
"""

from __future__ import annotations

import sys
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from pipeline.agents.director import BOOT_ORDER, Director


def _make_memory(tmp_path):
    from pipeline.agent_memory import AgentMemory
    return AgentMemory(persist_path=tmp_path / "agent_memory.json")


# ---------------------------------------------------------------------------
# Director: register + порядок start_all / stop_all
# ---------------------------------------------------------------------------

class TestDirectorBootOrder:
    """Реестр и BOOT_ORDER: start по возрастанию, stop — в обратном."""

    def test_start_all_calls_agents_in_boot_order(self, tmp_path):
        mem = _make_memory(tmp_path)
        order: list[str] = []

        def make_agent(name: str):
            m = MagicMock()
            m.name = name

            def on_start():
                order.append(name)

            m.start.side_effect = on_start
            return m

        with patch("pipeline.agents.director.get_gpu_manager"):
            d = Director(memory=mem, notify=None)

        with patch("pipeline.agents.director.time.sleep"), \
             patch("pipeline.notifications.send_telegram", return_value=None):
            for name in BOOT_ORDER:
                d.register(make_agent(name))
            d.start_all()

        assert order == list(BOOT_ORDER)

    def test_stop_all_calls_agents_in_reverse_boot_order(self, tmp_path):
        mem = _make_memory(tmp_path)
        order: list[str] = []

        def make_agent(name: str):
            m = MagicMock()
            m.name = name

            def on_stop():
                order.append(name)

            m.stop.side_effect = on_stop
            return m

        with patch("pipeline.agents.director.get_gpu_manager"):
            d = Director(memory=mem, notify=None)

        with patch("pipeline.notifications.send_telegram", return_value=None):
            for name in BOOT_ORDER:
                d.register(make_agent(name))
            d.stop_all()

        assert order == list(reversed(BOOT_ORDER))


# ---------------------------------------------------------------------------
# Commander: быстрые команды и dispatch без Ollama (fallback)
# ---------------------------------------------------------------------------

class TestCommander:
    def test_quick_status_requires_director(self, tmp_path):
        from pipeline.agents.commander import Commander

        mem = _make_memory(tmp_path)
        c = Commander(director=None, memory=mem, notify=None)
        out = c.handle_command("статус")
        assert "не подключён" in out.lower() or "DIRECTOR" in out

    def test_quick_status_uses_director_full_status(self, tmp_path):
        from pipeline.agents.commander import Commander

        mem = _make_memory(tmp_path)
        dr = MagicMock()
        dr.full_status.return_value = {
            "agents": {"SCOUT": {"status": "idle", "uptime": 0, "error": None}},
            "gpu":    {"active": {}, "queue_size": 0},
        }
        c = Commander(director=dr, memory=mem, notify=None)
        out = c.handle_command("status")
        assert "SCOUT" in out
        dr.full_status.assert_called()

    def test_fallback_start_calls_director_start_all(self, tmp_path):
        from pipeline.agents.commander import Commander

        mem = _make_memory(tmp_path)
        mem.set("ollama_available", False)

        dr = MagicMock()
        c = Commander(director=dr, memory=mem, notify=None, auto_confirm=True)
        out = c.handle_command("запусти всё")
        dr.start_all.assert_called_once()
        assert "запущен" in out.lower() or "✅" in out


# ---------------------------------------------------------------------------
# ShortsProjectCrew: делегирование start/stop/command/status
# ---------------------------------------------------------------------------

def _agent_factory(agent_name: str):
    def _(*args, **kwargs):
        m = MagicMock()
        m.name = agent_name
        m.start = MagicMock()
        m.stop = MagicMock()
        return m

    return _


@contextmanager
def _patched_crew_deps(tmp_path, dir_inst, cmd_inst, gpu):
    mem = _make_memory(tmp_path)
    # Патчим источник: crew делает from ... import get_memory / get_gpu_manager при загрузке
    patches = [
        patch("pipeline.agent_memory.get_memory", return_value=mem),
        patch("pipeline.agents.gpu_manager.get_gpu_manager", return_value=gpu),
        patch("pipeline.agents.director.Director", lambda *a, **k: dir_inst),
        patch("pipeline.agents.commander.Commander", lambda *a, **k: cmd_inst),
        patch("pipeline.agents.sentinel.Sentinel", side_effect=_agent_factory("SENTINEL")),
        patch("pipeline.agents.scout.Scout", side_effect=_agent_factory("SCOUT")),
        patch("pipeline.agents.curator.Curator", side_effect=_agent_factory("CURATOR")),
        patch("pipeline.agents.visionary.Visionary", side_effect=_agent_factory("VISIONARY")),
        patch("pipeline.agents.narrator.Narrator", side_effect=_agent_factory("NARRATOR")),
        patch("pipeline.agents.guardian.Guardian", side_effect=_agent_factory("GUARDIAN")),
        patch("pipeline.agents.accountant.Accountant", side_effect=_agent_factory("ACCOUNTANT")),
        patch("pipeline.agents.strategist.Strategist", side_effect=_agent_factory("STRATEGIST")),
        patch("pipeline.agents.editor.Editor", side_effect=_agent_factory("EDITOR")),
        patch("pipeline.agents.publisher.Publisher", side_effect=_agent_factory("PUBLISHER")),
    ]
    for p in patches:
        p.start()
    try:
        sys.modules.pop("pipeline.crew", None)
        from pipeline.crew import ShortsProjectCrew

        yield ShortsProjectCrew
    finally:
        for p in reversed(patches):
            p.stop()
        sys.modules.pop("pipeline.crew", None)


class TestShortsProjectCrewSmoke:
    def test_start_stop_invokes_gpu_commander_director(self, tmp_path):
        gpu = MagicMock()
        dir_inst = MagicMock()
        cmd_inst = MagicMock()

        with _patched_crew_deps(tmp_path, dir_inst, cmd_inst, gpu) as CrewCls:
            crew = CrewCls()
            crew.start()
            gpu.start.assert_called_once()
            cmd_inst.start.assert_called_once()
            dir_inst.start_all.assert_called_once()
            dir_inst.start.assert_called_once()

            crew.stop()
            dir_inst.stop_all.assert_called_once()
            cmd_inst.stop.assert_called_once()
            dir_inst.stop.assert_called_once()
            gpu.stop.assert_called_once()

    def test_command_delegates_to_commander(self, tmp_path):
        gpu = MagicMock()
        dir_inst = MagicMock()
        cmd_inst = MagicMock()
        cmd_inst.handle_command.return_value = "ok"

        with _patched_crew_deps(tmp_path, dir_inst, cmd_inst, gpu) as CrewCls:
            crew = CrewCls()
            assert crew.command("ping") == "ok"
            cmd_inst.handle_command.assert_called_once_with("ping")

    def test_status_delegates_to_director_full_status(self, tmp_path):
        gpu = MagicMock()
        dir_inst = MagicMock()
        cmd_inst = MagicMock()
        want = {"director": "running", "agents": {}}
        dir_inst.full_status.return_value = want

        with _patched_crew_deps(tmp_path, dir_inst, cmd_inst, gpu) as CrewCls:
            crew = CrewCls()
            assert crew.status() == want
            dir_inst.full_status.assert_called_once()

    def test_register_order_matches_boot_order(self, tmp_path):
        gpu = MagicMock()
        dir_inst = MagicMock()
        cmd_inst = MagicMock()

        with _patched_crew_deps(tmp_path, dir_inst, cmd_inst, gpu) as CrewCls:
            CrewCls()

        registered = [c.args[0].name for c in dir_inst.register.call_args_list]
        assert registered == list(BOOT_ORDER)
