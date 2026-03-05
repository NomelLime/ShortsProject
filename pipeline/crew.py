"""
pipeline/crew.py — Сборка и запуск всей агентной системы ShortsProject.

Использование:
    from pipeline.crew import ShortsProjectCrew
    crew = ShortsProjectCrew()
    crew.start()
    crew.commander.handle_command("статус")
    crew.stop()
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from pipeline.agent_memory import AgentMemory, get_memory
from pipeline.agents.gpu_manager import get_gpu_manager
from pipeline.agents.director   import Director
from pipeline.agents.commander  import Commander
from pipeline.agents.scout      import Scout
from pipeline.agents.curator    import Curator
from pipeline.agents.visionary  import Visionary
from pipeline.agents.narrator   import Narrator
from pipeline.agents.editor     import Editor
from pipeline.agents.strategist import Strategist
from pipeline.agents.guardian   import Guardian
from pipeline.agents.publisher  import Publisher
from pipeline.agents.accountant import Accountant
from pipeline.agents.sentinel   import Sentinel

logger = logging.getLogger(__name__)


class ShortsProjectCrew:
    """
    Полная система из 12 агентов.

    После инициализации:
      crew.start()                              → запустить всё
      crew.commander.handle_command("статус")   → команда
      crew.stop()                               → остановить всё
    """

    def __init__(
        self,
        notify: Optional[Callable[[str], None]] = None,
        auto_confirm: bool = False,
    ) -> None:
        self.memory  = get_memory()
        self.gpu     = get_gpu_manager()
        self._notify = notify

        # Инициализируем агентов
        self.director   = Director(memory=self.memory, notify=notify)
        self.commander  = Commander(
            director=self.director,
            memory=self.memory,
            notify=notify,
            auto_confirm=auto_confirm,
        )

        # Операционные агенты
        self.scout      = Scout(memory=self.memory,      notify=notify)
        self.curator    = Curator(memory=self.memory,    notify=notify)
        self.visionary  = Visionary(memory=self.memory,  notify=notify)
        self.narrator   = Narrator(memory=self.memory,   notify=notify)
        self.editor     = Editor(memory=self.memory,     notify=notify)
        self.strategist = Strategist(memory=self.memory, notify=notify)
        self.guardian   = Guardian(memory=self.memory,   notify=notify)
        self.publisher  = Publisher(memory=self.memory,  notify=notify)
        self.accountant = Accountant(memory=self.memory, notify=notify)
        self.sentinel   = Sentinel(memory=self.memory,   notify=notify)

        # Регистрируем в Director
        for agent in [
            self.sentinel, self.scout, self.curator, self.visionary,
            self.narrator, self.editor, self.strategist, self.guardian,
            self.publisher, self.accountant,
        ]:
            self.director.register(agent)

        logger.info("[CREW] Инициализировано 12 агентов")

    def start(self) -> None:
        """Запустить всю систему."""
        logger.info("[CREW] Запуск системы...")
        self.gpu.start()
        self.director.start()
        self.commander.start()
        self.director.start_all()
        if self._notify:
            self._notify("🚀 ShortsProject запущен! Все агенты активны.")
        logger.info("[CREW] Система запущена ✓")

    def stop(self) -> None:
        """Остановить всю систему."""
        logger.info("[CREW] Остановка системы...")
        self.director.stop_all()
        self.commander.stop()
        self.director.stop()
        self.gpu.stop()
        logger.info("[CREW] Система остановлена ✓")

    def status(self) -> dict:
        """Быстрый статус всей системы."""
        return self.director.full_status()

    def command(self, text: str) -> str:
        """Отправить команду через COMMANDER."""
        return self.commander.handle_command(text)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()
