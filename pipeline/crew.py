"""
pipeline/crew.py — Сборка и запуск всей агентной системы ShortsProject.

Использование:
    from pipeline.crew import ShortsProjectCrew

    # Запуск
    crew = ShortsProjectCrew()
    crew.start()

    # Команда от пользователя
    reply = crew.command("статус")

    # Context manager
    with ShortsProjectCrew() as crew:
        crew.command("добавь 5 аккаунтов tiktok")

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
from pipeline.agents.metrics_scout_platform import MetricsScoutPlatform
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
    Полная система из 13 агентов.

    Агенты знают друг о друге там где нужна координация:
      - EDITOR    знает VISIONARY (генерация мета)
      - PUBLISHER знает GUARDIAN  (карантин) и ACCOUNTANT (лимиты)
      - COMMANDER знает DIRECTOR  (делегирование)
      - DIRECTOR  знает всех      (запуск/стоп/watchdog)
    """

    def __init__(
        self,
        notify: Optional[Callable[[str], None]] = None,
        auto_confirm: bool = False,
    ) -> None:
        self.memory  = get_memory()
        self.gpu     = get_gpu_manager()
        self._notify = notify

        # ── Инициализация агентов ─────────────────────────────────────
        self.sentinel   = Sentinel(memory=self.memory,   notify=notify)
        self.scout      = Scout(memory=self.memory,      notify=notify)
        self.metrics_scout_platform = MetricsScoutPlatform(memory=self.memory, notify=notify)
        self.curator    = Curator(memory=self.memory,    notify=notify)
        self.visionary  = Visionary(memory=self.memory,  notify=notify)
        self.narrator   = Narrator(memory=self.memory,   notify=notify)
        self.guardian   = Guardian(memory=self.memory,   notify=notify)
        self.accountant = Accountant(memory=self.memory, notify=notify)
        self.strategist = Strategist(memory=self.memory, notify=notify)

        # EDITOR знает VISIONARY (мета) и NARRATOR (TTS)
        self.editor = Editor(
            memory=self.memory,
            notify=notify,
            visionary=self.visionary,
            narrator=self.narrator,
        )

        # PUBLISHER знает GUARDIAN и ACCOUNTANT
        self.publisher = Publisher(
            memory=self.memory,
            notify=notify,
            guardian=self.guardian,
            accountant=self.accountant,
        )

        # DIRECTOR оркестрирует всех
        self.director = Director(memory=self.memory, notify=notify)

        # COMMANDER — интерфейс пользователя → DIRECTOR
        self.commander = Commander(
            director=self.director,
            memory=self.memory,
            notify=notify,
            auto_confirm=auto_confirm,
        )

        # ── Регистрация в DIRECTOR (порядок = порядок запуска) ────────
        for agent in [
            self.sentinel,    # первым — мониторинг
            self.scout,       # поиск контента
            self.metrics_scout_platform,  # нативные метрики платформ
            self.curator,     # фильтрация
            self.visionary,   # AI метаданные
            self.narrator,    # TTS
            self.editor,      # монтаж
            self.strategist,  # аналитика
            self.guardian,    # безопасность
            self.publisher,   # загрузка
            self.accountant,  # лимиты
        ]:
            self.director.register(agent)

        logger.info("[CREW] 13 агентов инициализированы")

    # ------------------------------------------------------------------
    # Управление системой
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Запустить всю систему."""
        logger.info("[CREW] Запуск системы...")
        self.gpu.start()
        self.commander.start()     # интерфейс первый (принимает команды)
        self.director.start_all()  # запускаем всех агентов
        self.director.start()      # watchdog стартует последним — реестр уже заполнен

        if self._notify:
            self._notify(
                "🚀 <b>ShortsProject запущен!</b>\n"
                "13 агентов активны. Напиши <code>статус</code> для проверки."
            )
        logger.info("[CREW] Система запущена ✓")

    def stop(self) -> None:
        """Остановить всю систему."""
        logger.info("[CREW] Остановка системы...")
        self.director.stop_all()
        self.commander.stop()
        self.director.stop()
        self.gpu.stop()
        logger.info("[CREW] Система остановлена ✓")

    def command(self, text: str) -> str:
        """Отправить команду через COMMANDER."""
        return self.commander.handle_command(text)

    def status(self) -> dict:
        """Быстрый статус всей системы."""
        return self.director.full_status()

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *_):
        self.stop()
