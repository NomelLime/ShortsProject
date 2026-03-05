"""
pipeline/agents/base_agent.py — Базовый класс для всех агентов ShortsProject.

Каждый агент:
  - запускается в отдельном потоке через start() / stop()
  - имеет доступ к AgentMemory (self.memory)
  - автоматически регистрирует статус в памяти
  - поддерживает прерываемый sleep() (stop() будит поток)
  - отправляет Telegram-уведомления через self._send()
  - сохраняет отчёты через self.report()
"""
from __future__ import annotations

import logging
import threading
import time
import traceback
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, Callable, Dict, Optional


class AgentStatus(Enum):
    IDLE    = "IDLE"
    RUNNING = "RUNNING"
    WAITING = "WAITING"   # ждёт GPU или ресурса
    ERROR   = "ERROR"
    STOPPED = "STOPPED"


class BaseAgent(ABC):
    """
    Базовый класс для всех агентов.

    Подклассы обязаны реализовать run().
    run() вызывается внутри потока, запущенного через start().

    Паттерн использования в run():
        def run(self) -> None:
            while not self.should_stop:
                self._set_status(AgentStatus.RUNNING, "работа")
                # ... логика ...
                if not self.sleep(interval):
                    break
    """

    def __init__(
        self,
        name: str,
        memory=None,       # AgentMemory — опционально
        notify: Any = None,
    ) -> None:
        self.name          = name
        self.memory        = memory
        self._notify       = notify
        self.status        = AgentStatus.IDLE
        self._last_error: Optional[str] = None
        self._start_time:  Optional[float] = None

        self._stop_event   = threading.Event()
        self._thread:      Optional[threading.Thread] = None

        self.logger = logging.getLogger(f"agent.{name.lower()}")

        # Регистрируем в памяти при создании
        if self.memory:
            self.memory.register_agent(name)

    # ------------------------------------------------------------------
    # Абстрактный метод
    # ------------------------------------------------------------------

    @abstractmethod
    def run(self) -> None:
        """Основная логика агента. Запускается в отдельном потоке."""
        ...

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Запустить агента в фоновом потоке."""
        if self._thread and self._thread.is_alive():
            self.logger.warning("[%s] Уже запущен", self.name)
            return
        self._stop_event.clear()
        self._start_time = time.monotonic()
        self._thread = threading.Thread(
            target=self._run_wrapper,
            name=f"agent-{self.name.lower()}",
            daemon=True,
        )
        self._thread.start()
        self.logger.info("[%s] Поток запущен", self.name)

    def stop(self, timeout: float = 10.0) -> None:
        """Запросить остановку и дождаться завершения потока."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        self._set_status(AgentStatus.STOPPED)
        self.logger.info("[%s] Остановлен", self.name)

    def _run_wrapper(self) -> None:
        """Обёртка: перехватывает ошибки, обновляет статус."""
        try:
            self.run()
        except Exception as exc:
            self._last_error = str(exc)
            self._set_status(AgentStatus.ERROR, str(exc)[:120])
            self.logger.error(
                "[%s] Необработанная ошибка: %s\n%s",
                self.name, exc, traceback.format_exc(),
            )

    # ------------------------------------------------------------------
    # Управление потоком
    # ------------------------------------------------------------------

    @property
    def should_stop(self) -> bool:
        """True если получен сигнал на остановку."""
        return self._stop_event.is_set()

    def sleep(self, seconds: float) -> bool:
        """
        Прерываемый sleep. Возвращает False если был вызван stop().

        Использование:
            if not self.sleep(60):
                return  # агент остановлен
        """
        return not self._stop_event.wait(timeout=seconds)

    def get_uptime(self) -> Optional[float]:
        """Uptime агента в секундах или None если не запущен."""
        if self._start_time is None:
            return None
        return round(time.monotonic() - self._start_time, 1)

    # ------------------------------------------------------------------
    # Статус и отчётность
    # ------------------------------------------------------------------

    def _set_status(self, status: AgentStatus, detail: str = "") -> None:
        """Обновить статус + записать в AgentMemory."""
        self.status = status
        if self.memory:
            status_str = status.value if not detail else f"{status.value}: {detail}"
            self.memory.set_agent_status(self.name, status_str)

    def report(self, data: Dict[str, Any]) -> None:
        """Сохранить произвольный отчёт агента в AgentMemory."""
        if self.memory:
            self.memory.set_agent_report(self.name, data)

    def _send(self, message: str) -> None:
        """Отправить Telegram-уведомление (если notify задан)."""
        try:
            if callable(self._notify):
                self._notify(message)
            else:
                from pipeline.notifications import send_telegram
                send_telegram(message)
        except Exception as e:
            self.logger.debug("[%s] Уведомление не отправлено: %s", self.name, e)

    def __repr__(self) -> str:
        return f"<Agent {self.name} [{self.status.value}]>"
