"""
pipeline/agents/base_agent.py — Базовый класс для всех агентов ShortsProject.

Каждый агент:
  - имеет имя, роль и доступ к общей памяти (AgentMemory)
  - логирует все действия с меткой агента
  - может отправлять Telegram-уведомления
  - регистрирует свой статус в AgentMemory
  - перехватывает и обрабатывает ошибки стандартным образом
"""

from __future__ import annotations

import logging
import time
import traceback
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum, auto
from typing import Any, Dict, Optional

from pipeline.notifications import send_telegram


class AgentStatus(Enum):
    IDLE    = auto()
    RUNNING = auto()
    SUCCESS = auto()
    FAILED  = auto()
    WAITING = auto()


class BaseAgent(ABC):
    """
    Базовый класс для всех агентов.

    Подклассы обязаны реализовать метод run().
    Все вызовы run() проходят через execute() — он обеспечивает
    логирование, трекинг статуса и обработку ошибок.
    """

    def __init__(
        self,
        name: str,
        role: str,
        memory=None,          # AgentMemory — опционально во избежание цикличного импорта
        notify_on_fail: bool = True,
        notify_on_success: bool = False,
    ) -> None:
        self.name             = name
        self.role             = role
        self._memory          = memory
        self.notify_on_fail   = notify_on_fail
        self.notify_on_success = notify_on_success
        self.status           = AgentStatus.IDLE
        self.last_result: Any = None
        self.last_error: Optional[str] = None

        self.logger = logging.getLogger(f"agent.{name.lower()}")

    # ── Абстрактный метод ────────────────────────────────────────────────────

    @abstractmethod
    def run(self, **kwargs) -> Any:
        """Основная логика агента. Реализуется в подклассах."""
        ...

    # ── Публичный интерфейс ──────────────────────────────────────────────────

    def execute(self, **kwargs) -> Any:
        """
        Запускает агента с полным lifecycle:
          1. Обновляет статус → RUNNING
          2. Вызывает run(**kwargs)
          3. Обновляет статус → SUCCESS / FAILED
          4. Логирует результат и время выполнения
          5. Уведомляет Telegram при ошибке (если notify_on_fail)
        """
        self._set_status(AgentStatus.RUNNING)
        self._log_memory_event("start", kwargs)
        start_ts = time.monotonic()

        try:
            result = self.run(**kwargs)
            elapsed = time.monotonic() - start_ts

            self.last_result = result
            self.last_error  = None
            self._set_status(AgentStatus.SUCCESS)
            self._log_memory_event("success", {"elapsed_sec": round(elapsed, 2)})

            self.logger.info(
                "[%s] ✅ Выполнено за %.1f сек.", self.name, elapsed
            )
            if self.notify_on_success:
                send_telegram(
                    f"✅ <b>[{self.name}]</b> задача выполнена за {elapsed:.1f}с."
                )
            return result

        except Exception as exc:
            elapsed = time.monotonic() - start_ts
            tb      = traceback.format_exc()
            self.last_error = str(exc)
            self._set_status(AgentStatus.FAILED)
            self._log_memory_event("error", {"error": str(exc), "elapsed_sec": round(elapsed, 2)})

            self.logger.error(
                "[%s] ❌ Ошибка за %.1f сек.: %s\n%s",
                self.name, elapsed, exc, tb,
            )
            if self.notify_on_fail:
                send_telegram(
                    f"❌ <b>[{self.name}]</b> ошибка:\n<code>{str(exc)[:400]}</code>"
                )
            raise

    def report(self) -> Dict:
        """Возвращает текущий статус агента в виде словаря."""
        return {
            "name":        self.name,
            "role":        self.role,
            "status":      self.status.name,
            "last_error":  self.last_error,
            "last_result": str(self.last_result)[:200] if self.last_result else None,
        }

    # ── Вспомогательные методы ───────────────────────────────────────────────

    def _set_status(self, status: AgentStatus) -> None:
        self.status = status
        if self._memory:
            self._memory.set_agent_status(self.name, status.name)

    def _log_memory_event(self, event: str, data: Dict) -> None:
        if self._memory:
            self._memory.log_event(
                agent=self.name,
                event=event,
                data=data,
                ts=datetime.now().isoformat(timespec="seconds"),
            )

    def _get_memory(self, key: str, default: Any = None) -> Any:
        if self._memory:
            return self._memory.get(key, default)
        return default

    def _set_memory(self, key: str, value: Any) -> None:
        if self._memory:
            self._memory.set(key, value)

    def __repr__(self) -> str:
        return f"<Agent {self.name} [{self.status.name}]>"
