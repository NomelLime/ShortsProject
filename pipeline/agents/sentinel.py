"""pipeline/agents/sentinel.py — SENTINEL: мониторинг системы, алерты, авто-восстановление."""
from __future__ import annotations
import logging
from typing import Any, Optional
from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

class Sentinel(BaseAgent):
    def __init__(self, memory: Optional[AgentMemory] = None, notify: Any = None) -> None:
        super().__init__("SENTINEL", memory or get_memory(), notify)
        self._interval_sec = 120  # каждые 2 минуты

    def run(self) -> None:
        logger.info("[SENTINEL] Мониторинг запущен")
        while not self.should_stop:
            self._monitor()
            self.sleep(self._interval_sec)

    def _monitor(self) -> None:
        self._set_status(AgentStatus.RUNNING, "мониторинг")
        try:
            import psutil
            cpu = psutil.cpu_percent(interval=1)
            ram = psutil.virtual_memory().percent
            if cpu > 90:
                self._send(f"⚠️ [SENTINEL] Высокая нагрузка CPU: {cpu:.0f}%")
            if ram > 85:
                self._send(f"⚠️ [SENTINEL] Высокая нагрузка RAM: {ram:.0f}%")
            self.memory.set("system_metrics", {"cpu": cpu, "ram": ram}, persist=False)
        except ImportError:
            pass  # psutil не установлен — тихо пропускаем
        except Exception as e:
            logger.debug("[SENTINEL] Ошибка мониторинга: %s", e)
        finally:
            self._set_status(AgentStatus.IDLE)
