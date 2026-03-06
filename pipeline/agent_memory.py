"""
pipeline/agent_memory.py — Общее состояние всех агентов ShortsProject.

AgentMemory — потокобезопасное хранилище ключ-значение с персистентностью
(сохраняется в data/agent_memory.json).

Структура:
  {
    "kv": {                         ← произвольные ключи-значения
      "pipeline_phase": "upload",
      "active_accounts": 5,
      ...
    },
    "agents": {                     ← статусы агентов
      "SCOUT": "IDLE",
      "GUARDIAN": "RUNNING",
      ...
    },
    "events": [                     ← лог событий (последние 500)
      {"ts": "...", "agent": "SCOUT", "event": "start", "data": {...}},
      ...
    ]
  }
"""

from __future__ import annotations

import json
import os
import tempfile
import logging
import threading
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

logger = logging.getLogger(__name__)

_MEMORY_FILE = Path(__file__).parent.parent / "data" / "agent_memory.json"
_MAX_EVENTS  = 500


class AgentMemory:
    """
    Потокобезопасное in-memory хранилище с персистентностью на диск.

    Использование:
        memory = AgentMemory()
        memory.set("current_phase", "processing")
        phase = memory.get("current_phase")
        memory.log_event("SCOUT", "found_urls", {"count": 42})
    """

    def __init__(self, persist_path: Path = _MEMORY_FILE) -> None:
        self._persist_path = persist_path
        self._lock         = threading.RLock()
        self._kv: Dict[str, Any]    = {}
        self._agents: Dict[str, str] = {}
        self._events: Deque[Dict]   = deque(maxlen=_MAX_EVENTS)
        self._load()

    # ── Базовые операции KV ──────────────────────────────────────────────────

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._kv.get(key, default)

    def set(self, key: str, value: Any, persist: bool = True) -> None:
        with self._lock:
            self._kv[key] = value
            if persist:
                self._save()

    def delete(self, key: str) -> None:
        with self._lock:
            self._kv.pop(key, None)
            self._save()

    def get_all_kv(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._kv)

    # ── Статусы агентов ──────────────────────────────────────────────────────

    def register_agent(self, agent_name: str) -> None:
        """Регистрирует агента с начальным статусом IDLE."""
        with self._lock:
            if agent_name not in self._agents:
                self._agents[agent_name] = "IDLE"
                self._save()

    def set_agent_status(self, agent_name: str, status: str) -> None:
        """Обновляет статус агента в памяти (не сохраняет на диск — статусы транзиентны).
        KV-операции через set() по-прежнему персистятся при необходимости."""
        with self._lock:
            self._agents[agent_name] = status

    def set_agent_report(self, agent_name: str, data: Dict[str, Any]) -> None:
        """Сохраняет произвольный отчёт агента."""
        key = f"report_{agent_name.lower()}"
        with self._lock:
            self._kv[key] = {
                "ts": datetime.now().isoformat(timespec="seconds"),
                "data": data,
            }
            self._save()

    def get_agent_status(self, agent_name: str) -> Optional[str]:
        with self._lock:
            return self._agents.get(agent_name)

    def get_all_agent_statuses(self) -> Dict[str, str]:
        with self._lock:
            return dict(self._agents)

    # ── Лог событий ─────────────────────────────────────────────────────────

    def log_event(
        self,
        agent: str,
        event: str,
        data: Optional[Dict] = None,
        ts: Optional[str] = None,
    ) -> None:
        with self._lock:
            self._events.append({
                "ts":    ts or datetime.now().isoformat(timespec="seconds"),
                "agent": agent,
                "event": event,
                "data":  data or {},
            })
            # Не сохраняем при каждом event — только KV/статусы
            # (события читаются из памяти, не с диска)

    def get_events(
        self,
        agent: Optional[str] = None,
        last_n: int = 50,
    ) -> List[Dict]:
        with self._lock:
            events = list(self._events)
        if agent:
            events = [e for e in events if e["agent"] == agent]
        return events[-last_n:]

    # ── Сводный отчёт ────────────────────────────────────────────────────────

    def summary(self) -> Dict:
        with self._lock:
            return {
                "kv_keys":     list(self._kv.keys()),
                "agents":      dict(self._agents),
                "event_count": len(self._events),
                "last_events": list(self._events)[-10:],
            }

    # ── Персистентность ──────────────────────────────────────────────────────

    def _save(self) -> None:
        """Сохраняет KV и статусы агентов на диск атомично (без блокировки — вызывается внутри lock)."""
        try:
            self._persist_path.parent.mkdir(parents=True, exist_ok=True)
            data = {
                "kv":      self._kv,
                "agents":  self._agents,
                "saved_at": datetime.now().isoformat(timespec="seconds"),
            }
            text = json.dumps(data, ensure_ascii=False, indent=2)
            # Атомичная запись через временный файл — защита от коррупции при OOM/Ctrl+C
            fd, tmp = tempfile.mkstemp(dir=self._persist_path.parent, suffix=".tmp")
            try:
                os.write(fd, text.encode("utf-8"))
                os.close(fd)
                os.replace(tmp, str(self._persist_path))
            except Exception:
                try:
                    os.close(fd)
                except Exception:
                    pass
                try:
                    os.unlink(tmp)
                except Exception:
                    pass
                raise
        except Exception as exc:
            logger.warning("AgentMemory: не удалось сохранить на диск: %s", exc)

    def _load(self) -> None:
        if not self._persist_path.exists():
            return
        try:
            raw = json.loads(self._persist_path.read_text(encoding="utf-8"))
            self._kv     = raw.get("kv", {})
            self._agents = raw.get("agents", {})
            logger.info(
                "AgentMemory загружена: %d KV-ключей, %d агентов",
                len(self._kv), len(self._agents),
            )
        except Exception as exc:
            logger.warning("AgentMemory: не удалось загрузить с диска: %s", exc)

    def reset(self) -> None:
        """Полный сброс памяти (для тестов или ручного рестарта)."""
        with self._lock:
            self._kv.clear()
            self._agents.clear()
            self._events.clear()
            self._save()
        logger.info("AgentMemory сброшена.")


# Глобальный синглтон — все агенты используют один объект
_global_memory: Optional[AgentMemory] = None


def get_memory() -> AgentMemory:
    """Возвращает глобальный экземпляр AgentMemory (создаёт при первом вызове)."""
    global _global_memory
    if _global_memory is None:
        _global_memory = AgentMemory()
    return _global_memory
