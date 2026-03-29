"""
pipeline/pipeline_state.py — чекпоинты этапов пайплайна для resume и управления Orchestrator.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

from pipeline.config import BASE_DIR

STATE_FILE = BASE_DIR / "data" / "pipeline_state.json"
STAGE_ORDER = ["search", "download", "processing", "distribute", "upload", "finalize"]


def load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {"stages": {}}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"stages": {}}


def _atomic_write(data: dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, STATE_FILE)


def set_current_stage(stage: str) -> None:
    """Помечает текущий этап (для внешних координаторов, например Orchestrator LLM)."""
    state = load_state()
    state["current_stage"] = stage
    _atomic_write(state)


def save_stage_result(stage: str, success: bool, detail: dict[str, Any] | None = None) -> None:
    state = load_state()
    if "run_id" not in state:
        state["run_id"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    if "stages" not in state:
        state["stages"] = {}
    if "started_at" not in state:
        state["started_at"] = int(time.time())

    now = int(time.time())
    prev = state["stages"].get(stage, {})
    attempts = int(prev.get("attempts") or 0) + 1
    state["stages"][stage] = {
        "status": "done" if success else "failed",
        "finished_at": now,
        "attempts": attempts,
        "detail": detail or {},
    }
    state["last_completed"] = stage if success else state.get("last_completed")
    state["current_stage"] = stage

    if success and stage == STAGE_ORDER[-1]:
        state["finished_at"] = now
    elif not success:
        state["finished_at"] = None

    _atomic_write(state)


def is_stage_done(stage: str) -> bool:
    st = load_state().get("stages", {}).get(stage, {})
    return st.get("status") == "done"


def get_next_stage() -> str | None:
    # [FIX] Один вызов load_state() вместо до 6 (по одному на каждый этап)
    state = load_state()
    stages = state.get("stages", {})
    for s in STAGE_ORDER:
        if stages.get(s, {}).get("status") != "done":
            return s
    return None


def get_stage_attempts(stage: str) -> int:
    st = load_state().get("stages", {}).get(stage, {})
    return int(st.get("attempts") or 0)


def reset_state() -> None:
    """Новый цикл: сброс состояния."""
    data = {
        "run_id": str(uuid.uuid4())[:8] + "-" + time.strftime("%Y%m%d-%H%M%S"),
        "stages": {},
        "last_completed": None,
        "started_at": int(time.time()),
        "finished_at": None,
        "current_stage": None,
    }
    _atomic_write(data)
