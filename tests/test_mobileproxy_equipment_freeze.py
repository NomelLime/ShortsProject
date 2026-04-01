"""Тесты заморозки линий mobileproxy (без сети)."""

from __future__ import annotations

import json
import time
from pathlib import Path
from pipeline import config
from pipeline.mobileproxy_equipment_freeze import (
    equipment_freeze_key,
    freeze_invalid_equipment,
    is_equipment_frozen,
)


def test_equipment_freeze_key_stable():
    assert equipment_freeze_key(12, {"geoid": 100, "eid": 5, "proxy_operator": "Op"}) == "12:100:5:op"
    assert equipment_freeze_key(12, {"geoid": "100", "eid": None}) == "12:100:0:"
    assert equipment_freeze_key(12, {}) is None
    assert equipment_freeze_key(12, {"eid": 1}) is None


def test_freeze_and_thaw(tmp_path: Path, monkeypatch):
    f = tmp_path / "fz.json"
    monkeypatch.setattr(config, "MOBILEPROXY_INVALID_EQUIPMENT_FREEZE_FILE", f)
    monkeypatch.setattr(config, "MOBILEPROXY_INVALID_EQUIPMENT_FREEZE_HOURS", 24.0)

    row = {"geoid": 1, "eid": 2, "operator": "X"}
    freeze_invalid_equipment(7, row)
    key = equipment_freeze_key(7, row)
    assert key
    assert is_equipment_frozen(7, row)

    data = json.loads(f.read_text(encoding="utf-8"))
    assert key in data["frozen"]
    # истечение
    data["frozen"][key] = time.time() - 1.0
    f.write_text(json.dumps(data), encoding="utf-8")
    assert not is_equipment_frozen(7, row)
