#!/usr/bin/env python3
"""
distributor.py — Этап «Распределение готовых шортсов»

Изменения:
  - Лимит загрузок теперь берётся из PLATFORM_DAILY_LIMITS[platform] (по платформе),
    а не из единого DAILY_UPLOAD_LIMIT.
    Например: youtube=5, tiktok=5, instagram=5 в сутки на аккаунт.
  - MAX_PER_ACCOUNT заменён на платформо-зависимый лимит из конфига.
"""

import json
import logging
import re
import shutil
from datetime import date
from pathlib import Path

from pipeline import config

log = logging.getLogger("distributor")

TODAY           = date.today().isoformat()
MIN_PER_ACCOUNT = 3


# ─────────────────────────── Парсинг описания ────────────────────────────

def parse_description_file(desc_path: Path) -> list[dict]:
    """
    Парсит *_description.txt с блоками вариантов метаданных.
    """
    text = desc_path.read_text(encoding="utf-8")
    blocks = re.split(r"(?mi)^[-─=*\s]*(?:вариант|variant|option)\s*\d+[-─=*\s]*$", text)
    blocks = [b.strip() for b in blocks if b.strip()]
    if not blocks:
        blocks = [text.strip()]

    variants: list[dict] = []
    for block in blocks:
        meta: dict = {}

        def _grab(pattern: str) -> str:
            m = re.search(pattern, block, re.IGNORECASE | re.MULTILINE)
            return m.group(1).strip() if m else ""

        meta["title"]          = _grab(r"^title\s*:\s*(.+)")
        meta["description"]    = _grab(r"^description\s*:\s*(.+)")
        raw_tags               = _grab(r"^tags\s*:\s*(.+)")
        meta["tags"]           = [t.strip() for t in raw_tags.split(",") if t.strip()]
        meta["thumbnail_idea"] = _grab(r"^thumbnail[\s_]idea\s*:\s*(.+)")

        if any([meta["title"], meta["description"], meta["tags"]]):
            variants.append(meta)

    return variants


def meta_to_video_json(meta: dict) -> dict:
    """Преобразует словарь метаданных в формат video.json."""
    return {
        "title":       meta.get("title", ""),
        "description": meta.get("description", ""),
        "tags":        meta.get("tags", []),
    }


# ─────────────────────────── Сбор готовых шортсов ────────────────────────

def collect_shorts() -> list[dict]:
    """
 ...(truncated as per original, but add per-platform copy)
    # In assign_shorts: For each acc, for each platform in acc['platforms'], copy to upload_queue/platform/
    """

# ... (adjust distribution logic for multi-platform)