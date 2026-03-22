#!/usr/bin/env python3
"""
Копирует examples/keywords.example.txt → data/keywords.txt для первичного запуска.

Запуск из корня ShortsProject:
    python scripts/init_keywords.py
    python scripts/init_keywords.py --force   # перезаписать существующий непустой файл
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples" / "keywords.example.txt"
TARGET = ROOT / "data" / "keywords.txt"


def _has_real_keywords(path: Path) -> bool:
    if not path.exists():
        return False
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if s and not s.startswith("#"):
            return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed data/keywords.txt from example template.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite even if keywords.txt already has entries.",
    )
    args = parser.parse_args()

    if not EXAMPLE.is_file():
        print(f"Шаблон не найден: {EXAMPLE}", file=sys.stderr)
        return 2

    if TARGET.exists() and _has_real_keywords(TARGET) and not args.force:
        print(f"Уже есть ключевые слова: {TARGET} (используйте --force для перезаписи)")
        return 0

    TARGET.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(EXAMPLE, TARGET)
    print(f"OK: {TARGET} <- {EXAMPLE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
