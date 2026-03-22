"""
Возврат роликов из archive/ в Ready-made_shorts_with_description для повторного
распределения (например после окончания прогрева, если копий в очередях не было).

Запуск из корня ShortsProject:
  python -m pipeline.redistribute_from_archive --days 7
"""

from __future__ import annotations

import argparse
import logging
import shutil
from pathlib import Path

from pipeline import config

logger = logging.getLogger(__name__)


def redistribute_from_archive(days: int = 7, dry_run: bool = False) -> int:
    """
    Копирует *.mp4 из archive/<дата>/ за последние `days` календарных папок
    в OUTPUT_DIR, если файла с таким именем там ещё нет.

    Возвращает число скопированных файлов.
    """
    arch = config.ARCHIVE_DIR
    out = config.OUTPUT_DIR
    if not arch.exists():
        logger.warning("Архив не найден: %s", arch)
        return 0
    out.mkdir(parents=True, exist_ok=True)

    day_dirs = sorted(
        [d for d in arch.iterdir() if d.is_dir()],
        key=lambda p: p.name,
        reverse=True,
    )[: max(1, days)]

    copied = 0
    for day_dir in day_dirs:
        for src in day_dir.glob("*.mp4"):
            dest = out / src.name
            if dest.exists():
                continue
            if dry_run:
                logger.info("[dry_run] скопировать бы: %s -> %s", src, dest)
            else:
                shutil.copy2(src, dest)
                logger.info("Скопировано в OUTPUT: %s (из %s)", src.name, day_dir.name)
            copied += 1
    return copied


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    p = argparse.ArgumentParser(description="Копирование .mp4 из archive/ в OUTPUT")
    p.add_argument("--days", type=int, default=7, help="Сколько последних папок по дате обойти")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    n = redistribute_from_archive(days=args.days, dry_run=args.dry_run)
    print(f"Готово. Файлов: {n}")


if __name__ == "__main__":
    main()
