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

MIN_PER_ACCOUNT = 3

# JSON-файл для отслеживания, на какие платформы распределено каждое видео
DISTRIBUTED_TRACKING_FILE = config.BASE_DIR / "data" / "distributed_tracking.json"


def _load_distributed_tracking() -> dict:
    """Загружает таблицу распределения. Структура: {video_stem: {platform: bool}}"""
    if not DISTRIBUTED_TRACKING_FILE.exists():
        return {}
    try:
        return json.loads(DISTRIBUTED_TRACKING_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_distributed_tracking(data: dict) -> None:
    DISTRIBUTED_TRACKING_FILE.parent.mkdir(parents=True, exist_ok=True)
    DISTRIBUTED_TRACKING_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _get_active_platforms() -> set:
    """
    Возвращает множество платформ, для которых существует хотя бы один аккаунт.
    Именно они считаются «обязательными» для распределения перед удалением из OUTPUT_DIR.
    """
    accounts_root = Path(config.ACCOUNTS_ROOT)
    active: set = set()
    if not accounts_root.exists():
        return active
    for acc_dir in accounts_root.iterdir():
        cfg_path = acc_dir / "config.json"
        if acc_dir.is_dir() and cfg_path.exists():
            try:
                acc_cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
                platforms = acc_cfg.get("platforms", [acc_cfg.get("platform", "youtube")])
                if isinstance(platforms, str):
                    platforms = [platforms]
                active.update(platforms)
            except Exception:
                pass
    return active or set(config.ALL_PLATFORMS)


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
    """Собирает готовые шортсы из OUTPUT_DIR (видео + JSON-метаданные)."""
    shorts = []
    if not config.OUTPUT_DIR.exists():
        return shorts
    for video_path in sorted(config.OUTPUT_DIR.rglob("*.mp4")):
        json_path = video_path.with_suffix(".json")
        meta = {}
        if json_path.exists():
            try:
                meta = json.loads(json_path.read_text(encoding="utf-8"))
            except Exception:
                pass
        shorts.append({"video_path": video_path, "meta": meta})
    return shorts


def distribute_shorts(dry_run: bool = False) -> None:
    """
    Распределяет готовые шортсы по папкам upload_queue/<platform>/
    для каждого аккаунта с учётом дневных лимитов по платформам.

    После того как видео распределено во все очереди активных платформ,
    оно удаляется из OUTPUT_DIR — копии уже в очередях на загрузку.
    Архивирование исходника из preparing_shorts/ выполняет finalize.py
    после подтверждения реальной загрузки на все платформы.
    """
    today             = date.today().isoformat()
    shorts            = collect_shorts()
    active_platforms  = _get_active_platforms()
    dist_tracking     = _load_distributed_tracking()

    if not shorts:
        log.warning("Нет готовых шортсов в %s", config.OUTPUT_DIR)
        return

    accounts_root = Path(config.ACCOUNTS_ROOT)
    if not accounts_root.exists():
        log.error("Папка аккаунтов не найдена: %s", accounts_root)
        return

    accounts = [
        d for d in accounts_root.iterdir()
        if d.is_dir() and (d / "config.json").exists()
    ]
    if not accounts:
        log.warning("Аккаунты не найдены в %s", accounts_root)
        return

    short_idx = 0
    for acc_dir in accounts:
        acc_cfg_path = acc_dir / "config.json"
        try:
            acc_cfg = json.loads(acc_cfg_path.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning("Не удалось прочитать конфиг аккаунта %s: %s", acc_dir.name, e)
            continue

        platforms = acc_cfg.get("platforms", [acc_cfg.get("platform", "youtube")])
        if isinstance(platforms, str):
            platforms = [platforms]

        for platform in platforms:
            daily_limit = config.PLATFORM_DAILY_LIMITS.get(platform, config.DAILY_UPLOAD_LIMIT)
            queue_dir = acc_dir / "upload_queue" / platform
            queue_dir.mkdir(parents=True, exist_ok=True)

            existing = len(list(queue_dir.glob("*.mp4")))
            slots = max(0, daily_limit - existing)
            if slots == 0:
                log.info(
                    "[%s][%s] Очередь полная (%d/%d).",
                    acc_dir.name, platform, existing, daily_limit,
                )
                continue

            assigned = 0
            while assigned < slots and short_idx < len(shorts):
                item = shorts[short_idx]
                short_idx += 1
                video_src = item["video_path"]
                meta      = item["meta"]
                stem      = video_src.stem

                dest_video = queue_dir / video_src.name
                dest_meta  = queue_dir / f"{video_src.stem}_meta.json"

                if dry_run:
                    log.info("[dry_run] %s -> %s/%s", video_src.name, platform, acc_dir.name)
                    dist_tracking.setdefault(stem, {p: False for p in active_platforms})
                    dist_tracking[stem][platform] = True
                else:
                    try:
                        shutil.copy2(video_src, dest_video)
                        dest_meta.write_text(
                            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
                        )
                        log.info(
                            "Распределено: %s -> %s/%s",
                            video_src.name, acc_dir.name, platform,
                        )
                        dist_tracking.setdefault(stem, {p: False for p in active_platforms})
                        dist_tracking[stem][platform] = True
                    except Exception as e:
                        log.error("Ошибка копирования %s: %s", video_src.name, e)
                        continue

                assigned += 1

    if not dry_run:
        _save_distributed_tracking(dist_tracking)

    # Удаляем из OUTPUT_DIR видео, которые распределены на все активные платформы
    deleted = 0
    for stem, platform_map in dist_tracking.items():
        all_covered = all(platform_map.get(p, False) for p in active_platforms)
        if not all_covered:
            missing = [p for p in active_platforms if not platform_map.get(p, False)]
            log.debug("Пропуск удаления %s — ещё не распределено на: %s", stem, ", ".join(missing))
            continue

        for ext in config.VIDEO_EXT:
            video_file = config.OUTPUT_DIR / f"{stem}{ext}"
            if video_file.exists():
                if dry_run:
                    log.info("[dry_run] Удалено бы из OUTPUT_DIR: %s", video_file.name)
                else:
                    try:
                        video_file.unlink()
                        json_file = video_file.with_suffix(".json")
                        if json_file.exists():
                            json_file.unlink()
                        log.info("Удалено из OUTPUT_DIR (все платформы покрыты): %s", video_file.name)
                        deleted += 1
                    except Exception as exc:
                        log.error("Не удалось удалить %s: %s", video_file, exc)
                break

    log.info(
        "Распределение завершено. Использовано шортсов: %d / %d. Удалено из OUTPUT_DIR: %d.",
        short_idx, len(shorts), deleted,
    )