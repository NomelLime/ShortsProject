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
    Возвращает список записей:
    { "mp4": Path, "meta": dict, "source": str }

    Поддерживает два формата метаданных:
      1) Файл *_description.txt (старый формат)
      2) Файл *.json рядом с .mp4 (новый формат, создаётся cloner)
    """
    shorts: list[dict] = []
    ready_dir = config.OUTPUT_DIR

    if not ready_dir.exists():
        log.warning("Папка %s не найдена — нет видео для распределения.", ready_dir)
        return shorts

    for source_dir in sorted(ready_dir.iterdir()):
        if not source_dir.is_dir():
            continue

        for mp4_path in sorted(source_dir.glob("*.mp4")):
            # Пробуем JSON (приоритет — cloner создаёт его)
            json_path = mp4_path.with_suffix(".json")
            if json_path.exists():
                try:
                    meta = json.loads(json_path.read_text(encoding="utf-8"))
                    if meta.get("title") or meta.get("description"):
                        shorts.append({
                            "mp4":    mp4_path,
                            "meta":   meta,
                            "source": source_dir.name,
                        })
                        continue
                except json.JSONDecodeError:
                    pass

            # Fallback: старый формат *_description.txt
            stem     = mp4_path.stem
            desc_path = mp4_path.with_name(stem + "_description.txt")
            if desc_path.exists():
                variants = parse_description_file(desc_path)
                if variants:
                    shorts.append({
                        "mp4":    mp4_path,
                        "meta":   variants[0],
                        "source": source_dir.name,
                    })
                    continue

            log.warning("Нет метаданных для %s — пропускаем.", mp4_path.name)

    log.info("Собрано готовых шортсов: %d", len(shorts))
    return shorts


# ─────────────────────────── Работа с аккаунтами ─────────────────────────

def _get_platform_limit(platform: str) -> int:
    """Возвращает дневной лимит загрузок для данной платформы."""
    return config.PLATFORM_DAILY_LIMITS.get(platform.lower(), config.DAILY_UPLOAD_LIMIT)


def load_accounts() -> list[dict]:
    """
    Сканирует папку аккаунтов. Для каждого считывает config.json
    и daily_limit.json. Лимит определяется по платформе аккаунта.
    """
    accounts: list[dict] = []
    accounts_dir = Path(config.ACCOUNTS_ROOT)

    if not accounts_dir.exists():
        log.error("Папка %s не найдена!", accounts_dir)
        return accounts

    for acc_dir in sorted(accounts_dir.iterdir()):
        if not acc_dir.is_dir():
            continue

        config_path = acc_dir / "config.json"
        acc_config: dict = {}
        if config_path.exists():
            try:
                acc_config = json.loads(config_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as e:
                log.warning("Ошибка чтения %s: %s", config_path, e)

        platform = acc_config.get("platform", "youtube").lower()

        limit_path = acc_dir / "daily_limit.json"
        daily_limit_data: dict = {}
        if limit_path.exists():
            try:
                daily_limit_data = json.loads(limit_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as e:
                log.warning("Ошибка чтения %s: %s", limit_path, e)

        # Лимит: сначала из файла (если переопределён), затем из PLATFORM_DAILY_LIMITS
        platform_limit  = _get_platform_limit(platform)
        daily_limit     = int(daily_limit_data.get("limit", platform_limit))
        uploaded_today  = int(daily_limit_data.get("uploaded_today", {}).get(TODAY, 0))
        # Слоты: сколько ещё можно загрузить сегодня (но не больше platform_limit за раз)
        slots_left      = min(max(0, daily_limit - uploaded_today), platform_limit)

        queue_dir = acc_dir / "upload_queue"
        queue_dir.mkdir(parents=True, exist_ok=True)

        accounts.append({
            "name":           acc_dir.name,
            "dir":            acc_dir,
            "queue_dir":      queue_dir,
            "platform":       platform,
            "daily_limit":    daily_limit,
            "platform_limit": platform_limit,
            "uploaded":       uploaded_today,
            "slots_left":     slots_left,
            "limit_path":     limit_path,
            "limit_data":     daily_limit_data,
        })

    log.info("Загружено аккаунтов: %d", len(accounts))
    # Выводим сводку по платформам
    by_platform: dict[str, int] = {}
    for a in accounts:
        by_platform[a["platform"]] = by_platform.get(a["platform"], 0) + 1
    for p, cnt in by_platform.items():
        limit = _get_platform_limit(p)
        log.info("  Платформа %-12s — аккаунтов: %d | лимит/аккаунт: %d/сутки", p, cnt, limit)

    return accounts


# ─────────────────────────── Распределение ───────────────────────────────

def distribute_shorts(dry_run: bool = False) -> None:
    """Основная функция распределения."""
    shorts   = collect_shorts()
    accounts = load_accounts()

    if not shorts:
        log.info("Нет видео для распределения. Выходим.")
        return

    if not accounts:
        log.error("Аккаунты не найдены. Выходим.")
        return

    active_accounts = [a for a in accounts if a["slots_left"] > 0]
    if not active_accounts:
        log.warning("Все аккаунты достигли дневного лимита — распределение невозможно.")
        return

    log.info("Аккаунтов с доступными слотами: %d / %d", len(active_accounts), len(accounts))

    # Round-robin распределение
    distribution: dict[str, list[dict]] = {a["name"]: [] for a in active_accounts}
    short_queue = list(shorts)
    acc_index   = 0

    while short_queue:
        if all(
            len(distribution[a["name"]]) >= a["slots_left"]
            for a in active_accounts
        ):
            break

        acc = active_accounts[acc_index % len(active_accounts)]
        acc_index += 1

        assigned = distribution[acc["name"]]
        if len(assigned) >= acc["slots_left"]:
            continue

        assigned.append(short_queue.pop(0))

    # Копирование файлов
    total_copied  = 0
    failed_copies = 0
    copied_shorts: list[dict] = []

    for acc in active_accounts:
        assigned = distribution[acc["name"]]
        if not assigned:
            continue

        for short in assigned:
            mp4_src   = short["mp4"]
            json_data = meta_to_video_json(short["meta"])
            dest_mp4  = acc["queue_dir"] / mp4_src.name
            dest_json = acc["queue_dir"] / (mp4_src.stem + ".json")

            if dry_run:
                log.info(
                    "[dry_run] %s → %s/upload_queue/  [%s, лимит: %d/день]",
                    mp4_src.name, acc["name"], acc["platform"], acc["platform_limit"],
                )
                total_copied += 1
                copied_shorts.append(short)
                continue

            try:
                shutil.copy2(mp4_src, dest_mp4)
                dest_json.write_text(
                    json.dumps(json_data, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                total_copied += 1
                copied_shorts.append(short)
                log.info(
                    "  ✓  %-40s  →  %s/upload_queue/  [%s]",
                    mp4_src.name, acc["name"], acc["platform"],
                )
            except Exception as exc:
                failed_copies += 1
                log.error("  ✗  Ошибка копирования %s → %s: %s", mp4_src.name, acc["name"], exc)

        # Обновляем daily_limit.json аккаунта
        if not dry_run:
            limit_data = acc["limit_data"] or {}
            uploaded_today_dict = limit_data.setdefault("uploaded_today", {})
            uploaded_today_dict[TODAY] = acc["uploaded"] + len(assigned)
            try:
                acc["limit_path"].write_text(
                    json.dumps(limit_data, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            except Exception as exc:
                log.warning("Не удалось обновить %s: %s", acc["limit_path"], exc)

    # Очистка исходных файлов после успешного копирования
    if not dry_run:
        for short in copied_shorts:
            try:
                short["mp4"].unlink(missing_ok=True)
                # Удаляем и .json если он был рядом с mp4
                short["mp4"].with_suffix(".json").unlink(missing_ok=True)
            except Exception as exc:
                log.warning("Не удалось удалить временный файл: %s", exc)

        if config.OUTPUT_DIR.exists():
            for source_dir in config.OUTPUT_DIR.iterdir():
                if source_dir.is_dir() and not any(source_dir.iterdir()):
                    try:
                        source_dir.rmdir()
                        log.info("Удалена пустая папка источника: %s", source_dir.name)
                    except Exception:
                        pass

    log.info("─" * 60)
    log.info("ИТОГО распределено : %d видео", total_copied)
    log.info("Ошибок копирования : %d", failed_copies)
    log.info("Видео не помещены  : %d (нет слотов)", len(short_queue))
    log.info("─" * 60)

    for acc in active_accounts:
        count = len(distribution[acc["name"]])
        if count:
            log.info(
                "  %-30s  +%d видео  (платформа: %s, лимит: %d/день)",
                acc["name"], count, acc["platform"], acc["platform_limit"],
            )


if __name__ == "__main__":
    distribute_shorts()
