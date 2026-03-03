"""
pipeline/finalize.py – Этап финализации.

Изменения:
  - Архивирование исходника из preparing_shorts/ происходит ТОЛЬКО тогда,
    когда видео загружено хотя бы на одну из платформ каждой группы:
    youtube + tiktok + instagram.
  - Для отслеживания используется data/upload_tracking.json:
      { "video_stem": {"youtube": true, "tiktok": false, "instagram": true} }
  - Связь между клоном в upload_queue и исходником в preparing_shorts/
    устанавливается через имя файла: strip суффикса _clone\d+ → имя исходника.
"""

import json
import logging
import re
import shutil
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Set

from pipeline import config, utils
from pipeline.notifications import send_telegram, send_telegram_alert

logger = logging.getLogger(__name__)

ERROR_THRESHOLD = 5


# ─────────────────────────── Трекинг платформ ────────────────────────────

def _load_tracking() -> Dict[str, Dict[str, bool]]:
    """Загружает таблицу трекинга загрузок из upload_tracking.json."""
    if not config.UPLOAD_TRACKING_FILE.exists():
        return {}
    try:
        return json.loads(config.UPLOAD_TRACKING_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_tracking(data: Dict[str, Dict[str, bool]]) -> None:
    """Сохраняет таблицу трекинга в upload_tracking.json."""
    try:
        config.UPLOAD_TRACKING_FILE.parent.mkdir(parents=True, exist_ok=True)
        config.UPLOAD_TRACKING_FILE.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.error("Не удалось сохранить upload_tracking.json: %s", exc)


def _extract_source_stem(clone_path_str: str) -> str:
    """
    Извлекает имя исходного видео из имени клона.
    Пример: 'my_video_clip0001_clone03.mp4' → 'my_video'
            'my_video_clip0001.mp4'          → 'my_video'
            'my_video.mp4'                   → 'my_video'
    """
    stem = Path(clone_path_str).stem
    # Убираем _clone\d+ суффикс
    stem = re.sub(r"_clone\d+$", "", stem, flags=re.IGNORECASE)
    # Убираем _clip\d+ суффикс
    stem = re.sub(r"_clip\d+$", "", stem, flags=re.IGNORECASE)
    return stem


def _update_tracking(
    upload_results: List[Dict],
    tracking: Dict[str, Dict[str, bool]],
) -> Dict[str, Dict[str, bool]]:
    """
    Обновляет tracking-таблицу на основе результатов загрузки.
    Только успешно загруженные ('status': 'uploaded') обновляют таблицу.
    """
    for item in upload_results:
        if item.get("status") != "uploaded":
            continue

        platform    = item.get("platform", "").lower()
        source_path = item.get("source_path", "")
        if not platform or not source_path:
            continue

        stem = _extract_source_stem(source_path)
        if stem not in tracking:
            tracking[stem] = {p: False for p in config.ALL_PLATFORMS}
        tracking[stem][platform] = True

    return tracking


def _find_complete_sources(
    tracking: Dict[str, Dict[str, bool]],
) -> Set[str]:
    """
    Возвращает множество stems исходников, загруженных на ВСЕ платформы.
    """
    complete: Set[str] = set()
    for stem, platforms in tracking.items():
        if all(platforms.get(p, False) for p in config.ALL_PLATFORMS):
            complete.add(stem)
    return complete


# ─────────────────────────── Архивирование ───────────────────────────────

def _archive_sources(
    complete_stems: Set[str],
    dry_run: bool = False,
) -> tuple[int, int]:
    """
    Перемещает в archive/ только те исходники из preparing_shorts/,
    которые загружены на все платформы (stem в complete_stems).

    Возвращает (перемещено, ошибок).
    """
    today_str = date.today().isoformat()
    dest_dir  = config.ARCHIVE_DIR / today_str
    dest_dir.mkdir(parents=True, exist_ok=True)

    moved  = 0
    errors = 0

    if not config.PREPARING_DIR.exists():
        return moved, errors

    for src_path in list(config.PREPARING_DIR.iterdir()):
        if src_path.suffix.lower() not in config.VIDEO_EXT:
            continue

        # Проверяем, входит ли этот файл в список завершённых
        if src_path.stem not in complete_stems:
            logger.debug(
                "Архивирование пропущено (не все платформы): %s — загружено на: %s",
                src_path.name,
                ", ".join(
                    p for p, done in _load_tracking().get(src_path.stem, {}).items() if done
                ) or "нигде",
            )
            continue

        try:
            dest_path = dest_dir / src_path.name
            if dest_path.exists():
                ts        = datetime.now().strftime("%H%M%S")
                dest_path = dest_dir / f"{src_path.stem}_{ts}{src_path.suffix}"

            if dry_run:
                logger.info("[dry_run] Будет перемещено: %s → %s", src_path.name, dest_path)
                moved += 1
            else:
                shutil.move(str(src_path), str(dest_path))
                logger.info("Архивировано: %s → %s", src_path.name, dest_path)
                moved += 1
        except Exception as exc:
            logger.error("Ошибка при архивировании %s: %s", src_path, exc)
            errors += 1

    return moved, errors


# ─────────────────────────── Лимиты ──────────────────────────────────────

def _update_daily_limits(
    upload_results: List[Dict],
    dry_run: bool = False,
) -> None:
    """Обновляет глобальный daily_limit.json — сводный счётчик по аккаунтам."""
    today_str = date.today().isoformat()
    limits: Dict = {}

    if config.DAILY_LIMIT_FILE.exists():
        loaded = utils.load_json(config.DAILY_LIMIT_FILE)
        if loaded is not None:
            limits = loaded

    for item in upload_results:
        account_id = str(item.get("account_id", ""))
        if not account_id:
            continue
        platform = item.get("platform", "unknown")

        entry = limits.setdefault(account_id, {
            "date":          today_str,
            "uploads_today": 0,
            "daily_max":     config.PLATFORM_DAILY_LIMITS.get(platform, config.DAILY_UPLOAD_LIMIT),
            "platform":      platform,
        })

        if entry.get("date") != today_str:
            entry["date"]          = today_str
            entry["uploads_today"] = 0

        if item.get("status") == "uploaded":
            entry["uploads_today"] = entry.get("uploads_today", 0) + 1

    if dry_run:
        logger.info("[dry_run] daily_limit.json был бы обновлён.")
        return

    try:
        config.DAILY_LIMIT_FILE.parent.mkdir(parents=True, exist_ok=True)
        config.DAILY_LIMIT_FILE.write_text(
            json.dumps(limits, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info("daily_limit.json обновлён для %d аккаунтов", len(limits))
    except Exception as exc:
        logger.error("Не удалось сохранить daily_limit.json: %s", exc)


# ─────────────────────────── Статистика ──────────────────────────────────

def _collect_statistics(results: List[Dict]) -> Dict:
    total    = len(results)
    uploaded = sum(1 for r in results if r.get("status") == "uploaded")
    skipped  = sum(1 for r in results if r.get("status") == "skipped")
    errors   = sum(1 for r in results if r.get("status") == "error")

    error_details = [
        f"  • [{r.get('account_id', '?')}] "
        f"{Path(r.get('source_path', '?')).name} → {r.get('error_msg', 'неизвестно')}"
        for r in results if r.get("status") == "error"
    ]

    platforms: Dict[str, int] = {}
    for r in results:
        if r.get("status") == "uploaded":
            p = r.get("platform", "unknown")
            platforms[p] = platforms.get(p, 0) + 1

    return {
        "total":         total,
        "uploaded":      uploaded,
        "skipped":       skipped,
        "errors":        errors,
        "error_details": error_details,
        "platforms":     platforms,
        "timestamp":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _build_report_text(
    stats: Dict,
    archived: int,
    archive_errors: int,
    tracking: Dict[str, Dict[str, bool]],
    complete_stems: set,
) -> str:
    """Формирует текст Telegram-отчёта."""
    # Считаем сколько исходников ожидают ещё каких-то платформ
    pending = {
        stem: {p for p, done in pmap.items() if not done}
        for stem, pmap in tracking.items()
        if stem not in complete_stems and any(pmap.values())
    }

    lines = [
        "📊 *Отчёт финализации*",
        f"🕐 Время: `{stats['timestamp']}`",
        "",
        "📁 *Результаты загрузки:*",
        f"  🎬 Всего в очереди:   `{stats['total']}`",
        f"  ✅ Успешно загружено: `{stats['uploaded']}`",
        f"  ⏭ Пропущено:         `{stats['skipped']}`",
        f"  ❌ Ошибок:            `{stats['errors']}`",
        "",
        "📦 *Архивирование:*",
        f"  📂 Перемещено файлов: `{archived}` (все 3 платформы ✓)",
        f"  ⏳ Ожидают платформ:  `{len(pending)}`",
        f"  ⚠️ Ошибок архива:    `{archive_errors}`",
        "",
    ]

    if stats["platforms"]:
        lines.append("📱 *По платформам (загружено):*")
        for platform, count in stats["platforms"].items():
            limit = config.PLATFORM_DAILY_LIMITS.get(platform, config.DAILY_UPLOAD_LIMIT)
            lines.append(f"  • {platform}: `{count}` (лимит: {limit}/день)")
        lines.append("")

    if pending:
        lines.append("🔄 *Ожидают оставшихся платформ:*")
        for stem, missing in list(pending.items())[:5]:
            lines.append(f"  • `{stem[:30]}` → ждём: {', '.join(missing)}")
        if len(pending) > 5:
            lines.append(f"  ...и ещё {len(pending)-5} видео")
        lines.append("")

    if stats["error_details"]:
        lines.append("🔴 *Детали ошибок:*")
        shown = stats["error_details"][:10]
        lines.extend(shown)
        if len(stats["error_details"]) > 10:
            lines.append(f"  ...и ещё {len(stats['error_details']) - 10} ошибок (см. лог)")
        lines.append("")

    status_emoji = (
        "✅" if stats["errors"] == 0
        else ("🔴" if stats["errors"] > ERROR_THRESHOLD else "🟡")
    )
    lines.append(
        f"{status_emoji} *Итог:* загружено {stats['uploaded']} из {stats['total']} видео"
    )
    return "\n".join(lines)


def _cleanup(dry_run: bool = False) -> None:
    """Очищает временные файлы: temp/clips/ и urls.txt."""
    if config.TEMP_DIR.exists():
        files = list(config.TEMP_DIR.glob("*"))
        if dry_run:
            logger.info("[dry_run] Было бы удалено %d файлов из %s", len(files), config.TEMP_DIR)
        else:
            for f in files:
                try:
                    if f.is_file():
                        f.unlink()
                    elif f.is_dir():
                        shutil.rmtree(f)
                except Exception as exc:
                    logger.warning("Не удалось удалить %s: %s", f, exc)
            logger.info("Очищена директория temp/clips/ (%d объектов)", len(files))

    if config.URLS_FILE.exists():
        if dry_run:
            logger.info("[dry_run] urls.txt был бы очищен")
        else:
            try:
                config.URLS_FILE.write_text("", encoding="utf-8")
                logger.info("urls.txt очищен")
            except Exception as exc:
                logger.error("Не удалось очистить urls.txt: %s", exc)


# ─────────────────────────── Главная функция ─────────────────────────────

def finalize_and_report(
    upload_results: List[Dict],
    dry_run:        bool = False,
    skip_cleanup:   bool = False,
) -> Dict:
    """
    Этап финализации:
      1. Обновляем tracking-таблицу: какие исходники загружены на какие платформы
      2. Архивируем ТОЛЬКО те исходники, которые загружены на все 3 платформы
      3. Обновляем global daily_limit.json
      4. Собираем статистику
      5. Отправляем отчёт в Telegram
      6. Очищаем временные файлы (опционально)
    """
    logger.info("=" * 60)
    logger.info("ЭТАП: Финализация и отчётность")
    logger.info("Получено %d результатов из загрузки", len(upload_results))

    # 1. Обновляем tracking
    logger.info("Шаг 1: Обновление таблицы трекинга платформ")
    tracking = _load_tracking()
    tracking = _update_tracking(upload_results, tracking)
    _save_tracking(tracking)

    complete_stems = _find_complete_sources(tracking)
    logger.info(
        "Загружены на все платформы (%s): %d исходников",
        " + ".join(sorted(config.ALL_PLATFORMS)),
        len(complete_stems),
    )
    if complete_stems:
        for stem in list(complete_stems)[:5]:
            logger.info("  → %s", stem)
        if len(complete_stems) > 5:
            logger.info("  ...и ещё %d", len(complete_stems) - 5)

    # 2. Архивирование (только полностью загруженных)
    logger.info("Шаг 2: Архивирование полностью загруженных исходников")
    archived, archive_errors = _archive_sources(complete_stems, dry_run=dry_run)
    logger.info("Архивировано: %d, ошибок: %d", archived, archive_errors)

    # Удаляем заархивированные stems из tracking (они больше не нужны)
    if not dry_run and complete_stems:
        for stem in complete_stems:
            tracking.pop(stem, None)
        _save_tracking(tracking)

    # 3. Обновление daily_limit.json
    logger.info("Шаг 3: Обновление global daily_limit.json")
    _update_daily_limits(upload_results, dry_run=dry_run)

    # 4. Сбор статистики
    logger.info("Шаг 4: Сбор статистики")
    stats = _collect_statistics(upload_results)
    logger.info(
        "Статистика → всего: %d | загружено: %d | пропущено: %d | ошибок: %d",
        stats["total"], stats["uploaded"], stats["skipped"], stats["errors"],
    )

    # 5. Отправка отчёта
    logger.info("Шаг 5: Отправка отчёта в Telegram")
    report_text = _build_report_text(stats, archived, archive_errors, tracking, complete_stems)
    try:
        send_telegram(report_text, parse_mode="Markdown")
        logger.info("Telegram-отчёт отправлен")
    except Exception as exc:
        logger.error("Не удалось отправить Telegram-отчёт: %s", exc)

    if stats["errors"] > ERROR_THRESHOLD:
        warning_text = (
            f"🚨 *ВНИМАНИЕ!* Количество ошибок загрузки превысило порог!\n"
            f"Ошибок: `{stats['errors']}` (порог: `{ERROR_THRESHOLD}`)\n"
            f"Требуется ручная проверка лога."
        )
        logger.warning("Ошибок > %d! Отправляю предупреждение", ERROR_THRESHOLD)
        try:
            send_telegram_alert(warning_text, parse_mode="Markdown")
        except Exception as exc:
            logger.error("Не удалось отправить предупреждение: %s", exc)

    # 6. Очистка
    if skip_cleanup:
        logger.info("Шаг 6: Очистка пропущена (skip_cleanup=True)")
    else:
        logger.info("Шаг 6: Очистка temp/clips/ и urls.txt")
        _cleanup(dry_run=dry_run)

    logger.info("ЭТАП Финализация завершён успешно")
    logger.info("=" * 60)

    return stats
