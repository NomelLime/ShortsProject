"""
pipeline/agents/thumbnail_tester.py — THUMBNAIL_TESTER: A/B тест миниатюр.

Алгоритм:
  1. Visionary генерирует N вариантов thumbnail (разные кадры + разный overlay-текст)
  2. thumbnail_tester.py сохраняет варианты как thumbnail_{stem}_A.jpg / _B.jpg
  3. В analytics.json записывается `thumbnail_variants` с путями к файлам
  4. Uploader (через register_upload) записывает uploaded_thumbnail_variant
  5. Strategist через AB_TEST_COMPARE_AFTER_H часов: читает CTR по вариантам,
     выбирает winner, помечает в analytics.json

Конфиг:
  THUMBNAIL_AB_ENABLED   = 0/1
  THUMBNAIL_AB_VARIANTS  = 2      # 2 или 3 варианта
  AB_TEST_COMPARE_AFTER_H = 24   # часов до сравнения
"""
from __future__ import annotations

import json
import logging
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_THUMB_DIR_NAME = "thumbnails"  # папка внутри BASE_DIR/data/


# ─────────────────────────────────────────────────────────────────────────────
# Публичный API (вызывается из Editor/Visionary после обработки видео)
# ─────────────────────────────────────────────────────────────────────────────

def generate_thumbnail_variants(
    video_path: Path,
    video_stem: str,
    meta_variants: List[Dict],
    num_variants: int = 2,
) -> List[Path]:
    """
    Генерирует N thumbnail-вариантов для видео.

    Каждый вариант = кадр из другого временного диапазона + overlay-текст из meta.
    Сохраняет файлы в data/thumbnails/{stem}_A.jpg, _B.jpg, ...

    Возвращает список путей к сгенерированным файлам.
    """
    from pipeline import config as cfg

    if not getattr(cfg, "THUMBNAIL_AB_ENABLED", False):
        return []

    num_variants = min(num_variants, len(meta_variants), 3)
    if num_variants < 2:
        logger.debug("[THUMBNAIL_TESTER] Нужно минимум 2 варианта метаданных")
        return []

    thumb_dir = cfg.BASE_DIR / "data" / _THUMB_DIR_NAME
    thumb_dir.mkdir(parents=True, exist_ok=True)

    # Получаем длительность видео
    duration = _get_video_duration(video_path)
    if duration is None or duration < 5:
        logger.warning("[THUMBNAIL_TESTER] Не удалось получить длительность: %s", video_path)
        return []

    labels   = [chr(ord("A") + i) for i in range(num_variants)]
    paths: List[Path] = []

    for i, label in enumerate(labels):
        # Разные временные точки для каждого варианта
        t = duration * (0.2 + i * 0.3)  # 20%, 50%, 80% от длительности
        t = min(t, duration - 1)

        out_path = thumb_dir / f"{video_stem}_{label}.jpg"
        hook_text = meta_variants[i].get("hook_text", "") if meta_variants else ""

        ok = _extract_frame_with_overlay(video_path, t, out_path, hook_text, label)
        if ok:
            paths.append(out_path)
            logger.debug(
                "[THUMBNAIL_TESTER] Вариант %s создан: %s (t=%.1fs)",
                label, out_path.name, t,
            )

    # Записываем в analytics.json
    if paths:
        _register_thumbnail_variants(video_stem, labels[:len(paths)], paths)
        logger.info(
            "[THUMBNAIL_TESTER] Создано %d thumbnail-вариантов для %s",
            len(paths), video_stem,
        )

    return paths


def select_thumbnail_winner(
    video_stem: str,
    platform: str,
    winner_variant: str,
) -> None:
    """
    Помечает победителя A/B теста миниатюр в analytics.json.
    Вызывается из Strategist после сравнения CTR.
    """
    from pipeline import config as cfg
    from pipeline.analytics import _load_analytics, _save_analytics

    data  = _load_analytics()
    entry = data.get(video_stem)
    if not entry:
        return

    tv = entry.get("thumbnail_variants", {})
    tv[platform] = tv.get(platform, {})
    tv[platform]["winner"]     = winner_variant
    tv[platform]["decided_at"] = datetime.utcnow().isoformat()
    entry["thumbnail_variants"] = tv
    data[video_stem] = entry
    _save_analytics(data)
    logger.info(
        "[THUMBNAIL_TESTER] Победитель %s для %s/%s",
        winner_variant, video_stem, platform,
    )


def compare_thumbnail_results(min_hours: Optional[int] = None) -> List[Dict]:
    """
    Сравнивает CTR thumbnail-вариантов из analytics.json.
    Возвращает список dict с полями: stem, platform, winner, reason.
    Вызывается из Strategist._analysis_cycle().
    """
    from pipeline import config as cfg
    from pipeline.analytics import _load_analytics

    if not getattr(cfg, "THUMBNAIL_AB_ENABLED", False):
        return []

    compare_after_h = min_hours or getattr(cfg, "AB_TEST_COMPARE_AFTER_H", 24)
    data = _load_analytics()
    results = []
    now  = datetime.utcnow()

    for stem, entry in data.items():
        tv = entry.get("thumbnail_variants", {})
        if not tv:
            continue

        for platform, platform_data in tv.items():
            # Пропускаем уже решённые
            if platform_data.get("winner"):
                continue

            created_at = platform_data.get("created_at")
            if not created_at:
                continue

            try:
                age_h = (now - datetime.fromisoformat(created_at)).total_seconds() / 3600
            except (ValueError, TypeError):
                continue

            if age_h < compare_after_h:
                continue

            # Ищем CTR по вариантам из uploads
            variant_ctr: Dict[str, float] = {}
            for acct_key, upload in entry.get("uploads", {}).items():
                if not acct_key.startswith(platform):
                    continue
                var = upload.get("thumbnail_variant")
                ctr = upload.get("ctr")
                if var and ctr is not None:
                    variant_ctr.setdefault(var, []).append(float(ctr))

            if len(variant_ctr) < 2:
                continue  # недостаточно данных

            # Выбираем победителя по среднему CTR
            avg_ctr = {v: sum(vals) / len(vals) for v, vals in variant_ctr.items()}
            winner  = max(avg_ctr, key=lambda v: avg_ctr[v])

            select_thumbnail_winner(stem, platform, winner)

            results.append({
                "stem":     stem,
                "platform": platform,
                "winner":   winner,
                "avg_ctr":  avg_ctr,
                "reason":   f"CTR: {', '.join(f'{v}={c:.3f}' for v, c in avg_ctr.items())}",
            })

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Внутренние утилиты
# ─────────────────────────────────────────────────────────────────────────────

def _get_video_duration(video_path: Path) -> Optional[float]:
    """Получает длительность видео через ffprobe."""
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(video_path),
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        return float(result.stdout.strip())
    except (subprocess.TimeoutExpired, ValueError, FileNotFoundError):
        return None


def _extract_frame_with_overlay(
    video_path: Path,
    timestamp: float,
    out_path: Path,
    overlay_text: str,
    label: str,
) -> bool:
    """
    Извлекает кадр из video_path в момент timestamp секунд.
    Накладывает overlay_text + метку варианта.
    Сохраняет в out_path (.jpg).
    """
    from pipeline import config as cfg
    font_path = str(getattr(cfg, "FONT_PATH", ""))

    # Базовая команда: извлечь кадр
    drawtext_filter = ""
    if overlay_text and font_path and Path(font_path).exists():
        safe_text = overlay_text.replace("'", "\\'").replace(":", "\\:")[:50]
        drawtext_filter = (
            f",drawtext=fontfile='{font_path}':text='{safe_text}':"
            "fontcolor=white:fontsize=48:bordercolor=black:borderw=3:"
            "x=(w-text_w)/2:y=h*0.15"
        )

    cmd = [
        "ffmpeg", "-y",
        "-ss", str(timestamp),
        "-i", str(video_path),
        "-frames:v", "1",
        "-vf", f"scale=720:-1{drawtext_filter}",
        "-q:v", "2",
        str(out_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=30)
        return result.returncode == 0 and out_path.exists()
    except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
        logger.warning("[THUMBNAIL_TESTER] ffmpeg ошибка: %s", exc)
        return False


def _register_thumbnail_variants(
    video_stem: str,
    labels: List[str],
    paths: List[Path],
) -> None:
    """Записывает пути к thumbnail-вариантам в analytics.json."""
    from pipeline.analytics import _load_analytics, _save_analytics

    data  = _load_analytics()
    entry = data.setdefault(video_stem, {"title": "", "tags": [], "uploads": {}})
    tv    = entry.setdefault("thumbnail_variants", {})

    # Глобальные данные (не привязаны к платформе)
    tv["files"]      = {label: str(path) for label, path in zip(labels, paths)}
    tv["labels"]     = labels
    tv["created_at"] = datetime.utcnow().isoformat()

    data[video_stem] = entry
    _save_analytics(data)
