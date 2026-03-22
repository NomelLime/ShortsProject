"""
pipeline/serial_detector.py — Детектор серийного контента.

Алгоритм:
  1. Читает analytics.json (нужно ≥30 видео с данными)
  2. Для каждого видео вычисляет engagement_rate = (likes + comments) / views
  3. "Высокий retention" = views > MIN_VIEWS И engagement_rate в топ-25% исторических данных
  4. Такие видео помечаются как serial_candidates в AgentMemory

Интеграция:
  - Strategist вызывает detect_serial_candidates() в конце _analysis_cycle()
  - Narrator читает serial_candidates и добавляет "Часть 2:" к hook_text
    для следующего видео из той же ниши (по тегам)

Конфиг (через config.py / .env):
  SERIAL_ENABLED          = 0/1
  SERIAL_MIN_VIEWS        = 500    — минимум просмотров для анализа
  SERIAL_MIN_HISTORY      = 30     — минимум видео для расчёта процентиля
  SERIAL_TOP_PCT          = 25     — топ N% по engagement_rate
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Минимальный engagement_rate для включения в кандидаты (абсолютный guard)
_MIN_ENGAGEMENT_RATE = 0.01  # 1%


def detect_serial_candidates(
    force: bool = False,
) -> List[Dict]:
    """
    Анализирует analytics.json и возвращает список serial_candidates.

    Каждый кандидат:
    {
        "stem":             str,
        "title":            str,
        "tags":             List[str],
        "total_views":      int,
        "engagement_rate":  float,
        "platforms":        List[str],
    }

    Пишет результат в AgentMemory["serial_candidates"].

    Returns:
        список кандидатов (может быть пустым)
    """
    from pipeline import config as cfg
    from pipeline.analytics import _load_analytics
    from pipeline.agent_memory import get_memory

    if not force and not getattr(cfg, "SERIAL_ENABLED", False):
        return []

    min_views   = int(getattr(cfg, "SERIAL_MIN_VIEWS", 500))
    min_history = int(getattr(cfg, "SERIAL_MIN_HISTORY", 30))
    top_pct     = int(getattr(cfg, "SERIAL_TOP_PCT", 25))

    data = _load_analytics()

    # Собираем метрики для всех видео с реальными данными
    records = _collect_records(data, min_views)

    if len(records) < min_history:
        logger.info(
            "[SerialDetector] Недостаточно данных: %d видео (нужно %d)",
            len(records), min_history,
        )
        return []

    # Вычисляем порог engagement_rate (процентиль)
    engagement_rates = sorted(r["engagement_rate"] for r in records)
    threshold_idx    = max(0, int(len(engagement_rates) * (1 - top_pct / 100)) - 1)
    threshold        = engagement_rates[threshold_idx]
    threshold        = max(threshold, _MIN_ENGAGEMENT_RATE)

    # Фильтруем кандидатов
    candidates = [
        r for r in records
        if r["engagement_rate"] >= threshold
    ]

    # Сортируем по engagement_rate DESC
    candidates.sort(key=lambda r: r["engagement_rate"], reverse=True)

    # Сохраняем в AgentMemory
    memory = get_memory()
    memory.set("serial_candidates", candidates)
    from datetime import datetime, timezone

    memory.set(
        "serial_candidates_updated_at",
        datetime.now(timezone.utc).isoformat(),
    )

    logger.info(
        "[SerialDetector] Найдено %d кандидатов (порог engagement=%.3f, всего видео=%d)",
        len(candidates), threshold, len(records),
    )
    return candidates


def _collect_records(data: Dict, min_views: int) -> List[Dict]:
    """
    Из analytics.json извлекает агрегированные метрики по видео.
    Учитывает все платформы.
    """
    records = []

    for stem, entry in data.items():
        if not isinstance(entry, dict):
            continue

        uploads = entry.get("uploads", {})
        if not isinstance(uploads, dict):
            continue

        total_views    = 0
        total_likes    = 0
        total_comments = 0
        platforms      = []

        for platform, upload in uploads.items():
            if not isinstance(upload, dict):
                continue
            views    = upload.get("views")    or 0
            likes    = upload.get("likes")    or 0
            comments = upload.get("comments") or 0

            if views > 0:
                total_views    += views
                total_likes    += likes
                total_comments += comments
                platforms.append(platform.split(":")[0] if ":" in platform else platform)

        if total_views < min_views:
            continue

        engagement_rate = (total_likes + total_comments) / total_views

        records.append({
            "stem":            stem,
            "title":           entry.get("title", stem),
            "tags":            entry.get("tags", []),
            "total_views":     total_views,
            "total_likes":     total_likes,
            "total_comments":  total_comments,
            "engagement_rate": engagement_rate,
            "platforms":       list(set(platforms)),
        })

    return records


def get_serial_candidates() -> List[Dict]:
    """
    Читает serial_candidates из AgentMemory (без пересчёта).
    Используется Narrator для определения hook_text.
    """
    from pipeline.agent_memory import get_memory
    return get_memory().get("serial_candidates") or []


def find_serial_parent(tags: List[str], stem_exclude: str = "") -> Optional[Dict]:
    """
    По списку тегов нового видео ищет подходящего serial_candidate
    (максимальное пересечение тегов).

    Returns:
        dict serial_candidate или None
    """
    candidates = get_serial_candidates()
    if not candidates:
        return None

    tags_set = {t.lower().strip() for t in tags if t.strip()}
    if not tags_set:
        return None

    best: Optional[Dict] = None
    best_score           = 0

    for cand in candidates:
        if cand.get("stem") == stem_exclude:
            continue
        cand_tags = {t.lower().strip() for t in cand.get("tags", [])}
        overlap   = len(tags_set & cand_tags)
        if overlap > best_score:
            best_score = overlap
            best       = cand

    # Нужно хотя бы 1 совпадающий тег
    return best if best_score >= 1 else None


def make_serial_hook(parent: Dict, base_hook: str) -> str:
    """
    Формирует hook_text для продолжения серии.

    Пример: "Часть 2: <base_hook>"
    Если base_hook содержит нишевое слово из тегов родителя — оставляем.
    """
    parent_title = parent.get("title", "").strip()

    if base_hook:
        return f"Часть 2: {base_hook}"

    if parent_title:
        return f"Часть 2: продолжение темы «{parent_title[:40]}»"

    return "Часть 2: продолжение"
