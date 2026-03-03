"""
pipeline/main_processing.py – Этап обработки видео:
нарезка → AI-генерация метаданных → постобработка → клонирование.
"""

import logging
import shutil
from pathlib import Path
from typing import List, Optional

from pipeline import config, utils
from pipeline.ai import generate_video_metadata, load_trending_hashtags, check_ollama
from pipeline.cloner import run_cloning
from pipeline.postprocessor import stage_postprocess
from pipeline.slicer import stage_slice

logger = logging.getLogger(__name__)


def run_processing(dry_run: bool = False) -> List[Path]:
    """
    Запускает полный цикл обработки для каждого видео в PREPARING_DIR:
      1. Нарезка на клипы (slicer) с учётом best_segment из AI
      2. AI-генерация метаданных (ollama + yolo) — несколько вариантов
      3. Постобработка: наложение на фон в круге + текстовые оверлеи (postprocessor)
      4. Клонирование с вариациями, каждый клон получает случайный вариант метаданных (cloner)

    Возвращает список путей к готовым шортсам в OUTPUT_DIR.
    """
    logger.info("═══ Запуск обработки видео ═══")

    # Подготовка директорий
    config.TEMP_DIR.mkdir(parents=True, exist_ok=True)
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Ресурсы
    vcodec, vcodec_opts = utils.detect_encoder()
    bg_path             = utils.get_random_asset(config.BG_DIR, ('.mp4', '.mov', '.avi'))
    banner_path         = utils.get_random_asset(config.BANNER_DIR, ('.png', '.jpg', '.webp'))
    music_path          = utils.get_random_asset(config.MUSIC_DIR, ('.mp3', '.wav', '.aac', '.ogg'))
    trending_hashtags   = load_trending_hashtags()
    ai_available        = config.AI_ENABLED and check_ollama()

    if not bg_path:
        logger.warning("Фон не найден в %s — постобработка будет пропущена.", config.BG_DIR)

    # Собираем исходные видео
    source_files = sorted([
        f for f in config.PREPARING_DIR.iterdir()
        if f.suffix.lower() in config.VIDEO_EXT
    ])

    if not source_files:
        logger.warning("Нет видео в %s — обработка нечего делать.", config.PREPARING_DIR)
        return []

    logger.info("Найдено исходных видео: %d", len(source_files))
    all_ready_shorts: List[Path] = []

    for video_path in source_files:
        logger.info("──── Обработка: %s ────", video_path.name)
        source_name = video_path.stem

        # ── 2. AI-генерация метаданных (до нарезки, чтобы передать best_segment) ──
        # Порядок изменён: сначала AI, потом нарезка — best_segment нужен slicer'у
        metadata_variants: List[dict] = []
        if ai_available:
            try:
                metadata_variants = generate_video_metadata(video_path, trending_hashtags)
                logger.info(
                    "AI сгенерировал %d вариантов метаданных для '%s'",
                    len(metadata_variants), video_path.name,
                )
            except Exception as e:
                logger.warning(
                    "AI недоступен для %s: %s — использую заглушку.",
                    video_path.name, e,
                )

        # Если AI не дал вариантов — используем заглушку
        if not metadata_variants:
            metadata_variants = [_default_meta(source_name)]

        # ── 1. Нарезка (с передачей best_segment из метаданных) ────────────
        clip_dir = config.TEMP_DIR / source_name
        try:
            clips = stage_slice(
                source_name, video_path, clip_dir,
                metadata_variants=metadata_variants,
            )
        except Exception as e:
            logger.error("Ошибка нарезки %s: %s", video_path.name, e)
            continue

        if not clips:
            logger.warning("Нарезка не дала клипов для %s", video_path.name)
            continue
        logger.info("Нарезано клипов: %d", len(clips))

        # ── 3. Постобработка каждого клипа ──────────────────────────────────
        # Первый клип получает первый вариант метаданных (он же с best_segment).
        # Остальные клипы тоже используют первый вариант для постобработки —
        # финальное разнообразие метаданных обеспечивается на этапе клонирования.
        postprocessed: List[Path] = []
        for clip_idx, clip_path in enumerate(clips):
            out_dir = config.OUTPUT_DIR / source_name
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / clip_path.name

            # Для постобработки используем первый вариант метаданных
            clip_meta = metadata_variants[0]

            if dry_run:
                logger.info("[dry_run] Постобработка: %s → %s", clip_path.name, out_path)
                postprocessed.append(out_path)
                continue

            if bg_path:
                ok = stage_postprocess(
                    clip_path, str(bg_path), out_path,
                    vcodec, vcodec_opts,
                    meta=clip_meta,     # передаём метаданные для текстовых оверлеев
                )
            else:
                # Нет фона — просто копируем клип
                shutil.copy2(clip_path, out_path)
                ok = True

            if ok:
                postprocessed.append(out_path)
                # Намеренно НЕ сохраняем _description.txt здесь:
                # финальные метаданные будут сохранены в .json при клонировании
            else:
                logger.warning("Постобработка не удалась: %s", clip_path.name)

        if not postprocessed:
            logger.warning(
                "После постобработки не осталось клипов для %s", video_path.name
            )
            continue

        # ── 4. Клонирование ─────────────────────────────────────────────────
        if dry_run:
            logger.info("[dry_run] Клонирование: %d клипов пропущено.", len(postprocessed))
            all_ready_shorts.extend(postprocessed)
            continue

        clone_tasks = _build_clone_tasks(
            postprocessed, banner_path, music_path,
            vcodec, vcodec_opts,
            metadata_variants=metadata_variants,   # передаём все варианты
        )

        if clone_tasks:
            workers = min(
                config.GPU_SLOTS if vcodec == 'h264_nvenc' else config.MAX_WORKERS,
                len(clone_tasks),
            )
            log_lines, valid_clones = run_cloning(clone_tasks, total_workers=workers)
            for line in log_lines:
                logger.info(line)
            all_ready_shorts.extend(valid_clones)
            logger.info("Клонов создано: %d", len(valid_clones))

            # Удаляем исходные постобработанные клипы — в OUTPUT_DIR остаются только клоны
            _remove_source_clips(postprocessed)
        else:
            all_ready_shorts.extend(postprocessed)

        # ── Очистка temp-клипов текущего источника ──────────────────────────
        _cleanup_clip_dir(clip_dir)

    logger.info(
        "═══ Обработка завершена: %d шортсов готово ═══", len(all_ready_shorts)
    )
    return all_ready_shorts


# ----------------------------------------------------------------------
# Вспомогательные функции
# ----------------------------------------------------------------------

def _default_meta(source_name: str) -> dict:
    """Возвращает базовые метаданные при недоступном AI (все поля, включая виральность)."""
    return {
        "title":          f"Amazing Short #{source_name[:40]}",
        "description":    "Subscribe for more! 🔔 #shorts #viral #trending",
        "tags":           ["shorts", "viral", "trending"],
        "thumbnail_idea": "A key moment from the video.",
        # Поля виральности — пустые, не ломают обратную совместимость
        "hook_text":    "",
        "best_segment": None,
        "overlays":     [],
        "loop_prompt":  "",
    }


def _build_clone_tasks(
    sources: List[Path],
    banner_path,
    music_path,
    vcodec: str,
    vcodec_opts: dict,
    metadata_variants: Optional[List[dict]] = None,
) -> list:
    """
    Формирует список задач для cloner.run_cloning.
    Каждая задача включает metadata_variants — список вариантов метаданных,
    из которых cloner случайно выберет один при создании клона.
    """
    if metadata_variants is None:
        metadata_variants = []

    tasks = []
    idx   = 0
    for src_path in sources:
        for clone_num in range(1, config.CLONES_PER_VIDEO + 1):
            out_path = src_path.parent / f"{src_path.stem}_clone{clone_num:02d}.mp4"
            tasks.append((
                idx,
                str(src_path),
                str(banner_path) if banner_path else None,
                str(music_path)  if music_path  else None,
                str(out_path),
                vcodec,
                vcodec_opts,
                clone_num,
                metadata_variants,   # НОВЫЙ ЭЛЕМЕНТ — список вариантов метаданных
            ))
            idx += 1
    return tasks


def _remove_source_clips(postprocessed: List[Path]) -> None:
    """
    Удаляет исходные постобработанные клипы после завершения клонирования.
    В OUTPUT_DIR должны оставаться только клоны (с .json метаданными).
    """
    for clip_path in postprocessed:
        try:
            if clip_path.exists():
                clip_path.unlink()
                logger.debug("Удалён исходный постобработанный клип: %s", clip_path.name)
        except Exception as e:
            logger.warning("Не удалось удалить клип %s: %s", clip_path.name, e)


def _cleanup_clip_dir(clip_dir: Path) -> None:
    """Удаляет папку с временными клипами после обработки."""
    try:
        if clip_dir.exists():
            shutil.rmtree(clip_dir)
            logger.debug("Удалена temp-папка: %s", clip_dir)
    except Exception as e:
        logger.warning("Не удалось удалить %s: %s", clip_dir, e)
