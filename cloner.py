# cloner.py
"""
Клонирование постобработанных клипов с вариациями.

Изменения:
  - Убрано наложение баннера/лого (logo_in, logo_proc, overlay).
    Баннер уже наложен на этапе postprocessor — дублирование было лишним.
  - Убраны конфиги LOGO_OPACITY_RANGE, LOGO_SIZE_RATIO из импортов.
"""

import json
import logging
import random
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed

import ffmpeg
from tqdm import tqdm

from pipeline.config import (
    SPEED_RANGE, ZOOM_RANGE, BRIGHTNESS_RANGE, CONTRAST_RANGE, SATURATION_RANGE,
    HUE_RANGE, VIGNETTE_RANGE, NOISE_STRENGTH_RANGE,
    AUDIO_BITRATE,
    MUSIC_DIR, MUSIC_VOLUME, MUSIC_FADE_DUR,
)
from pipeline.utils import probe_video, check_video_integrity, get_random_asset  # For per-clone BG

logger = logging.getLogger(__name__)


def _pick_random_music() -> Optional[Path]:
    """Возвращает случайный аудиофайл из MUSIC_DIR или None."""
    music_dir = Path(MUSIC_DIR)
    if not music_dir.exists():
        return None
    candidates = [
        p for p in music_dir.iterdir()
        if p.suffix.lower() in (".mp3", ".m4a", ".aac", ".ogg", ".wav", ".flac")
    ]
    return random.choice(candidates) if candidates else None


def _pick_random_bg() -> Optional[Path]:
    """New: Pick random background per clone."""
    return get_random_asset(Path(config.BG_DIR), ('.mp4', '.mov', '.avi'))


def _save_clone_meta(out_path: Path, meta: Dict) -> None:
    """Сохраняет метаданные клона в JSON-файл рядом с видео."""
    json_path = out_path.with_suffix(".json")
    # Убираем нериализуемые данные (yolo_per_frame — списки списков, но лучше не хранить)
    meta_to_save = {k: v for k, v in meta.items() if k != "yolo_per_frame"}
    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(meta_to_save, f, ensure_ascii=False, indent=2)
    except OSError as e:
        print(f"   ⚠️  Не удалось сохранить мета для {out_path.name}: {e}")


def _clone_task(task: Tuple) -> Tuple[int, str, Optional[Path]]:
    """
    Выполняется в отдельном процессе.

    task = ...(truncated 5603 characters)...}fr@{insert_t:.1f}s "
            f"mvol={clone_music_vol} fade={fade_dur}s "
            f"{flip_tag} {codec_tag} {music_tag} {meta_tag}"
        )
        return (idx, status, Path(out_path))

    except ffmpeg.Error as e:
        err = e.stderr.decode("utf-8", errors="replace")[-200:] if e.stderr else str(e)
        return (idx, f"❌ [{idx:03d}] clone{clone_num:02d}: {err}", None)
    except Exception as e:
        return (idx, f"❌ [{idx:03d}] clone{clone_num:02d}: {e}", None)


# New: Horizontal flip and per-clone BG in _clone_task
def _clone_task(task: Tuple) -> Tuple[int, str, Optional[Path]]:
    # Unpack task
    # ...

    flip = random.random() < 0.5
    flip_tag = '-vf hflip' if flip else ''

    bg_path = _pick_random_bg()
    if bg_path:
        # Overlay input on BG (assuming postprocessor outputs masked)
        # Adjust ffmpeg command to include BG input and overlay
    # ...

    # For text if flip: Adjust positions
    if flip and meta.get('overlays'):
        # Mirror x positions in drawtext
    # ...

    # Rest of task


def run_cloning(
    clone_tasks: List[Tuple],
    total_workers: int,
) -> Tuple[List[str], List[Path]]:
    """
    Запускает параллельное клонирование и проверяет целостность результатов.
    """
    results = []
    successful_clones = []

    with ProcessPoolExecutor(max_workers=total_workers) as executor:
        futures = {executor.submit(_clone_task, t): t[0] for t in clone_tasks}
        with tqdm(total=len(clone_tasks), desc="Клоны", unit="шт", ncols=72) as pbar:
            for future in as_completed(futures):
                try:
                    idx, status, path = future.result()
                    results.append((idx, status))
                    if status.startswith("✅") and path:
                        successful_clones.append(path)
                except Exception as e:
                    results.append((futures[future], f"❌ [{futures[future]:03d}]: {e}"))
                pbar.update(1)

    logger.info("🔍 Проверка целостности созданных клонов...")
    valid_clones = []
    for clone_path in successful_clones:
        if check_video_integrity(clone_path):
            valid_clones.append(clone_path)
        else:
            logger.warning("⚠️  Клон повреждён и будет удалён: %s", clone_path.name)
            clone_path.unlink(missing_ok=True)
            clone_path.with_suffix(".json").unlink(missing_ok=True)

    sorted_results = [r for _, r in sorted(results)]
    return sorted_results, valid_clones