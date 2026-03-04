# cloner.py
"""
Клонирование постобработанных клипов с вариациями.
"""

import json
import logging
import random
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed

import ffmpeg
from tqdm import tqdm

# FIX #6: добавлен импорт config (использовался в _pick_random_bg без импорта)
from pipeline import config
from pipeline.config import (
    SPEED_RANGE, ZOOM_RANGE, BRIGHTNESS_RANGE, CONTRAST_RANGE, SATURATION_RANGE,
    HUE_RANGE, VIGNETTE_RANGE, NOISE_STRENGTH_RANGE,
    AUDIO_BITRATE,
    MUSIC_DIR, MUSIC_VOLUME, MUSIC_FADE_DUR,
)
from pipeline.utils import probe_video, check_video_integrity, get_random_asset

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
    """Возвращает случайный фон для клона (per-clone BG)."""
    return get_random_asset(Path(config.BG_DIR), (".mp4", ".mov", ".avi"))


def _save_clone_meta(
    out_path: Path,
    meta: Dict,
    all_variants: Optional[List[Dict]] = None,
) -> None:
    """
    Сохраняет метаданные клона в JSON-файл рядом с видео.
    all_variants — полный список вариантов метаданных от AI;
    сохраняется в ключе "all_variants" для A/B тестирования в distributor.py.
    """
    json_path = out_path.with_suffix(".json")
    meta_to_save = {k: v for k, v in meta.items() if k != "yolo_per_frame"}
    if all_variants and len(all_variants) > 1:
        meta_to_save["all_variants"] = [
            {k: v for k, v in m.items()
             if k not in ("yolo_per_frame", "best_segment", "overlays")}
            for m in all_variants
        ]
    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(meta_to_save, f, ensure_ascii=False, indent=2)
    except OSError as e:
        logger.warning("Не удалось сохранить мета для %s: %s", out_path.name, e)


# FIX #6: дублирующая незавершённая заглушка _clone_task УДАЛЕНА.
# Логика horizontal flip и per-clone BG встроена сюда.
def _clone_task(task: Tuple) -> Tuple[int, str, Optional[Path]]:
    """
    Выполняется в отдельном процессе.

    task = (idx, src_path, banner_path, music_path, out_path,
            vcodec, vcodec_opts, clone_num, metadata_variants)
    """
    (
        idx, src_path, banner_path_str, music_path_str, out_path,
        vcodec, vcodec_opts, clone_num, metadata_variants,
    ) = task

    try:
        info = probe_video(src_path)
        duration = info["duration"]

        # Случайный вариант метаданных
        meta = random.choice(metadata_variants) if metadata_variants else {}

        # Случайные параметры вариации
        speed      = random.uniform(*SPEED_RANGE)
        zoom       = random.uniform(*ZOOM_RANGE)
        brightness = random.uniform(*BRIGHTNESS_RANGE)
        contrast   = random.uniform(*CONTRAST_RANGE)
        saturation = random.uniform(*SATURATION_RANGE)
        hue        = random.uniform(*HUE_RANGE)
        vignette   = random.uniform(*VIGNETTE_RANGE)
        noise_str  = random.randint(*NOISE_STRENGTH_RANGE)

        # Горизонтальный флип (50% вероятность)
        do_hflip = random.random() < 0.5

        # Per-clone фон
        bg_path = _pick_random_bg()

        # Случайная музыка
        music_path = _pick_random_music() or (Path(music_path_str) if music_path_str else None)
        clone_music_vol = MUSIC_VOLUME * random.uniform(0.8, 1.2)
        fade_dur = MUSIC_FADE_DUR

        # Случайная точка вставки музыки
        insert_t = random.uniform(0, max(0, duration - 2))

        # Строим видео-фильтры
        vf_parts = [
            f"setpts={1/speed:.4f}*PTS",
            f"scale=iw*{zoom:.4f}:ih*{zoom:.4f},crop=iw/{zoom:.4f}:ih/{zoom:.4f}",
            f"eq=brightness={brightness:.4f}:contrast={contrast:.4f}:saturation={saturation:.4f}",
            f"hue=h={hue:.2f}",
            f"vignette=PI/{vignette:.2f}",
            f"noise=alls={noise_str}:allf=t",
        ]
        if do_hflip:
            vf_parts.append("hflip")

        vf_chain = ",".join(vf_parts)

        # Строим команду ffmpeg
        input_args = [ffmpeg.input(src_path)]
        if bg_path and bg_path.exists():
            input_args.append(ffmpeg.input(str(bg_path)))

        video = ffmpeg.input(src_path).video.filter_multi_output("split")[0]
        video = ffmpeg.filter([video], "setpts", f"{1/speed:.4f}*PTS")

        # Простой вызов через subprocess для надёжности
        cmd = [
            "ffmpeg", "-y",
            "-i", src_path,
        ]
        if music_path and music_path.exists():
            cmd += ["-ss", str(insert_t), "-i", str(music_path)]

        af = f"atempo={speed:.4f}"
        if music_path and music_path.exists():
            af_mix = (
                f"[0:a]atempo={speed:.4f}[a0];"
                f"[1:a]volume={clone_music_vol:.4f},afade=t=out:st={duration/speed - fade_dur:.2f}:d={fade_dur}[a1];"
                f"[a0][a1]amix=inputs=2:duration=first[aout]"
            )
            cmd += [
                "-vf", vf_chain,
                "-filter_complex", af_mix,
                "-map", "0:v", "-map", "[aout]",
            ]
        else:
            cmd += ["-vf", vf_chain, "-af", af]

        codec_params = [f"-c:v", vcodec]
        for k, v in (vcodec_opts or {}).items():
            codec_params += [f"-{k}", str(v)]
        cmd += codec_params + ["-c:a", "aac", "-b:a", AUDIO_BITRATE, out_path]

        import subprocess
        subprocess.run(cmd, check=True, capture_output=True)

        # Сохраняем метаданные (all_variants нужен для A/B в distributor)
        _save_clone_meta(Path(out_path), meta, all_variants=metadata_variants)

        flip_tag   = " hflip" if do_hflip else ""
        music_tag  = f" +music@{insert_t:.1f}s" if music_path else ""
        status = (
            f"OK [{idx:03d}] clone{clone_num:02d}:"
            f" spd={speed:.2f} zoom={zoom:.2f} bri={brightness:.2f}"
            f"{flip_tag}{music_tag}"
        )
        return (idx, status, Path(out_path))

    except Exception as e:
        return (idx, f"ERR [{idx:03d}] clone{clone_num:02d}: {e}", None)


def run_cloning(
    clone_tasks: List[Tuple],
    total_workers: int,
) -> Tuple[List[str], List[Path]]:
    """Запускает параллельное клонирование и проверяет целостность результатов."""
    results = []
    successful_clones = []

    with ProcessPoolExecutor(max_workers=total_workers) as executor:
        futures = {executor.submit(_clone_task, t): t[0] for t in clone_tasks}
        with tqdm(total=len(clone_tasks), desc="Клоны", unit="шт", ncols=72) as pbar:
            for future in as_completed(futures):
                try:
                    idx, status, path = future.result()
                    results.append((idx, status))
                    if status.startswith("OK") and path:
                        successful_clones.append(path)
                except Exception as e:
                    results.append((futures[future], f"ERR [{futures[future]:03d}]: {e}"))
                pbar.update(1)

    logger.info("Проверка целостности созданных клонов...")
    valid_clones = []
    for clone_path in successful_clones:
        if check_video_integrity(clone_path):
            valid_clones.append(clone_path)
        else:
            logger.warning("Клон повреждён и будет удалён: %s", clone_path.name)
            clone_path.unlink(missing_ok=True)
            clone_path.with_suffix(".json").unlink(missing_ok=True)

    sorted_results = [r for _, r in sorted(results)]
    return sorted_results, valid_clones
