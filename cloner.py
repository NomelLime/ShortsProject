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
from pipeline.utils import probe_video, check_video_integrity

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

    task = (idx, src_path, _banner_unused, _music_path_unused,
            out_path, vcodec, vcodec_opts, clone_num, metadata_variants)

    Баннер (_banner_unused) передаётся для совместимости с main_processing,
    но в клонере НЕ используется — он уже наложен в postprocessor.
    """
    (idx, src_path, _banner_unused, _music_path_unused,
     out_path, vcodec, vcodec_opts, clone_num,
     metadata_variants) = task

    # Музыка выбирается независимо для каждого клона
    music_path = _pick_random_music()

    # Случайный вариант метаданных для этого клона
    chosen_meta: Dict = {}
    if metadata_variants:
        chosen_meta = random.choice(metadata_variants)

    try:
        ci  = probe_video(src_path)
        dur = ci["duration"]
        fps = ci["fps"]
        sr  = ci["sample_rate"]
        cw, ch = ci["width"], ci["height"]

        speed      = round(random.uniform(*SPEED_RANGE), 3)
        zoom       = round(random.uniform(*ZOOM_RANGE), 3)
        brightness = round(random.uniform(*BRIGHTNESS_RANGE), 3)
        contrast   = round(random.uniform(*CONTRAST_RANGE), 3)
        saturation = round(random.uniform(*SATURATION_RANGE), 3)
        hue        = round(random.uniform(*HUE_RANGE), 1)
        vignette   = round(random.uniform(*VIGNETTE_RANGE), 2)
        noise_str  = random.randint(*NOISE_STRENGTH_RANGE)
        do_hflip   = random.choice([True, False])
        v_pts      = round(1.0 / speed, 6)

        blank_frames = random.randint(1, 2)
        blank_dur    = blank_frames / fps
        insert_t     = random.uniform(3.0, max(3.1, dur - 3.0))

        clone_music_vol = round(random.uniform(MUSIC_VOLUME, MUSIC_VOLUME + 0.05), 3)
        fade_dur        = round(random.uniform(MUSIC_FADE_DUR, MUSIC_FADE_DUR * 2.0), 2)

        src_in = ffmpeg.input(src_path)

        # --- Видео ---
        v = (
            src_in.video
            .filter("setpts", f"{v_pts}*PTS")
            .filter("scale", f"trunc({cw}*{zoom}/2)*2", "-2")
            .filter("crop", cw, ch)
            .filter("eq", brightness=brightness, contrast=contrast)
            .filter("hue", h=hue, s=saturation)
            .filter("noise", all_strength=noise_str, all_flags="a")
        )
        if do_hflip:
            v = v.filter("hflip")
        v = v.filter("vignette", angle=vignette)

        # БАННЕР/ЛОГО УБРАН: постобработка (postprocessor) уже накладывает баннер.
        # Дублирование здесь удалено.

        # --- Вставка пустого кадра ---
        v_split = ffmpeg.filter_multi_output(v, "split", 2)
        vp1 = (v_split.stream(0)
               .filter("trim", end=insert_t)
               .filter("setpts", "PTS-STARTPTS")
               .filter("setsar", "1/1"))
        vp2 = (v_split.stream(1)
               .filter("trim", start=insert_t)
               .filter("setpts", "PTS-STARTPTS")
               .filter("setsar", "1/1"))
        blank_v = (
            ffmpeg.input(
                f"color=c=black:size={cw}x{ch}:duration={blank_dur:.6f}:rate={fps:.4f}",
                f="lavfi",
            )
            .filter("format", "yuv420p")
            .filter("setsar", "1/1")
        )
        video_out = ffmpeg.concat(vp1, blank_v, vp2, v=1, a=0)

        # --- Аудио оригинала ---
        if ci["has_audio"]:
            orig_a = src_in.audio.filter("atempo", speed).filter("volume", 0.85)
        else:
            orig_a = ffmpeg.input(
                f"anullsrc=r={sr}:cl=stereo", f="lavfi", t=dur
            ).audio

        # --- Фоновая музыка ---
        if music_path and music_path.exists():
            music_a = (
                ffmpeg.input(str(music_path), stream_loop=-1, t=dur)
                .audio
                .filter("atempo", speed)
                .filter("afade", type="in",  start_time=0, duration=fade_dur)
                .filter("afade", type="out", start_time=max(0.0, dur - fade_dur), duration=fade_dur)
                .filter("volume", clone_music_vol)
            )
            audio_source = ffmpeg.filter(
                [orig_a, music_a], "amix",
                inputs=2, duration="first", weights="1 1",
            )
        else:
            audio_source = orig_a

        # --- Вставка пустого аудиофрагмента ---
        a_split = ffmpeg.filter_multi_output(audio_source, "asplit", 2)
        ap1 = (a_split.stream(0)
               .filter("atrim", end=insert_t)
               .filter("asetpts", "PTS-STARTPTS"))
        ap2 = (a_split.stream(1)
               .filter("atrim", start=insert_t)
               .filter("asetpts", "PTS-STARTPTS"))
        blank_a   = ffmpeg.input(f"anullsrc=r={sr}:cl=stereo", f="lavfi", t=blank_dur)
        audio_out = ffmpeg.concat(ap1, blank_a, ap2, v=0, a=1)

        opts = vcodec_opts or {}
        (ffmpeg.output(
            video_out, audio_out, out_path,
            vcodec=vcodec, acodec="aac",
            audio_bitrate=AUDIO_BITRATE,
            format="mp4", pix_fmt="yuv420p",
            movflags="+faststart", map_metadata=-1,
            **opts,
        )
         .overwrite_output()
         .run(capture_stdout=True, capture_stderr=True))

        if chosen_meta:
            _save_clone_meta(Path(out_path), chosen_meta)

        flip_tag  = "🔄" if do_hflip else "➡️"
        codec_tag = "[GPU]" if vcodec == "h264_nvenc" else "[CPU]"
        music_tag = f"🎵{music_path.stem[:10]}" if music_path else "🔇"
        meta_tag  = f"📝{chosen_meta.get('title', '')[:15]}" if chosen_meta else "📝—"
        status = (
            f"✅ [{idx:03d}] clone{clone_num:02d} | "
            f"spd={speed} zoom={zoom} hue={hue}° "
            f"vig={vignette} blank={blank_frames}fr@{insert_t:.1f}s "
            f"mvol={clone_music_vol} fade={fade_dur}s "
            f"{flip_tag} {codec_tag} {music_tag} {meta_tag}"
        )
        return (idx, status, Path(out_path))

    except ffmpeg.Error as e:
        err = e.stderr.decode("utf-8", errors="replace")[-200:] if e.stderr else str(e)
        return (idx, f"❌ [{idx:03d}] clone{clone_num:02d}: {err}", None)
    except Exception as e:
        return (idx, f"❌ [{idx:03d}] clone{clone_num:02d}: {e}", None)


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
