# postprocessor.py
"""
Постобработка клипов: масштабирование до 9:16, наложение на фон,
баннер, текстовые оверлеи (hook, loop_prompt, custom overlays).

Реализация через ffmpeg subprocess — надёжнее python-ffmpeg
для сложных filter_complex графов.
"""

import logging
import random
import subprocess
import textwrap
from pathlib import Path
from typing import Dict, List, Optional

from pipeline import config
from pipeline.config import (
    OUTPUT_W, OUTPUT_H, OUTPUT_FPS,
    CIRCLE_RATIO_LANDSCAPE, CIRCLE_RATIO_PORTRAIT,
    CIRCLE_VARIATION, AUDIO_BITRATE,
    BANNER_DIR, BANNER_HEIGHT_PCT, CIRCLE_OFFSET_PCT,
    FONT_PATH,
    HOOK_TEXT_DURATION, HOOK_TEXT_POSITION,
    LOOP_PROMPT_DURATION,
    OVERLAY_DEFAULT_DURATION, OVERLAY_POSITION,
    TTS_VOLUME, TTS_VOICE_OVER_MIX,
    BLURRED_BG_ENABLED, BLURRED_BG_SIGMA, BLURRED_BG_DARKEN,
    VIDEO_FILTER_ENABLED, VIDEO_FILTER_DEFAULT, VIDEO_FILTER_RANDOM,
    HOOK_ZOOM_ENABLED, HOOK_ZOOM_DURATION, HOOK_ZOOM_START, HOOK_ZOOM_END,
)
from pipeline.utils import probe_video

logger = logging.getLogger(__name__)

SHAPES = ["circle", "rounded_rect", "portrait_center"]


# ─────────────────────────────────────────────────────────────────────────────
# Вспомогательные функции
# ─────────────────────────────────────────────────────────────────────────────

def _pick_random_banner() -> Optional[Path]:
    banner_dir = Path(BANNER_DIR)
    if not banner_dir.exists():
        return None
    candidates = [p for p in banner_dir.iterdir()
                  if p.suffix.lower() in (".png", ".jpg", ".jpeg", ".webp")]
    return random.choice(candidates) if candidates else None


def _check_font() -> bool:
    font = Path(FONT_PATH)
    if not font.exists():
        logger.warning(
            "Шрифт не найден: %s — оверлеи пропущены. Поместите Roboto-Bold.ttf в assets/fonts/",
            FONT_PATH,
        )
        return False
    return True


def _escape_drawtext(text: str) -> str:
    """Экранирует спецсимволы для ffmpeg drawtext."""
    return (text
            .replace("\\", "\\\\")
            .replace("'",  "\\'")
            .replace(":",  "\\:")
            .replace("[",  "\\[")
            .replace("]",  "\\]"))


def _font_size_for_text(text: str, base: int = 56) -> int:
    n = len(text)
    if n <= 20: return base
    if n <= 40: return max(36, base - 10)
    return max(28, base - 20)


# ─────────────────────────────────────────────────────────────────────────────
# Построение filter_complex
# ─────────────────────────────────────────────────────────────────────────────

def _build_filter_complex(
    duration: float,
    has_audio: bool,
    has_bg: bool,
    has_banner: bool,
    banner_h_px: int,
    shape: str,
    font_str: str,
    meta: Dict,
    circle_ratio: float,
    bg_idx: int,
    banner_idx: int,
) -> str:
    filters: List[str] = []

    video_area_h = OUTPUT_H - banner_h_px if has_banner else OUTPUT_H
    circle_d = int(min(OUTPUT_W, video_area_h) * circle_ratio)
    circle_d -= circle_d % 2

    # ── Маска видео ──────────────────────────────────────────────────────────
    if shape == "circle":
        r = circle_d // 2
        filters.append(
            f"[0:v]scale={circle_d}:{circle_d}:force_original_aspect_ratio=increase,"
            f"crop={circle_d}:{circle_d},"
            f"format=yuva420p,"
            f"geq=lum='p(X,Y)':a='if(lte(hypot(X-{r},Y-{r}),{r}),255,0)'[vmask]"
        )
        overlay_x = f"({OUTPUT_W}-{circle_d})/2"
        overlay_y = f"({video_area_h}-{circle_d})/2"

    elif shape == "rounded_rect":
        rw = int(OUTPUT_W * 0.92); rw -= rw % 2
        rh = int(video_area_h * 0.88); rh -= rh % 2
        filters.append(
            f"[0:v]scale={rw}:{rh}:force_original_aspect_ratio=increase,"
            f"crop={rw}:{rh},"
            f"format=yuva420p,"
            f"vignette=PI/4[vmask]"
        )
        overlay_x = f"({OUTPUT_W}-{rw})/2"
        overlay_y = f"({video_area_h}-{rh})/2"
        circle_d = rw

    else:  # portrait_center
        filters.append(
            f"[0:v]scale={OUTPUT_W}:{video_area_h}:force_original_aspect_ratio=decrease,"
            f"pad={OUTPUT_W}:{video_area_h}:(ow-iw)/2:(oh-ih)/2:black,"
            f"format=yuva420p[vmask]"
        )
        overlay_x = "0"
        overlay_y = "0"
        _portrait_shape = True  # маркер для blurred_bg

    # ── Фон ─────────────────────────────────────────────────────────────────
    # Приоритет: bg_path (видео-фон) > BLURRED_BG_ENABLED (размытие) > чёрные полосы
    _use_blurred = (
        not has_bg
        and BLURRED_BG_ENABLED
        and locals().get("_portrait_shape", False)  # только portrait_center
    )

    if has_bg:
        filters.append(
            f"[{bg_idx}:v]scale={OUTPUT_W}:{OUTPUT_H}:force_original_aspect_ratio=increase,"
            f"crop={OUTPUT_W}:{OUTPUT_H},setsar=1,fps={OUTPUT_FPS}[bg]"
        )
        filters.append(
            f"[bg][vmask]overlay={overlay_x}:{overlay_y}:format=auto[vbase]"
        )
    elif _use_blurred:
        # Размытый фон: split источника → fg (масштабируется внутрь) + bg (размытый)
        # eq brightness: 0.0 = без изменений, отрицательное = темнее
        darken_adj = BLURRED_BG_DARKEN - 1.0  # 0.6 → -0.4
        filters.append(
            f"[0:v]split[_fg_src][_bg_src];"
            f"[_bg_src]scale={OUTPUT_W}:{OUTPUT_H}:force_original_aspect_ratio=increase,"
            f"crop={OUTPUT_W}:{OUTPUT_H},"
            f"boxblur={BLURRED_BG_SIGMA}:{BLURRED_BG_SIGMA},"
            f"eq=brightness={darken_adj:.2f},setsar=1[bg_blur];"
            f"[_fg_src]scale={OUTPUT_W}:{video_area_h}:force_original_aspect_ratio=decrease,"
            f"format=yuva420p[fg_for_blur]"
        )
        filters.append(
            f"[bg_blur][fg_for_blur]overlay=(W-w)/2:(H-h)/2:format=auto[vbase]"
        )
    else:
        filters.append(f"color=black:{OUTPUT_W}x{OUTPUT_H}:r={OUTPUT_FPS}[bg_black]")
        filters.append(f"[bg_black][vmask]overlay={overlay_x}:{overlay_y}:format=auto[vbase]")

    current = "[vbase]"

    # ── Визуальный фильтр (ФИЧА 3) ───────────────────────────────────────────
    # Порядок: после фона, перед баннером/текстом.
    # Фильтр берётся из meta["visual_filter"], затем из VIDEO_FILTER_DEFAULT.
    if VIDEO_FILTER_ENABLED or meta.get("visual_filter"):
        from pipeline.video_filters import get_filter, get_random_filter
        filter_name = meta.get("visual_filter") or VIDEO_FILTER_DEFAULT
        if VIDEO_FILTER_RANDOM and (not filter_name or filter_name == "none"):
            filter_name = get_random_filter()
        filter_str = get_filter(filter_name)
        if filter_str:
            filters.append(f"{current}{filter_str}[vfiltered]")
            current = "[vfiltered]"

    # ── Hook-zoom в первые N секунд (ФИЧА 5) ─────────────────────────────────
    # Ken Burns zoom-in: от HOOK_ZOOM_START до HOOK_ZOOM_END за HOOK_ZOOM_DURATION сек.
    # Не применяется к коротким видео (< HOOK_ZOOM_DURATION * 2).
    if HOOK_ZOOM_ENABLED and duration > HOOK_ZOOM_DURATION * 2:
        zoom_frames = int(HOOK_ZOOM_DURATION * OUTPUT_FPS)
        z_start = HOOK_ZOOM_START
        z_end   = HOOK_ZOOM_END
        zoom_expr = (
            f"if(lt(on,{zoom_frames}),"
            f"{z_start:.3f}+({z_end:.3f}-{z_start:.3f})*on/{zoom_frames},"
            f"{z_end:.3f})"
        )
        filters.append(
            f"{current}zoompan=z='{zoom_expr}'"
            f":x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)'"
            f":d=1:s={OUTPUT_W}x{OUTPUT_H}:fps={OUTPUT_FPS}[vzoomed]"
        )
        current = "[vzoomed]"

    # ── Баннер ───────────────────────────────────────────────────────────────
    if has_banner and banner_h_px > 0:
        banner_y = OUTPUT_H - banner_h_px
        filters.append(f"[{banner_idx}:v]scale={OUTPUT_W}:{banner_h_px}[banner_s]")
        filters.append(f"{current}[banner_s]overlay=0:{banner_y}[vbanner]")
        current = "[vbanner]"

    # ── Текстовые оверлеи ────────────────────────────────────────────────────
    text_filters: List[str] = []
    if font_str:
        fe = font_str.replace("\\", "/").replace(":", "\\:")

        hook_text = meta.get("hook_text", "")
        if hook_text:
            t = _escape_drawtext(hook_text)
            fs = _font_size_for_text(hook_text)
            text_filters.append(
                f"drawtext=fontfile='{fe}':text='{t}':"
                f"fontsize={fs}:fontcolor=white:borderw=3:bordercolor=black:"
                f"shadowx=2:shadowy=2:shadowcolor=black@0.8:"
                f"x=(w-text_w)/2:y=(h-text_h)/2:"
                f"enable='between(t,0,{HOOK_TEXT_DURATION})'"
            )

        loop_text = meta.get("loop_prompt", "")
        if loop_text:
            t = _escape_drawtext(loop_text)
            fs = _font_size_for_text(loop_text, base=48)
            t_start = max(0.0, duration - LOOP_PROMPT_DURATION)
            text_filters.append(
                f"drawtext=fontfile='{fe}':text='{t}':"
                f"fontsize={fs}:fontcolor=yellow:borderw=3:bordercolor=black:"
                f"shadowx=2:shadowy=2:shadowcolor=black@0.8:"
                f"x=(w-text_w)/2:y=h*0.85:"
                f"enable='between(t,{t_start:.2f},{duration:.2f})'"
            )

        for ov in meta.get("overlays", []):
            ov_text  = ov.get("text", "")
            ov_start = float(ov.get("start", 0))
            ov_dur   = float(ov.get("duration", OVERLAY_DEFAULT_DURATION))
            ov_end   = ov_start + ov_dur
            if not ov_text or ov_start >= duration:
                continue
            t  = _escape_drawtext(ov_text)
            fs = _font_size_for_text(ov_text, base=44)
            ox, oy = ("(w-text_w)/2", "h*0.8")
            if ":" in OVERLAY_POSITION:
                ox, oy = OVERLAY_POSITION.split(":", 1)
            text_filters.append(
                f"drawtext=fontfile='{fe}':text='{t}':"
                f"fontsize={fs}:fontcolor=white:borderw=2:bordercolor=black:"
                f"x={ox}:y={oy}:"
                f"enable='between(t,{ov_start:.2f},{ov_end:.2f})'"
            )

    if text_filters:
        filters.append(f"{current}" + ",".join(text_filters) + "[vout]")
    else:
        filters.append(f"{current}null[vout]")

    return ";".join(filters)


# ─────────────────────────────────────────────────────────────────────────────
# Постобработка одного клипа
# ─────────────────────────────────────────────────────────────────────────────

def _postprocess_single(
    clip_path: Path,
    out_path: Path,
    banner_path: Optional[Path],
    font_str: str,
    vcodec: str,
    vcodec_opts: Dict,
    meta: Dict,
    shape: str,
    bg_path: Optional[Path] = None,
    tts_audio_path: Optional[Path] = None,
) -> bool:
    try:
        info      = probe_video(clip_path)
        duration  = info["duration"]
        w, h      = info["width"], info["height"]
        has_audio = info["has_audio"]

        is_landscape = w > h
        circle_ratio = (CIRCLE_RATIO_LANDSCAPE if is_landscape else CIRCLE_RATIO_PORTRAIT)
        circle_ratio = circle_ratio + random.uniform(-CIRCLE_VARIATION, CIRCLE_VARIATION)
        circle_ratio = max(0.5, min(0.98, circle_ratio))

        actual_banner = banner_path or _pick_random_banner()
        has_banner    = actual_banner is not None and actual_banner.exists()
        banner_h_px   = int(OUTPUT_H * BANNER_HEIGHT_PCT) if has_banner else 0
        banner_h_px  -= banner_h_px % 2

        has_bg  = bg_path is not None and bg_path.exists()
        has_tts = tts_audio_path is not None and Path(tts_audio_path).exists()

        # Индексы входных потоков
        bg_idx, banner_idx, tts_idx = -1, -1, -1
        next_idx = 1
        if has_bg:
            bg_idx = next_idx; next_idx += 1
        if has_banner:
            banner_idx = next_idx; next_idx += 1
        if has_tts:
            tts_idx = next_idx; next_idx += 1

        out_path.parent.mkdir(parents=True, exist_ok=True)

        fc = _build_filter_complex(
            duration=duration,
            has_audio=has_audio,
            has_bg=has_bg,
            has_banner=has_banner,
            banner_h_px=banner_h_px,
            shape=shape,
            font_str=font_str,
            meta=meta,
            circle_ratio=circle_ratio,
            bg_idx=bg_idx,
            banner_idx=banner_idx,
        )

        # ── Строим ffmpeg команду ──────────────────────────────────────
        cmd = ["ffmpeg", "-y", "-i", str(clip_path)]
        if has_bg:
            cmd += ["-stream_loop", "-1", "-i", str(bg_path)]
        if has_banner:
            cmd += ["-i", str(actual_banner)]
        if has_tts:
            cmd += ["-i", str(tts_audio_path)]

        cmd += ["-filter_complex", fc, "-map", "[vout]"]

        # ── Аудио: оригинал + TTS mix ──────────────────────────────────
        if has_tts and has_audio:
            # Микшируем оригинальный аудио + TTS голос
            # TTS_VOICE_OVER_MIX = 0.85 → голос 85%, оригинал 15%
            orig_vol  = round(1.0 - TTS_VOICE_OVER_MIX, 2)
            voice_vol = round(TTS_VOICE_OVER_MIX * TTS_VOLUME, 2)
            # Обрезаем TTS по длине видео, затем миксуем
            audio_fc = (
                f"[0:a]volume={orig_vol}[orig_a];"
                f"[{tts_idx}:a]apad,atrim=duration={duration:.3f},"
                f"volume={voice_vol}[tts_a];"
                f"[orig_a][tts_a]amix=inputs=2:duration=first:normalize=0[aout]"
            )
            cmd += [
                "-filter_complex", f"{fc};{audio_fc}",
                "-map", "[vout]",
                "-map", "[aout]",
                "-c:a", "aac", "-b:a", AUDIO_BITRATE,
            ]
            # Убираем предыдущий -map [vout] (он уже внутри filter_complex)
            # Пересобираем команду без дублирования
            cmd = ["ffmpeg", "-y", "-i", str(clip_path)]
            if has_bg:
                cmd += ["-stream_loop", "-1", "-i", str(bg_path)]
            if has_banner:
                cmd += ["-i", str(actual_banner)]
            if has_tts:
                cmd += ["-i", str(tts_audio_path)]

            combined_fc = f"{fc};{audio_fc}"
            cmd += ["-filter_complex", combined_fc,
                    "-map", "[vout]", "-map", "[aout]",
                    "-c:a", "aac", "-b:a", AUDIO_BITRATE]

        elif has_tts and not has_audio:
            # Только TTS голос — оригинал без аудио
            audio_fc = (
                f"[{tts_idx}:a]apad,atrim=duration={duration:.3f},"
                f"volume={TTS_VOLUME:.2f}[aout]"
            )
            cmd = ["ffmpeg", "-y", "-i", str(clip_path)]
            if has_bg:
                cmd += ["-stream_loop", "-1", "-i", str(bg_path)]
            if has_banner:
                cmd += ["-i", str(actual_banner)]
            if has_tts:
                cmd += ["-i", str(tts_audio_path)]

            combined_fc = f"{fc};{audio_fc}"
            cmd += ["-filter_complex", combined_fc,
                    "-map", "[vout]", "-map", "[aout]",
                    "-c:a", "aac", "-b:a", AUDIO_BITRATE]

        else:
            # Без TTS — оригинальная логика
            cmd += ["-filter_complex", fc, "-map", "[vout]"]
            if has_audio:
                cmd += ["-map", "0:a", "-c:a", "aac", "-b:a", AUDIO_BITRATE]
            else:
                cmd += ["-an"]

        cmd += ["-c:v", vcodec]
        for k, v in (vcodec_opts or {}).items():
            cmd += [f"-{k}", str(v)]

        cmd += [
            "-r", str(OUTPUT_FPS),
            "-pix_fmt", "yuv420p",
            "-t", str(duration),
            "-movflags", "+faststart",
            str(out_path),
        ]

        logger.debug("ffmpeg cmd: %s", " ".join(cmd))
        subprocess.run(cmd, check=True, capture_output=True, timeout=300)

        if out_path.exists() and out_path.stat().st_size > 0:
            logger.info("Постобработка OK: %s (shape=%s, tts=%s)", out_path.name, shape, has_tts)
            return True

        logger.error("Постобработка: выходной файл пуст: %s", out_path)
        return False

    except subprocess.CalledProcessError as e:
        logger.error("ffmpeg ошибка %s:\n%s", clip_path.name,
                     e.stderr.decode(errors="replace")[-800:])
        return False
    except subprocess.TimeoutExpired:
        logger.error("ffmpeg timeout >300s для %s", clip_path.name)
        return False
    except Exception as e:
        logger.error("Постобработка %s: %s", clip_path.name, e)
        return False
    try:
        info      = probe_video(clip_path)
        duration  = info["duration"]
        w, h      = info["width"], info["height"]
        has_audio = info["has_audio"]

        is_landscape = w > h
        circle_ratio = (CIRCLE_RATIO_LANDSCAPE if is_landscape else CIRCLE_RATIO_PORTRAIT)
        circle_ratio = circle_ratio + random.uniform(-CIRCLE_VARIATION, CIRCLE_VARIATION)
        circle_ratio = max(0.5, min(0.98, circle_ratio))

        actual_banner = banner_path or _pick_random_banner()
        has_banner    = actual_banner is not None and actual_banner.exists()
        banner_h_px   = int(OUTPUT_H * BANNER_HEIGHT_PCT) if has_banner else 0
        banner_h_px  -= banner_h_px % 2

        has_bg = bg_path is not None and bg_path.exists()

        # Индексы входных потоков
        bg_idx, banner_idx = -1, -1
        next_idx = 1
        if has_bg:
            bg_idx = next_idx; next_idx += 1
        if has_banner:
            banner_idx = next_idx; next_idx += 1

        out_path.parent.mkdir(parents=True, exist_ok=True)

        fc = _build_filter_complex(
            duration=duration,
            has_audio=has_audio,
            has_bg=has_bg,
            has_banner=has_banner,
            banner_h_px=banner_h_px,
            shape=shape,
            font_str=font_str,
            meta=meta,
            circle_ratio=circle_ratio,
            bg_idx=bg_idx,
            banner_idx=banner_idx,
        )

        cmd = ["ffmpeg", "-y", "-i", str(clip_path)]
        if has_bg:
            cmd += ["-stream_loop", "-1", "-i", str(bg_path)]
        if has_banner:
            cmd += ["-i", str(actual_banner)]

        cmd += ["-filter_complex", fc, "-map", "[vout]"]

        if has_audio:
            cmd += ["-map", "0:a", "-c:a", "aac", "-b:a", AUDIO_BITRATE]
        else:
            cmd += ["-an"]

        cmd += ["-c:v", vcodec]
        for k, v in (vcodec_opts or {}).items():
            cmd += [f"-{k}", str(v)]

        cmd += [
            "-r", str(OUTPUT_FPS),
            "-pix_fmt", "yuv420p",
            "-t", str(duration),
            "-movflags", "+faststart",
            str(out_path),
        ]

        logger.debug("ffmpeg cmd: %s", " ".join(cmd))
        subprocess.run(cmd, check=True, capture_output=True, timeout=300)

        if out_path.exists() and out_path.stat().st_size > 0:
            logger.info("Постобработка OK: %s (shape=%s)", out_path.name, shape)
            return True

        logger.error("Постобработка: выходной файл пуст: %s", out_path)
        return False

    except subprocess.CalledProcessError as e:
        logger.error("ffmpeg ошибка %s:\n%s", clip_path.name,
                     e.stderr.decode(errors="replace")[-800:])
        return False
    except subprocess.TimeoutExpired:
        logger.error("ffmpeg timeout >300s для %s", clip_path.name)
        return False
    except Exception as e:
        logger.error("Постобработка %s: %s", clip_path.name, e)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Публичный интерфейс
# ─────────────────────────────────────────────────────────────────────────────

def stage_postprocess(
    clips: List[Path],
    banner_path: Optional[Path],
    vcodec: str,
    vcodec_opts: Optional[Dict] = None,
    metadata_variants: Optional[List[Dict]] = None,
    bg_path: Optional[Path] = None,
    tts_audio_paths: Optional[List[Optional[Path]]] = None,
) -> List[Path]:
    """
    Постобработка списка клипов. Возвращает список готовых путей.

    Args:
        tts_audio_paths: список .wav файлов (по одному на клип) или None.
                         Если передан — голос микшируется с оригинальным аудио.
    """
    if metadata_variants is None:
        metadata_variants = []
    if vcodec_opts is None:
        vcodec_opts = {}
    if tts_audio_paths is None:
        tts_audio_paths = []

    font_ok  = _check_font()
    font_str = str(FONT_PATH) if font_ok else ""

    successful: List[Path] = []
    for i, clip_path in enumerate(clips):
        meta     = random.choice(metadata_variants) if metadata_variants else {}
        shape    = random.choice(SHAPES)
        out_path = config.OUTPUT_DIR / clip_path.with_suffix(".mp4").name

        # TTS файл для этого клипа (если есть)
        tts_path = tts_audio_paths[i] if i < len(tts_audio_paths) else None

        if _postprocess_single(
            clip_path=clip_path,
            out_path=out_path,
            banner_path=banner_path,
            font_str=font_str,
            vcodec=vcodec,
            vcodec_opts=vcodec_opts,
            meta=meta,
            shape=shape,
            bg_path=bg_path,
            tts_audio_path=tts_path,
        ):
            successful.append(out_path)
        else:
            logger.warning("Постобработка не удалась: %s", clip_path.name)

    logger.info("Постобработка: %d/%d OK", len(successful), len(clips))
    return successful
