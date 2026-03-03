# postprocessor.py
import logging
import random
from pathlib import Path
from typing import Dict, List, Optional

import ffmpeg

from pipeline.config import (
    OUTPUT_W, OUTPUT_H, OUTPUT_FPS,
    CIRCLE_RATIO_LANDSCAPE, CIRCLE_RATIO_PORTRAIT,
    CIRCLE_VARIATION, AUDIO_BITRATE,
    BANNER_DIR, BANNER_HEIGHT_PCT, CIRCLE_OFFSET_PCT,  # NEW PARAMS 2026
    FONT_PATH,
    HOOK_TEXT_DURATION, HOOK_TEXT_POSITION,
    LOOP_PROMPT_DURATION,
    OVERLAY_DEFAULT_DURATION, OVERLAY_POSITION,
)
from pipeline.utils import probe_video

logger = logging.getLogger(__name__)


def _pick_random_banner() -> Optional[Path]:
    """Возвращает случайный файл баннера из BANNER_DIR или None, если папка пуста."""
    banner_dir = Path(BANNER_DIR)
    if not banner_dir.exists():
        return None
    candidates = [
        p for p in banner_dir.iterdir()
        if p.suffix.lower() in ('.png', '.jpg', '.jpeg', '.webp')
    ]
    return random.choice(candidates) if candidates else None


def _check_font() -> bool:
    """
    Проверяет наличие файла шрифта FONT_PATH.
    Логирует предупреждение, если шрифт не найден.
    Возвращает True, если шрифт доступен.
    """
    font = Path(FONT_PATH)
    if not font.exists():
        logger.warning(
            "Шрифт не найден: %s — текстовые оверлеи будут пропущены. "
            "Поместите Roboto-Bold.ttf в assets/fonts/",
            FONT_PATH,
        )
        return False
    return True


def _resolve_text_position(position: str, is_hook: bool = False) -> Dict[str, str]:
    """
    Преобразует настройку позиции текста в параметры x/y для ffmpeg drawtext.

    Args:
        position: "center" или FFmpeg-выражение вида "expr_x:expr_y"
        is_hook: True для hook_text (по центру экрана)

    Returns:
        Словарь {'x': ..., 'y': ...}
    """
    if position == "center":
        return {
            'x': '(w-text_w)/2',
            'y': '(h-text_h)/2',
        }
    # Пользовательское выражение: "x_expr:y_expr"
    if ':' in position:
        x_expr, y_expr = position.split(':', 1)
        return {'x': x_expr, 'y': y_expr}
    # Fallback — центр
    return {'x': '(w-text_w)/2', 'y': '(h-text_h)/2'}


def _resolve_overlay_position(position: str) -> Dict[str, str]:
    """
    Аналогично _resolve_text_position, но для оверлеев.
    Парсит строку формата "x=expr:y=expr" (как в OVERLAY_POSITION).
    """
    # Формат "x=<expr>:y=<expr>"
    xy = {}
    for part in position.split(':'):
        part = part.strip()
        if part.startswith('x='):
            xy['x'] = part[2:]
        elif part.startswith('y='):
            xy['y'] = part[2:]
    if 'x' in xy and 'y' in xy:
        return xy
    # Fallback
    return {'x': '(w-text_w)/2', 'y': 'h*0.8'}


def _escape_drawtext(text: str) -> str:
    """
    Экранирует специальные символы для FFmpeg drawtext.
    Символы ' : \\ [ ] { } нужно экранировать.
    """
    # Порядок важен: сначала бэкслеш
    text = text.replace('\\', '\\\\')
    text = text.replace("'",  "\\'")
    text = text.replace(':',  '\\:')
    return text


def _apply_drawtext(
    stream,
    text: str,
    font_path: str,
    font_size: int,
    enable_expr: str,
    x_expr: str,
    y_expr: str,
    font_color: str = 'white',
    border_color: str = 'black',
    border_w: int = 2,
):
    """
    Применяет фильтр drawtext к видеопотоку ffmpeg.

    Args:
        stream:       ffmpeg-поток (video node)
        text:         отображаемый текст (будет экранирован)
        font_path:    абсолютный путь к .ttf-файлу
        font_size:    размер шрифта в пикселях
        enable_expr:  выражение enable= (например 'between(t,0,3)')
        x_expr:       выражение для x-координаты
        y_expr:       выражение для y-координаты
        font_color:   цвет шрифта
        border_color: цвет обводки
        border_w:     толщина обводки (px)

    Returns:
        Обновлённый ffmpeg-поток с наложенным текстом.
    """
    safe_text = _escape_drawtext(text)
    return stream.drawtext(
        text=safe_text,
        fontfile=font_path,
        fontsize=font_size,
        fontcolor=font_color,
        bordercolor=border_color,
        borderw=border_w,
        x=x_expr,
        y=y_expr,
        enable=enable_expr,
    )


def stage_postprocess(
    clip_path: Path,
    bg_path: str,
    out_path: Path,
    vcodec: str,
    vcodec_opts: Dict,
    meta: Optional[Dict] = None,
) -> bool:
    """
    Накладывает клип на фон в виде круга.
    Баннер размещается сверху (top-center, высота BANNER_HEIGHT_PCT * OUTPUT_H).
    Круг располагается ниже баннера с отступом CIRCLE_OFFSET_PCT * OUTPUT_H.
    Аудио полностью удаляется — выходной файл без звука.

    Если переданы метаданные (meta), применяются текстовые оверлеи:
      - hook_text:   текст-крючок в первые HOOK_TEXT_DURATION секунд
      - overlays:    список {time, text, duration} — тайм-оверлеи
      - loop_prompt: призыв к пересмотру в последние LOOP_PROMPT_DURATION секунд

    Args:
        clip_path:   путь к исходному клипу
        bg_path:     путь к фоновому видео
        out_path:    путь к выходному файлу
        vcodec:      видеокодек (libx264 или h264_nvenc)
        vcodec_opts: дополнительные опции кодека
        meta:        словарь метаданных (опционально; обратная совместимость)
    """
    # Нормализуем meta — None или отсутствующие поля не должны ломать обработку
    if meta is None:
        meta = {}

    hook_text   = meta.get('hook_text',   '') or ''
    overlays    = meta.get('overlays',    []) or []
    loop_prompt = meta.get('loop_prompt', '') or ''

    # Проверяем шрифт один раз (если есть хоть один текстовый оверлей)
    has_text = bool(hook_text or overlays or loop_prompt)
    font_ok  = _check_font() if has_text else False
    font_str = str(FONT_PATH)

    try:
        ci = probe_video(str(clip_path))
        clip_ar  = ci['width'] / ci['height']
        duration = ci['duration']

        # --- Геометрия баннера ---
        banner_h = int(OUTPUT_H * BANNER_HEIGHT_PCT)   # NEW PARAMS 2026
        banner_h -= banner_h % 2                        # чётное число пикселей

        # --- Геометрия круга ---
        circle_offset   = int(OUTPUT_H * CIRCLE_OFFSET_PCT)  # NEW PARAMS 2026
        base_ratio      = CIRCLE_RATIO_LANDSCAPE if clip_ar >= 1.0 else CIRCLE_RATIO_PORTRAIT
        ratio_variation = random.uniform(1.0 - CIRCLE_VARIATION, 1.0 + CIRCLE_VARIATION)

        circle_d = int(OUTPUT_W * base_ratio * ratio_variation)
        circle_d -= circle_d % 2

        # Горизонтальный центр, вертикально — ниже баннера + отступ
        cx = (OUTPUT_W - circle_d) // 2
        cy = banner_h + circle_offset
        r  = circle_d // 2

        # --- Фон ---
        bg_in = ffmpeg.input(bg_path, stream_loop=-1, t=duration)
        bg_v = (
            bg_in.video
            .filter('scale', OUTPUT_W, OUTPUT_H,
                    force_original_aspect_ratio='increase')
            .filter('crop', OUTPUT_W, OUTPUT_H)
            .filter('fps', OUTPUT_FPS)
            .filter('setsar', '1/1')
        )

        # --- Клип в круге ---
        clip_in = ffmpeg.input(str(clip_path))
        clip_v = (
            clip_in.video
            .filter('scale', circle_d, circle_d,
                    force_original_aspect_ratio='increase')
            .filter('crop', circle_d, circle_d)
            .filter('fps', OUTPUT_FPS)
            .filter('format', 'yuva420p')
            .filter(
                'geq',
                lum='p(X,Y)', cb='p(X,Y)', cr='p(X,Y)',
                a=f'if(lte(hypot(X-{r},Y-{r}),{r}),255,0)',
            )
        )

        # Наложить круг с клипом на фон
        video_out = ffmpeg.overlay(bg_v, clip_v, x=cx, y=cy, shortest=1)

        # --- Баннер сверху (top-center) ---
        banner_path = _pick_random_banner()
        if banner_path:
            banner_in = ffmpeg.input(str(banner_path), stream_loop=-1, t=duration)
            banner_v = (
                banner_in.video
                .filter('scale', OUTPUT_W, banner_h,
                        force_original_aspect_ratio='disable')
                .filter('setsar', '1/1')
            )
            video_out = ffmpeg.overlay(video_out, banner_v, x=0, y=0, shortest=1)

        # ── Текстовые оверлеи (только если шрифт доступен) ────────────────

        if font_ok:

            # --- hook_text: текст-крючок в начале ---
            if hook_text:
                pos = _resolve_text_position(HOOK_TEXT_POSITION, is_hook=True)
                try:
                    video_out = _apply_drawtext(
                        stream=video_out,
                        text=hook_text,
                        font_path=font_str,
                        font_size=52,
                        enable_expr=f'between(t,0,{HOOK_TEXT_DURATION})',
                        x_expr=pos['x'],
                        y_expr=pos['y'],
                        font_color='white',
                        border_color='black',
                        border_w=3,
                    )
                    logger.debug("hook_text применён: '%s'", hook_text)
                except Exception as e:
                    logger.warning("Не удалось применить hook_text '%s': %s", hook_text, e)

            # --- overlays: тайм-оверлеи по ходу видео ---
            for ov in overlays:
                ov_text = ov.get('text', '').strip()
                ov_time = ov.get('time', 0.0)
                ov_dur  = ov.get('duration', OVERLAY_DEFAULT_DURATION)
                if not ov_text:
                    continue
                # Пропускаем оверлеи, выходящие за длину клипа
                if ov_time >= duration:
                    logger.debug(
                        "Оверлей '%s' at %.2fs пропущен: выходит за длину клипа %.2fs",
                        ov_text, ov_time, duration,
                    )
                    continue
                ov_end = min(ov_time + ov_dur, duration)
                pos = _resolve_overlay_position(OVERLAY_POSITION)
                try:
                    video_out = _apply_drawtext(
                        stream=video_out,
                        text=ov_text,
                        font_path=font_str,
                        font_size=38,
                        enable_expr=f'between(t,{ov_time},{ov_end})',
                        x_expr=pos['x'],
                        y_expr=pos['y'],
                        font_color='white',
                        border_color='black',
                        border_w=2,
                    )
                    logger.debug("Оверлей применён: '%s' @ %.2f→%.2f", ov_text, ov_time, ov_end)
                except Exception as e:
                    logger.warning("Не удалось применить оверлей '%s': %s", ov_text, e)

            # --- loop_prompt: призыв к пересмотру в конце ---
            if loop_prompt and duration > LOOP_PROMPT_DURATION:
                lp_start = duration - LOOP_PROMPT_DURATION
                pos = _resolve_overlay_position(OVERLAY_POSITION)
                try:
                    video_out = _apply_drawtext(
                        stream=video_out,
                        text=loop_prompt,
                        font_path=font_str,
                        font_size=42,
                        enable_expr=f'between(t,{lp_start},{duration})',
                        x_expr=pos['x'],
                        y_expr=pos['y'],
                        font_color='yellow',
                        border_color='black',
                        border_w=3,
                    )
                    logger.debug("loop_prompt применён: '%s'", loop_prompt)
                except Exception as e:
                    logger.warning("Не удалось применить loop_prompt '%s': %s", loop_prompt, e)

        # --- Аудио: полностью удаляем (тишина) ---
        # NEW PARAMS 2026 — шортс выходит без звука
        audio_out = ffmpeg.input(
            'anullsrc=r=44100:cl=stereo', f='lavfi', t=duration
        ).audio

        opts = vcodec_opts or {}
        (ffmpeg.output(
            video_out, audio_out, str(out_path),
            vcodec=vcodec, acodec='aac',
            audio_bitrate=AUDIO_BITRATE,
            format='mp4', pix_fmt='yuv420p',
            movflags='+faststart', map_metadata=-1,
            **opts,
        )
         .overwrite_output()
         .run(capture_stdout=True, capture_stderr=True))
        return True

    except ffmpeg.Error as e:
        err = e.stderr.decode('utf-8', errors='replace')[-200:] if e.stderr else str(e)
        logger.error("❌ Постобработка %s: %s", clip_path.name, err)
        return False
    except Exception as e:
        logger.error("❌ Постобработка %s: %s", clip_path.name, e)
        return False
