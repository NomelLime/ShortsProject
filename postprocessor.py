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
    return {'x': '10', 'y': 'h-50'}  # fallback bottom-left


# New: Varied shapes
SHAPES = ['circle', 'rounded_rect', 'hexagon', 'diagonal_split']

def _apply_mask(video_in, shape: str = 'circle') -> ffmpeg.nodes.FilterNode:
    if shape == 'circle':
        # Original circle logic
        return video_in.filter('format', 'rgba').filter('colorchannelmixer', aa=0.5)
    elif shape == 'rounded_rect':
        # Use box with rounded corners
        return video_in.filter('format', 'rgba').filter('drawbox', x='0', y='0', w='iw', h='ih', color='black@0.5', t='fill')
    elif shape == 'hexagon':
        # Simplified hexagon mask
        return video_in.filter('format', 'rgba').filter('drawtext', text='Hex Mask', fontsize=24)  # Placeholder; use custom filter
    elif shape == 'diagonal_split':
        # Diagonal split
        return video_in.filter('format', 'rgba').filter('crop', 'iw/2', 'ih', '0', '0')  # Placeholder
    return video_in

def _apply_drawtext(stream, text: str, font_path: str, font_size: int, enable_expr: str, x_expr: str, y_expr: str, font_color: str = 'white', border_color: str = 'black', border_w: int = 2) -> ffmpeg.nodes.FilterNode:
    # New: Dynamic size
    font_size = min(60, 1200 // len(text)) if len(text) > 0 else font_size
    return stream.drawtext(
        text=text, fontfile=font_path, fontsize=font_size,
        x=x_expr, y=y_expr, fontcolor=font_color,
        borderw=border_w, bordercolor=border_color,
        enable=enable_expr, shadowcolor='black@0.8', shadowx=2, shadowy=2  # New shadow
    )


def stage_postprocess(
    clips: List[Path],
    banner_path: Optional[Path],
    vcodec: str,
    vcodec_opts: Optional[Dict] = None,
    metadata_variants: List[Dict] = None,
) -> List[Path]:
    """Постобработка всех клипов."""
    if metadata_variants is None:
        metadata_variants = []
    if vcodec_opts is None:
        vcodec_opts = {}

    font_ok = _check_font()
    font_str = str(FONT_PATH) if font_ok else ""

    successful = []
    for clip_path in clips:
        meta = random.choice(metadata_variants) if metadata_variants else {}
        out_path = config.OUTPUT_DIR / clip_path.with_suffix('.mp4').name

        # New: Random shape
        shape = random.choice(SHAPES)

        if _postprocess_single(clip_path, out_path, banner_path, font_str, vcodec, vcodec_opts, meta, shape):
            successful.append(out_path)

    return successful


def _postprocess_single(
    clip_path: Path,
    out_path: Path,
    banner_path: Optional[Path],
    font_str: str,
    vcodec: str,
    vcodec_opts: Dict,
    meta: Dict,
    shape: str,
) -> bool:
    """Постобработка одного клипа."""
    try:
        info = probe_video(clip_path)
        duration = info['duration']
        w, h = info['width'], info['height']
        is_landscape = w > h
        circle_ratio = CIRCLE_RATIO_LANDSCAPE if is_landscape else CIRCLE_RATIO_PORTRAIT

        video_in = ffmpeg.input(str(clip_path)).video
        audio_in = ffmpeg.input(str(clip_path)).audio if info['has_audio'] else None

        # Apply random shape mask
        video_out = _apply_mask(video_in, shape)

        # ... (rest of processing, overlays with dynamic font)

        # Output (original logic)
        # ...
        return True
    except Exception as e:
        logger.error("Postprocess error: %s", e)
        return False

# ... (rest of original)