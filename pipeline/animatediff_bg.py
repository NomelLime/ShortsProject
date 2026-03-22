"""
Генерация фонового видео, когда в assets/backgrounds нет подходящего .mp4.

Порядок (если ANIMATEDIFF_ENABLED=1):
  1. Внешний скрипт ANIMATEDIFF_SCRIPT — аргументы: <topic> <output_mp4>
     (интеграция с настоящим AnimateDiff / ComfyUI / вашим пайплайном).
  2. FFmpeg Ken-Burns по статичному изображению (.jpg/.png) из backgrounds — без ML.

Полноценный AnimateDiff в репозиторий не вшит (гигабайты весов + diffusers);
подключайте через ANIMATEDIFF_SCRIPT или отдельный сервис.
"""
from __future__ import annotations

import logging
import random
import re
import shutil
import subprocess
import tempfile
from contextlib import nullcontext
from pathlib import Path
from typing import Callable, ContextManager, List, Optional

logger = logging.getLogger(__name__)


def _slug_topic(topic: str) -> str:
    s = re.sub(r"[^\w\s-]", "", topic, flags=re.UNICODE)[:80]
    return s.strip() or "bg"


def _list_bg_images(bg_dir: Path) -> List[Path]:
    out: List[Path] = []
    for ext in ("*.jpg", "*.jpeg", "*.png", "*.webp"):
        out.extend(bg_dir.glob(ext))
    return out


def _pick_image_for_topic(bg_dir: Path, topic: str) -> Optional[Path]:
    images = _list_bg_images(bg_dir)
    if not images:
        return None
    words = [w for w in topic.lower().split() if len(w) > 2]
    if words:
        scored = []
        for p in images:
            stem = p.stem.lower()
            score = sum(1 for w in words if w in stem)
            scored.append((score, p))
        scored.sort(key=lambda x: -x[0])
        if scored[0][0] > 0:
            return scored[0][1]
    return random.choice(images)


def _run_external_script(script: str, topic: str, out_path: Path) -> bool:
    try:
        r = subprocess.run(
            [script, topic, str(out_path)],
            capture_output=True,
            text=True,
            timeout=3600,
        )
        if r.returncode != 0:
            logger.warning(
                "[AnimateDiff] скрипт вернул %s: %s",
                r.returncode,
                (r.stderr or r.stdout or "")[:500],
            )
            return False
        return out_path.exists() and out_path.stat().st_size > 0
    except FileNotFoundError:
        logger.warning("[AnimateDiff] ANIMATEDIFF_SCRIPT не найден: %s", script)
        return False
    except subprocess.TimeoutExpired:
        logger.error("[AnimateDiff] таймаут внешнего скрипта")
        return False
    except Exception as e:
        logger.warning("[AnimateDiff] ошибка скрипта: %s", e)
        return False


def _ffmpeg_ken_burns(image: Path, out_path: Path, duration: int, size: str) -> bool:
    if not shutil.which("ffmpeg"):
        logger.warning("[AnimateDiff] ffmpeg не в PATH — Ken-Burns недоступен")
        return False
    # Формат: "1280:720" (как в ffmpeg scale/pad)
    if ":" in size:
        w, h = size.split(":", 1)
    else:
        w, h = "1280", "720"
    # zoompan: ~5 c при 25 fps → d=125; плавный зум
    s_wh = f"{w}x{h}"
    vf = (
        f"scale={w}:{h}:force_original_aspect_ratio=decrease,"
        f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2:black,"
        f"zoompan=z='min(zoom+0.002,1.25)':d=125:x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':s={s_wh}:fps=25"
    )
    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        "-loop", "1", "-i", str(image),
        "-vf", vf,
        "-t", str(duration),
        "-pix_fmt", "yuv420p",
        str(out_path),
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if r.returncode != 0:
            logger.warning("[AnimateDiff] ffmpeg: %s", (r.stderr or "")[:400])
            return False
        return out_path.exists() and out_path.stat().st_size > 0
    except Exception as e:
        logger.warning("[AnimateDiff] ffmpeg ошибка: %s", e)
        return False


def generate_motion_background(
    topic: str,
    acquire_script_gpu: Optional[Callable[[], ContextManager]] = None,
) -> Optional[Path]:
    """
    Возвращает путь к сгенерированному .mp4 или None.

    Требует ANIMATEDIFF_ENABLED=1 в окружении (см. config.ANIMATEDIFF_ENABLED).

    acquire_script_gpu: фабрика контекст-менеджера (например GPUResourceManager.acquire)
    для запуска ANIMATEDIFF_SCRIPT; Ken-Burns после скрипта выполняется без этого lock.
    """
    from pipeline import config as cfg

    if not getattr(cfg, "ANIMATEDIFF_ENABLED", False):
        return None

    slug = _slug_topic(topic)
    tmp_dir = Path(tempfile.mkdtemp(prefix="animatediff_bg_"))
    out_path = tmp_dir / f"motion_{slug[:40]}.mp4"

    script = str(getattr(cfg, "ANIMATEDIFF_SCRIPT", "") or "").strip()
    if script:
        logger.info("[AnimateDiff] внешний скрипт: %s topic=%r", script, topic[:60])
        cm = (
            acquire_script_gpu()
            if acquire_script_gpu is not None
            else nullcontext()
        )
        with cm:
            script_ok = _run_external_script(script, topic, out_path)
        if script_ok:
            logger.info("[AnimateDiff] готово: %s", out_path)
            return out_path
        if not getattr(cfg, "ANIMATEDIFF_FF_FALLBACK", True):
            return None
        # после неудачи скрипта — пробуем Ken-Burns (новый tmp-файл)
        out_path = tmp_dir / f"motion_fb_{slug[:36]}.mp4"

    if not getattr(cfg, "ANIMATEDIFF_FF_FALLBACK", True):
        logger.info("[AnimateDiff] скрипт не задан, FF-fallback выключен — пропуск")
        return None

    bg_dir = Path(cfg.BASE_DIR) / "assets" / "backgrounds"
    if not bg_dir.is_dir():
        logger.debug("[AnimateDiff] нет каталога backgrounds для Ken-Burns")
        return None

    image = _pick_image_for_topic(bg_dir, topic)
    if image is None:
        logger.debug("[AnimateDiff] нет .jpg/.png в backgrounds для Ken-Burns")
        return None

    duration = int(getattr(cfg, "ANIMATEDIFF_DURATION_SEC", 5) or 5)
    size = str(getattr(cfg, "ANIMATEDIFF_SIZE", "1280:720"))

    logger.info("[AnimateDiff] Ken-Burns из %s → %s", image.name, out_path.name)
    if _ffmpeg_ken_burns(image, out_path, duration=duration, size=size):
        return out_path

    try:
        tmp_dir.rmdir()
    except OSError:
        pass
    return None
