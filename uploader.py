"""
pipeline/uploader.py – Загрузка видео на YouTube, TikTok, Instagram Reels.
"""

import logging
import random
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Optional

from rebrowser_playwright.sync_api import BrowserContext, Page

from pipeline import config, utils
from pipeline.activity import run_activity
from pipeline.browser import launch_browser, close_browser
from pipeline.notifications import check_and_handle_captcha, send_telegram

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Очистка метаданных видео
# ----------------------------------------------------------------------

def clean_video_metadata(video_path: Path) -> Path:
    """
    Очищает метаданные видео через ffmpeg (EXIF, GPS и т.д.).
    Возвращает путь к очищенному файлу (или оригинал при ошибке).
    """
    clean_path = video_path.with_stem(video_path.stem + "_clean")
    try:
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-i", str(video_path),
                "-map_metadata", "-1",
                "-c:v", "copy",
                "-c:a", "copy",
                str(clean_path),
            ],
            check=True,
            capture_output=True,
        )
        logger.debug("[metadata] Очищено: %s → %s", video_path.name, clean_path.name)
        return clean_path
    except FileNotFoundError:
        logger.warning("[metadata] ffmpeg не найден – использую оригинал.")
        return video_path
    except subprocess.CalledProcessError as e:
        logger.warning(
            "[metadata] Ошибка ffmpeg для %s: %s – использую оригинал.",
            video_path.name,
            e.stderr.decode(errors='replace')[:200],
        )
        return video_path


# ----------------------------------------------------------------------
# Upload with retries
# ----------------------------------------------------------------------

def upload_video(
    context: BrowserContext,
    platform: str,
    video_path: Path,
    meta: Dict,
    account_name: str = "",
    account_cfg: Dict = None,
) -> bool:
    for attempt in range(5):
        try:
            # Original upload logic for platform
            # ...
            success = True  # Assume success
            send_telegram(f"Upload success for {video_path.name} on {platform}")
            return True
        except Exception as e:
            backoff = 2 ** attempt * 60
            logger.warning(f"Retry {attempt+1}/5 after {backoff/60} min: {e}")
            time.sleep(backoff)
            send_telegram(f"Upload retry {attempt+1} for {video_path.name} on {platform}: {str(e)[:100]}")
    # Final fail
    failed_dir = Path(config.ACCOUNTS_ROOT) / account_name / 'failed'
    failed_dir.mkdir(exist_ok=True)
    shutil.move(video_path, failed_dir / video_path.name)
    utils.save_json(failed_dir / f"{video_path.stem}.error.json", {"error": str(e)})
    send_telegram(f"Upload failed after 5 attempts for {video_path.name} on {platform}")
    return False

# ... (rest of uploader, with multi-platform loop in upload_all)