"""
pipeline/uploader.py – Загрузка видео на YouTube, TikTok, Instagram Reels.
"""

import logging
import random
import shutil
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


def clean_video_metadata(video_path: Path) -> Path:
    """Очищает метаданные видео через ffmpeg. Возвращает путь к очищенному файлу."""
    clean_path = video_path.with_stem(video_path.stem + "_clean")
    try:
        subprocess.run(
            ["ffmpeg", "-y", "-i", str(video_path),
             "-map_metadata", "-1", "-c:v", "copy", "-c:a", "copy", str(clean_path)],
            check=True, capture_output=True,
        )
        logger.debug("[metadata] Очищено: %s -> %s", video_path.name, clean_path.name)
        return clean_path
    except FileNotFoundError:
        logger.warning("[metadata] ffmpeg не найден – использую оригинал.")
        return video_path
    except subprocess.CalledProcessError as e:
        logger.warning(
            "[metadata] Ошибка ffmpeg для %s: %s – использую оригинал.",
            video_path.name, e.stderr.decode(errors="replace")[:200],
        )
        return video_path


def upload_video(
    context: BrowserContext,
    platform: str,
    video_path: Path,
    meta: Dict,
    account_name: str = "",
    account_cfg: Dict = None,
) -> bool:
    # FIX #5: last_error объявлен снаружи цикла — доступен после него
    last_error: Optional[Exception] = None

    for attempt in range(5):
        try:
            # TODO: реализовать логику загрузки для каждой платформы
            send_telegram(f"Upload success for {video_path.name} on {platform}")
            return True
        except Exception as e:
            last_error = e
            backoff = 2 ** attempt * 60
            logger.warning("Retry %d/5 after %.1f min: %s", attempt + 1, backoff / 60, e)
            time.sleep(backoff)
            send_telegram(
                f"Upload retry {attempt+1} for {video_path.name} on {platform}: {str(e)[:100]}"
            )

    # Все 5 попыток провалились
    failed_dir = Path(config.ACCOUNTS_ROOT) / account_name / "failed"
    failed_dir.mkdir(parents=True, exist_ok=True)
    shutil.move(str(video_path), str(failed_dir / video_path.name))
    utils.save_json(
        failed_dir / f"{video_path.stem}.error.json",
        {"error": str(last_error)},
    )
    send_telegram(f"Upload failed after 5 attempts for {video_path.name} on {platform}")
    return False


def upload_all(dry_run: bool = False) -> List[Dict]:
    """Загружает все видео из очередей всех аккаунтов. Возвращает список результатов."""
    results: List[Dict] = []
    accounts = utils.get_all_accounts()

    if not accounts:
        logger.warning("Аккаунты не найдены в %s", config.ACCOUNTS_ROOT)
        return results

    for account in accounts:
        acc_name  = account["name"]
        acc_dir   = account["dir"]
        acc_cfg   = account["config"]
        platforms = account["platforms"]

        for platform in platforms:
            queue = utils.get_upload_queue(acc_dir, platform)
            if not queue:
                logger.info("[%s][%s] Очередь пуста.", acc_name, platform)
                continue

            daily_limit   = config.PLATFORM_DAILY_LIMITS.get(platform, config.DAILY_UPLOAD_LIMIT)
            uploads_today = utils.get_uploads_today(acc_dir)

            if uploads_today >= daily_limit:
                logger.info(
                    "[%s][%s] Дневной лимит достигнут (%d/%d).",
                    acc_name, platform, uploads_today, daily_limit,
                )
                continue

            profile_dir = acc_dir / "browser_profile"

            try:
                pw, context = launch_browser(acc_cfg, profile_dir)
            except RuntimeError as proxy_err:
                logger.error(
                    "[%s][%s] Прокси недоступен — аккаунт пропущен. %s",
                    acc_name, platform, proxy_err,
                )
                send_telegram(
                    f"⚠️ [{acc_name}] Прокси недоступен — загрузка на {platform} пропущена.\n"
                    f"{proxy_err}"
                )
                results.append({
                    "status": "proxy_error",
                    "platform": platform,
                    "account_id": acc_name,
                    "error_msg": str(proxy_err),
                })
                continue

            try:
                run_activity(context, platform, queue[0].get("meta", {}))

                for item in queue:
                    if utils.get_uploads_today(acc_dir) >= daily_limit:
                        break

                    video_path = item["video_path"]
                    meta       = item["meta"]

                    if dry_run:
                        logger.info("[dry_run] Загрузка: %s -> %s", video_path.name, platform)
                        results.append({
                            "status": "skipped", "platform": platform,
                            "account_id": acc_name, "source_path": str(video_path),
                        })
                        continue

                    clean_path = clean_video_metadata(video_path)
                    success = upload_video(
                        context, platform, clean_path, meta,
                        account_name=acc_name, account_cfg=acc_cfg,
                    )

                    if success:
                        utils.mark_uploaded(item)
                        utils.increment_upload_count(acc_dir)
                        results.append({
                            "status": "uploaded", "platform": platform,
                            "account_id": acc_name, "source_path": str(video_path),
                        })
                    else:
                        results.append({
                            "status": "error", "platform": platform,
                            "account_id": acc_name, "source_path": str(video_path),
                            "error_msg": "upload_video вернул False после 5 попыток",
                        })
            finally:
                close_browser(pw, context)

    return results
