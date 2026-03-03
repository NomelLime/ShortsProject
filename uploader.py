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
from pipeline.notifications import check_and_handle_captcha

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
# YouTube
# ----------------------------------------------------------------------

def _get_youtube_upload_url(account_cfg: dict) -> str:
    channel_id = account_cfg.get("channel_id", "").strip()
    if channel_id:
        return f"https://studio.youtube.com/channel/{channel_id}/videos/upload"
    return config.PLATFORM_URLS["youtube"]["upload"]


def _upload_youtube(
    page: Page,
    video_path: Path,
    meta: dict,
    account_name: str,
    account_cfg: dict,
) -> bool:
    upload_url = _get_youtube_upload_url(account_cfg)
    logger.info("[YouTube] Переход: %s", upload_url)
    page.goto(upload_url, wait_until="domcontentloaded", timeout=30_000)
    utils.human_sleep(2, 4)
    check_and_handle_captcha(page, "youtube", account_name)

    try:
        page.locator("ytcp-button#select-files-button").click(timeout=10_000)
        utils.human_sleep(1, 2)
    except Exception:
        logger.warning("[YouTube] Кнопка выбора файла не найдена – пробую через input")

    file_input = page.locator("input[type='file']").first
    file_input.set_input_files(str(video_path))
    logger.info("[YouTube] Файл передан: %s", video_path.name)

    try:
        page.wait_for_selector("ytcp-video-upload-progress", timeout=15_000)
        logger.debug("[YouTube] Прогресс-бар появился.")
    except Exception:
        logger.warning("[YouTube] Прогресс-бар не появился, продолжаю.")

    utils.human_sleep(2, 4)

    title_sel  = "ytcp-social-suggestions-textbox#title-textarea div[contenteditable='true']"
    title_text = meta.get("title", "Без названия")
    page.locator(title_sel).first.click()
    page.keyboard.press("Control+A")
    utils.type_humanlike(page, title_sel, title_text, clear_first=False)

    desc_sel = "ytcp-social-suggestions-textbox#description-textarea div[contenteditable='true']"
    utils.type_humanlike(page, desc_sel, meta.get("description", ""))

    try:
        page.locator("ytcp-button#toggle-button").click(timeout=5_000)
        utils.human_sleep(1, 2)
        tags_sel = "input.ytcp-chip-bar"
        for tag in meta.get("tags", [])[:30]:
            page.locator(tags_sel).first.type(tag)
            page.keyboard.press("Enter")
            time.sleep(random.uniform(0.3, 0.8))
    except Exception as e:
        logger.warning("[YouTube] Теги не добавлены: %s", e)

    utils.human_sleep(2, 4)

    for step in range(3):
        try:
            page.locator("ytcp-button#next-button").click(timeout=8_000)
            utils.human_sleep(1.5, 3)
            check_and_handle_captcha(page, "youtube", account_name)
        except Exception:
            logger.warning("[YouTube] Кнопка Next не найдена на шаге %d", step)

    try:
        page.locator("ytcp-button#done-button").click(timeout=8_000)
    except Exception:
        page.locator("ytcp-button#publish-button").click(timeout=8_000)
    logger.info("[YouTube] Нажата кнопка Publish/Done")

    return _wait_upload_complete_youtube(page)


def _wait_upload_complete_youtube(page: Page) -> bool:
    try:
        page.wait_for_selector(
            "ytcp-video-upload-progress[upload-state='complete']",
            timeout=config.UPLOAD_TIMEOUT_MS,
        )
        logger.info("[YouTube] ✅ Загрузка завершена!")
        return True
    except Exception:
        try:
            page.wait_for_selector(
                "ytcp-uploads-still-processing-dialog, ytcp-video-upload-complete-dialog",
                timeout=config.UPLOAD_TIMEOUT_MS,
            )
            logger.info("[YouTube] ✅ Загрузка завершена (диалог)!")
            return True
        except Exception as e:
            logger.error("[YouTube] Таймаут загрузки: %s", e)
            return False


# ----------------------------------------------------------------------
# TikTok
# ----------------------------------------------------------------------

def _upload_tiktok(
    page: Page,
    video_path: Path,
    meta: dict,
    account_name: str,
    account_cfg: dict,
) -> bool:
    logger.info("[TikTok] Переход на страницу загрузки…")
    page.goto(config.PLATFORM_URLS["tiktok"]["upload"], wait_until="domcontentloaded", timeout=30_000)
    utils.human_sleep(3, 5)
    check_and_handle_captcha(page, "tiktok", account_name)

    file_input = page.locator("input[type='file']").first
    file_input.set_input_files(str(video_path))
    logger.info("[TikTok] Файл передан: %s", video_path.name)

    utils.human_sleep(5, 10)
    check_and_handle_captcha(page, "tiktok", account_name)

    title    = meta.get("title", "")
    tags_str = " ".join(f"#{t.replace(' ', '')}" for t in meta.get("tags", [])[:5])
    caption  = f"{title} {tags_str}".strip()
    try:
        cap_sel = "div[contenteditable='true'][data-placeholder]"
        page.locator(cap_sel).first.click()
        page.keyboard.press("Control+A")
        for char in caption:
            page.keyboard.type(char)
            time.sleep(random.uniform(0.04, 0.14))
        utils.human_sleep(1, 2)
    except Exception as e:
        logger.warning("[TikTok] Не удалось ввести caption: %s", e)

    try:
        page.locator("button[data-e2e='post_video_button']").click(timeout=10_000)
    except Exception:
        page.get_by_text("Post").last.click()
    logger.info("[TikTok] Нажата кнопка Post")

    return _wait_upload_complete_tiktok(page)


def _wait_upload_complete_tiktok(page: Page) -> bool:
    try:
        page.wait_for_url("**/profile*", timeout=config.UPLOAD_TIMEOUT_MS)
        logger.info("[TikTok] ✅ Загрузка завершена (редирект на профиль)!")
        return True
    except Exception:
        try:
            page.wait_for_selector(
                "[data-e2e='upload-success'], .upload-success-text",
                timeout=config.UPLOAD_TIMEOUT_MS,
            )
            logger.info("[TikTok] ✅ Загрузка завершена!")
            return True
        except Exception as e:
            logger.error("[TikTok] Таймаут загрузки: %s", e)
            return False


# ----------------------------------------------------------------------
# Instagram Reels
# ----------------------------------------------------------------------

def _upload_instagram(
    page: Page,
    video_path: Path,
    meta: dict,
    account_name: str,
    account_cfg: dict,
) -> bool:
    logger.info("[Instagram] Переход на главную страницу…")
    page.goto(config.PLATFORM_URLS["instagram"]["home"], wait_until="domcontentloaded", timeout=30_000)
    utils.human_sleep(2, 4)
    check_and_handle_captcha(page, "instagram", account_name)

    try:
        create_btn = page.locator(
            "svg[aria-label='New post'], a[href='/create/style/']"
        ).first
        create_btn.click(timeout=8_000)
        utils.human_sleep(1, 2)
    except Exception as e:
        logger.warning("[Instagram] Кнопка создания поста не найдена: %s", e)
        return False

    try:
        page.get_by_text("Reel").click(timeout=5_000)
        utils.human_sleep(1, 2)
    except Exception:
        logger.warning("[Instagram] Вкладка Reel не найдена, продолжаю…")

    file_input = page.locator("input[type='file']").first
    file_input.set_input_files(str(video_path))
    logger.info("[Instagram] Файл передан: %s", video_path.name)
    utils.human_sleep(4, 8)
    check_and_handle_captcha(page, "instagram", account_name)

    for step in range(2):
        try:
            page.get_by_text("Next").click(timeout=8_000)
            utils.human_sleep(1.5, 3)
        except Exception:
            logger.warning("[Instagram] Кнопка Next не найдена на шаге %d", step)

    title     = meta.get("title", "")
    tags_str  = " ".join(f"#{t.replace(' ', '')}" for t in meta.get("tags", [])[:10])
    full_text = f"{title}\n\n{meta.get('description', '')}\n\n{tags_str}".strip()
    try:
        cap_sel = (
            "textarea[aria-label='Write a caption...'], "
            "div[aria-label='Write a caption...']"
        )
        utils.type_humanlike(page, cap_sel, full_text)
    except Exception as e:
        logger.warning("[Instagram] Caption не введён: %s", e)

    utils.human_sleep(1, 2)

    try:
        page.get_by_text("Share").click(timeout=8_000)
        logger.info("[Instagram] Нажата кнопка Share")
    except Exception as e:
        logger.error("[Instagram] Кнопка Share не найдена: %s", e)
        return False

    return _wait_upload_complete_instagram(page)


def _wait_upload_complete_instagram(page: Page) -> bool:
    try:
        page.wait_for_selector(
            "[aria-label='Reel shared.'], div:has-text('Your reel has been shared')",
            timeout=config.UPLOAD_TIMEOUT_MS,
        )
        logger.info("[Instagram] ✅ Загрузка завершена!")
        return True
    except Exception as e:
        logger.error("[Instagram] Таймаут загрузки: %s", e)
        return False


# ----------------------------------------------------------------------
# Диспетчер загрузки для одного видео
# ----------------------------------------------------------------------

_UPLOADERS = {
    "youtube":   _upload_youtube,
    "tiktok":    _upload_tiktok,
    "instagram": _upload_instagram,
}


def upload_video(
    context:      BrowserContext,
    platform:     str,
    video_path:   Path,
    meta:         dict,
    account_name: str = "",
    account_cfg:  Optional[dict] = None,
) -> bool:
    """
    Запускает процесс загрузки одного видео на указанную платформу.
    Возвращает True при успехе. Гарантирует удаление временного _clean-файла.
    """
    uploader_fn = _UPLOADERS.get(platform)
    if not uploader_fn:
        logger.error("Неизвестная платформа: %s", platform)
        return False

    account_cfg = account_cfg or {}
    actual_path = clean_video_metadata(video_path)
    # Флаг: был ли создан временный файл (отличается от оригинала)
    created_clean = actual_path != video_path

    page = context.new_page()
    # Stealth применяется автоматически через context.on("page", ...) в browser.py

    try:
        return uploader_fn(page, actual_path, meta, account_name, account_cfg)
    except Exception as e:
        logger.error("[%s] Критическая ошибка загрузки: %s", platform, e, exc_info=True)
        return False
    finally:
        utils.human_sleep(2, 4)
        page.close()
        # Удаляем временный _clean-файл после загрузки
        if created_clean and actual_path.exists():
            try:
                actual_path.unlink()
                logger.debug("[metadata] Удалён временный файл: %s", actual_path.name)
            except OSError as e:
                logger.warning("[metadata] Не удалось удалить %s: %s", actual_path.name, e)


# ----------------------------------------------------------------------
# Главная функция: загрузить все видео из очередей всех аккаунтов
# ----------------------------------------------------------------------

def upload_all(dry_run: bool = False) -> List[Dict]:
    """
    Проходит по всем аккаунтам и загружает видео из их очередей.

    Возвращает список результатов:
        [{ "source_path", "account_id", "platform", "status", "error_msg" }, ...]
    """
    if dry_run:
        logger.info("🔷 DRY RUN: реальная загрузка не производится.")

    accounts = utils.get_all_accounts()
    if not accounts:
        logger.error("Нет аккаунтов для загрузки.")
        return []

    results: List[Dict] = []

    for account in accounts:
        name      = account["name"]
        acc_cfg   = account["config"]
        platform  = acc_cfg.get("platform", "youtube").lower()
        profile_dir = account["profile_dir"]
        queue_dir   = account["upload_queue_dir"]
        acc_dir     = profile_dir.parent

        logger.info("\n%s\nАккаунт: %s  |  Платформа: %s\n%s",
                    "=" * 60, name, platform.upper(), "=" * 60)

        if utils.is_daily_limit_reached(acc_dir):
            logger.warning("[%s] Дневной лимит достигнут, пропускаю.", name)
            continue

        queue = utils.get_upload_queue(queue_dir)
        if not queue:
            logger.info("[%s] Очередь пуста.", name)
            continue

        uploads_today    = utils.get_uploads_today(acc_dir)
        remaining_slots  = config.DAILY_UPLOAD_LIMIT - uploads_today
        videos_to_upload = queue[:remaining_slots]

        logger.info(
            "[%s] В очереди: %d | Загружено сегодня: %d | Слотов: %d | Будет загружено: %d",
            name, len(queue), uploads_today, remaining_slots, len(videos_to_upload),
        )

        if dry_run:
            logger.info("[%s] dry-run: пропускаю реальную загрузку.", name)
            for item in videos_to_upload:
                results.append({
                    "source_path": str(item["video_path"]),
                    "account_id":  name,
                    "platform":    platform,
                    "status":      "skipped",
                    "error_msg":   "dry-run",
                })
            continue

        # Реальная загрузка
        pw = context = None
        try:
            pw, context = launch_browser(acc_cfg, profile_dir)

            # Симуляция активности перед загрузкой
            if videos_to_upload:
                run_activity(context, platform, videos_to_upload[0]["meta"])

            for item in videos_to_upload:
                if utils.is_daily_limit_reached(acc_dir):
                    logger.info("[%s] Лимит достигнут, прекращаю загрузки.", name)
                    break

                video_path = item["video_path"]
                meta       = item["meta"]
                logger.info("[%s] Загружаю: %s", name, video_path.name)

                success = upload_video(
                    context, platform, video_path, meta,
                    account_name=name, account_cfg=acc_cfg,
                )

                results.append({
                    "source_path": str(video_path),
                    "account_id":  name,
                    "platform":    platform,
                    "status":      "uploaded" if success else "error",
                    "error_msg":   "" if success else "upload_video returned False",
                })

                if success:
                    utils.mark_uploaded(item)
                    utils.increment_upload_count(acc_dir)
                    logger.info("[%s] ✅ %s — успешно.", name, video_path.name)
                else:
                    logger.error("[%s] ❌ Ошибка загрузки %s.", name, video_path.name)

        except TimeoutError as e:
            logger.error("[%s] Пропускаю аккаунт из-за таймаута: %s", name, e)
            for item in videos_to_upload:
                results.append({
                    "source_path": str(item["video_path"]),
                    "account_id":  name,
                    "platform":    platform,
                    "status":      "error",
                    "error_msg":   f"TimeoutError: {e}",
                })
        except Exception as e:
            logger.critical("[%s] Необработанная ошибка: %s", name, e, exc_info=True)
            for item in videos_to_upload:
                results.append({
                    "source_path": str(item["video_path"]),
                    "account_id":  name,
                    "platform":    platform,
                    "status":      "error",
                    "error_msg":   f"Exception: {e}",
                })
        finally:
            if pw and context:
                close_browser(pw, context)

    logger.info("Загрузка завершена. Всего записей в results: %d", len(results))
    return results
