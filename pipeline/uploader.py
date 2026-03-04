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
from pipeline.browser import launch_browser, close_browser, check_session_valid
from pipeline.notifications import check_and_handle_captcha, send_telegram
from pipeline.session_manager import ensure_session_fresh, mark_session_verified
from pipeline.analytics import register_upload

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Утилиты
# ─────────────────────────────────────────────────────────────────────────────

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


def _human_type(page: Page, text: str) -> None:
    """Печатает текст посимвольно с задержками, имитируя живого пользователя."""
    for char in text:
        page.keyboard.type(char)
        time.sleep(random.uniform(0.03, 0.12))


# ─────────────────────────────────────────────────────────────────────────────
# YouTube
# ─────────────────────────────────────────────────────────────────────────────

def _upload_youtube(page: Page, video_path: Path, meta: Dict) -> None:
    """Загружает видео на YouTube через YouTube Studio."""
    title       = (meta.get("title") or video_path.stem)[:100]
    description = (meta.get("description") or "")[:4900]

    logger.info("[youtube] Переход в YouTube Studio...")
    page.goto("https://studio.youtube.com", wait_until="domcontentloaded", timeout=30_000)
    time.sleep(random.uniform(2, 4))
    check_and_handle_captcha(page, "youtube")

    # Кнопка «Создать» → «Загрузить видео»
    logger.info("[youtube] Открываем диалог загрузки...")
    for sel in ["ytcp-button#create-icon", "button[aria-label*='reate']", "#create-icon"]:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=5_000):
                btn.click()
                break
        except Exception:
            continue
    time.sleep(random.uniform(1, 2))

    for sel in [
        "tp-yt-paper-item#text-item-0",
        "[test-id='upload-beta']",
        "tp-yt-paper-listbox tp-yt-paper-item:first-child",
    ]:
        try:
            item = page.locator(sel).first
            if item.is_visible(timeout=3_000):
                item.click()
                break
        except Exception:
            continue
    time.sleep(random.uniform(1, 2))

    # Загрузка файла
    logger.info("[youtube] Загружаем файл: %s", video_path.name)
    page.locator("input[type=file]").first.set_input_files(str(video_path))

    # Ждём появления поля заголовка
    logger.info("[youtube] Ожидаем открытия формы...")
    page.wait_for_selector(
        "#title-textarea, ytcp-social-suggestions-textbox#title-textarea",
        timeout=60_000,
    )
    time.sleep(random.uniform(1, 2))

    # Заголовок
    logger.info("[youtube] Заполняем заголовок...")
    for sel in [
        "#title-textarea div[contenteditable='true']",
        "ytcp-social-suggestions-textbox#title-textarea div[contenteditable]",
        "#container #title div[contenteditable]",
    ]:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=3_000):
                el.click()
                page.keyboard.press("Control+a")
                _human_type(page, title)
                break
        except Exception:
            continue
    time.sleep(random.uniform(0.5, 1.5))

    # Описание
    if description:
        for sel in [
            "#description-textarea div[contenteditable='true']",
            "#description-container div[contenteditable]",
            "ytcp-social-suggestions-textbox#description-textarea div[contenteditable]",
        ]:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=3_000):
                    el.click()
                    _human_type(page, description)
                    break
            except Exception:
                continue
        time.sleep(random.uniform(0.5, 1))

    # Не для детей
    try:
        el = page.locator("tp-yt-paper-radio-button[name='VIDEO_MADE_FOR_KIDS_NOT_MFK']").first
        if el.is_visible(timeout=3_000):
            el.click()
    except Exception:
        pass

    # «Далее» × 3
    for step in range(3):
        logger.info("[youtube] Шаг %d/3 — нажимаем «Далее»...", step + 1)
        for sel in [
            "ytcp-button#next-button",
            "button[aria-label*='ext']",
            "ytcp-stepper-navigation ytcp-button:last-child",
        ]:
            try:
                btn = page.locator(sel).first
                if btn.is_visible(timeout=5_000):
                    btn.click()
                    break
            except Exception:
                continue
        time.sleep(random.uniform(1.5, 2.5))
        check_and_handle_captcha(page, "youtube")

    # Ждём завершения загрузки файла на сервер
    logger.info("[youtube] Ожидаем завершения загрузки файла (до 10 мин)...")
    deadline = time.time() + 600
    while time.time() < deadline:
        try:
            done_el = page.locator(
                "span.ytcp-video-upload-progress:has-text('Upload complete'),"
                "span.ytcp-video-upload-progress:has-text('Processing complete'),"
                "[class*='progress-label']:has-text('100%')"
            ).first
            if done_el.is_visible(timeout=3_000):
                break
        except Exception:
            pass
        try:
            content = page.locator("ytcp-video-upload-progress").inner_text(timeout=2_000)
            if "complete" in content.lower() or "100" in content:
                break
        except Exception:
            pass
        time.sleep(5)

    # Публикация
    logger.info("[youtube] Нажимаем «Сохранить»...")
    for sel in [
        "ytcp-button#done-button",
        "ytcp-button[test-id='publish-button']",
        "button[aria-label*='ublish']",
        "button[aria-label*='ave']",
    ]:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=5_000):
                btn.click()
                break
        except Exception:
            continue

    try:
        page.wait_for_selector(
            "ytcp-video-upload-dialog[uploading='false'], "
            "[class*='success-dialog'], "
            "ytcp-uploads-still-processing-dialog",
            timeout=30_000,
        )
    except Exception:
        pass

    time.sleep(random.uniform(2, 4))
    logger.info("[youtube] Публикация завершена: %s", video_path.name)


# ─────────────────────────────────────────────────────────────────────────────
# TikTok
# ─────────────────────────────────────────────────────────────────────────────

def _upload_tiktok(page: Page, video_path: Path, meta: Dict) -> None:
    """Загружает видео на TikTok через веб-интерфейс загрузки."""
    title    = meta.get("title") or ""
    tags     = meta.get("tags", [])
    hashtags = " ".join(f"#{t.strip().replace(' ', '')}" for t in tags[:20])
    caption  = f"{title} {hashtags}".strip()[:2200]

    logger.info("[tiktok] Переходим на страницу загрузки...")
    page.goto("https://www.tiktok.com/upload", wait_until="domcontentloaded", timeout=30_000)
    time.sleep(random.uniform(2, 4))
    check_and_handle_captcha(page, "tiktok")

    # Загрузка файла
    logger.info("[tiktok] Загружаем файл: %s", video_path.name)
    for sel in ["input[type='file']", "input[name='upload-btn']", "input[accept*='video']"]:
        try:
            el = page.locator(sel).first
            el.set_input_files(str(video_path))
            break
        except Exception:
            continue

    # Ждём появления поля caption (означает, что видео обработано)
    logger.info("[tiktok] Ожидаем обработки видео (до 3 мин)...")
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            caption_el = page.locator(
                ".public-DraftEditor-content, "
                "[class*='caption'][contenteditable='true'], "
                "[data-e2e='video-desc'] div[contenteditable]"
            ).first
            if caption_el.is_visible(timeout=3_000):
                break
        except Exception:
            pass
        check_and_handle_captcha(page, "tiktok")
        time.sleep(4)
    time.sleep(random.uniform(1, 2))

    # Заполняем подпись
    if caption:
        logger.info("[tiktok] Заполняем подпись...")
        for sel in [
            ".public-DraftEditor-content",
            "[class*='caption'][contenteditable='true']",
            "[data-e2e='video-desc'] div[contenteditable]",
            "div[contenteditable='true']",
        ]:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=3_000):
                    el.click()
                    page.keyboard.press("Control+a")
                    _human_type(page, caption)
                    break
            except Exception:
                continue
        time.sleep(random.uniform(0.5, 1.5))

    # Публикация
    logger.info("[tiktok] Нажимаем «Post»...")
    for sel in [
        "button.btn-post",
        "button[data-e2e='post_video_button']",
        "button:has-text('Post')",
        "div[data-e2e='post_video_button']",
    ]:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=5_000):
                btn.click()
                break
        except Exception:
            continue

    try:
        page.wait_for_url("**/upload**", timeout=30_000)
    except Exception:
        pass

    time.sleep(random.uniform(2, 4))
    logger.info("[tiktok] Публикация завершена: %s", video_path.name)


# ─────────────────────────────────────────────────────────────────────────────
# Instagram
# ─────────────────────────────────────────────────────────────────────────────

def _upload_instagram(page: Page, video_path: Path, meta: Dict) -> None:
    """
    Загружает видео как Instagram Reels.
    Использует мобильный viewport — Instagram требует его для загрузки видео
    в браузерном режиме автоматизации.
    """
    description = meta.get("description") or meta.get("title") or ""
    tags        = meta.get("tags", [])
    hashtags    = " ".join(f"#{t.strip().replace(' ', '')}" for t in tags[:30])
    caption     = f"{description} {hashtags}".strip()[:2200]

    # Мобильный viewport для корректной работы загрузки
    page.set_viewport_size({"width": 390, "height": 844})
    page.evaluate(
        "Object.defineProperty(navigator, 'userAgent', {get: () => "
        "'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/16.0 Mobile/15E148 Safari/604.1'})"
    )

    logger.info("[instagram] Переходим на Instagram...")
    page.goto("https://www.instagram.com", wait_until="domcontentloaded", timeout=30_000)
    time.sleep(random.uniform(2, 4))
    check_and_handle_captcha(page, "instagram")

    # Кнопка «+» для создания поста
    logger.info("[instagram] Открываем форму создания поста...")
    for sel in [
        "svg[aria-label='New post']",
        "a[href='/create/select/']",
        "svg[aria-label='New Post']",
        "[data-testid='new-post-button']",
    ]:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=5_000):
                el.click()
                break
        except Exception:
            continue
    time.sleep(random.uniform(1, 2))

    # Загрузка файла
    logger.info("[instagram] Загружаем файл: %s", video_path.name)
    uploaded = False
    try:
        file_input = page.locator("input[type='file']").first
        file_input.set_input_files(str(video_path))
        uploaded = True
    except Exception:
        pass

    if not uploaded:
        for sel in ["button:has-text('Select from computer')", "button:has-text('Select From Computer')"]:
            try:
                btn = page.locator(sel).first
                if btn.is_visible(timeout=3_000):
                    with page.expect_file_chooser() as fc_info:
                        btn.click()
                    fc_info.value.set_files(str(video_path))
                    break
            except Exception:
                continue
    time.sleep(random.uniform(2, 4))

    # Выбираем Reels если появился выбор формата
    try:
        reels_btn = page.locator("button:has-text('Reels'), [aria-label='Reels']").first
        if reels_btn.is_visible(timeout=5_000):
            reels_btn.click()
            time.sleep(1)
    except Exception:
        pass

    # Нажимаем «Next» до экрана описания (до 3 раз)
    for _ in range(3):
        try:
            next_btn = page.locator(
                "button:has-text('Next'), button:has-text('Далее'), [aria-label='Next']"
            ).first
            if next_btn.is_visible(timeout=5_000):
                next_btn.click()
                time.sleep(random.uniform(1, 2))
                check_and_handle_captcha(page, "instagram")
            else:
                break
        except Exception:
            break

    # Заполняем подпись
    if caption:
        logger.info("[instagram] Заполняем подпись...")
        for sel in [
            "textarea[aria-label*='caption'], textarea[placeholder*='caption']",
            "div[contenteditable='true'][aria-label*='caption']",
            "div[role='textbox'][contenteditable='true']",
        ]:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=3_000):
                    el.click()
                    _human_type(page, caption)
                    break
            except Exception:
                continue
        time.sleep(random.uniform(0.5, 1.5))

    # Публикация
    logger.info("[instagram] Нажимаем «Share»...")
    for sel in [
        "button:has-text('Share')",
        "button:has-text('Поделиться')",
        "div[role='button']:has-text('Share')",
        "[aria-label='Share']",
    ]:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=5_000):
                btn.click()
                break
        except Exception:
            continue

    try:
        page.wait_for_selector(
            "span:has-text('Your reel has been shared'), "
            "div:has-text('Your video has been shared')",
            timeout=60_000,
        )
    except Exception:
        pass

    time.sleep(random.uniform(2, 4))
    logger.info("[instagram] Публикация завершена: %s", video_path.name)


# ─────────────────────────────────────────────────────────────────────────────
# Диспетчер
# ─────────────────────────────────────────────────────────────────────────────

_PLATFORM_UPLOADERS = {
    "youtube":   _upload_youtube,
    "tiktok":    _upload_tiktok,
    "instagram": _upload_instagram,
}


def upload_video(
    context: BrowserContext,
    platform: str,
    video_path: Path,
    meta: Dict,
    account_name: str = "",
    account_cfg: Dict = None,
) -> bool:
    """
    Загружает видео на платформу с повторными попытками (до 5).
    Возвращает True при успехе, False после 5 неудач.
    """
    uploader_fn = _PLATFORM_UPLOADERS.get(platform)
    if uploader_fn is None:
        logger.error("[%s] Неизвестная платформа — загрузка невозможна.", platform)
        return False

    last_error: Optional[Exception] = None

    for attempt in range(5):
        page = context.new_page()
        try:
            uploader_fn(page, video_path, meta)
            send_telegram(f"✅ Загружено <b>{video_path.name}</b> на <b>{platform}</b>")
            return True
        except Exception as e:
            last_error = e
            backoff = 2 ** attempt * 60
            logger.warning(
                "[%s] Попытка %d/5 неудачна: %s — следующая через %.1f мин",
                platform, attempt + 1, e, backoff / 60,
            )
            time.sleep(backoff)
            send_telegram(
                f"⚠️ Повтор {attempt+1}/5 для <b>{video_path.name}</b> "
                f"на <b>{platform}</b>: {str(e)[:100]}"
            )
        finally:
            try:
                page.close()
            except Exception:
                pass

    # Все 5 попыток провалились
    failed_dir = Path(config.ACCOUNTS_ROOT) / account_name / "failed"
    failed_dir.mkdir(parents=True, exist_ok=True)
    shutil.move(str(video_path), str(failed_dir / video_path.name))
    utils.save_json(
        failed_dir / f"{video_path.stem}.error.json",
        {"error": str(last_error)},
    )
    send_telegram(
        f"❌ Не удалось загрузить <b>{video_path.name}</b> на <b>{platform}</b> "
        f"после 5 попыток. Видео перемещено в <code>failed/</code>."
    )
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Основная функция
# ─────────────────────────────────────────────────────────────────────────────

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
                    "status": "proxy_error", "platform": platform,
                    "account_id": acc_name, "error_msg": str(proxy_err),
                })
                continue

            try:
                # Проверяем и при необходимости обновляем сессию через session_manager
                session_ok = ensure_session_fresh(context, acc_name, platform)
                if session_ok:
                    mark_session_verified(acc_name, platform, valid=True)
                else:
                    results.append({
                        "status": "not_logged_in", "platform": platform,
                        "account_id": acc_name,
                        "error_msg": "Сессия недействительна, ручной вход не завершён",
                    })
                    continue

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
                        # Регистрируем загрузку в analytics.json для отложенного сбора статистики
                        # video_url будет уточнён при первом сборе (пока неизвестен)
                        register_upload(
                            video_stem=Path(video_path).stem,
                            platform=platform,
                            video_url="",
                            meta=meta,
                        )
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
