"""
pipeline/uploader.py – Загрузка видео на YouTube, TikTok, Instagram Reels.

Изменения:
  - Фикс #1: каждый _upload_* возвращает str (URL видео после публикации)
  - Фикс #7: backoff capped at 5 мин (было до 16 мин на попытку)
  - Фича B:  A/B тестирование — метаданные берутся из meta["ab_variant"]
  - Фича D:  карантин — интеграция quarantine.mark_error / mark_success
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
from pipeline.quarantine import is_quarantined, mark_error as q_mark_error, mark_success as q_mark_success

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Утилиты
# ─────────────────────────────────────────────────────────────────────────────

def clean_video_metadata(video_path: Path) -> Path:
    """Очищает метаданные видео через ffmpeg."""
    clean_path = video_path.with_stem(video_path.stem + "_clean")
    try:
        subprocess.run(
            ["ffmpeg", "-y", "-i", str(video_path),
             "-map_metadata", "-1", "-c:v", "copy", "-c:a", "copy", str(clean_path)],
            check=True, capture_output=True,
        )
        return clean_path
    except FileNotFoundError:
        return video_path
    except subprocess.CalledProcessError as e:
        logger.warning("[metadata] ffmpeg ошибка: %s", e.stderr.decode(errors="replace")[:200])
        return video_path


def _human_type(page: Page, text: str) -> None:
    """Посимвольный ввод с задержками."""
    for char in text:
        page.keyboard.type(char)
        time.sleep(random.uniform(0.03, 0.12))


def _try_get_url(page: Page, selectors: list[str], base: str = "") -> str:
    """Пытается извлечь href из первого найденного селектора."""
    for sel in selectors:
        try:
            el   = page.locator(sel).first
            href = el.get_attribute("href", timeout=4_000)
            if href:
                return href if href.startswith("http") else base + href
        except Exception:
            continue
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# YouTube
# ─────────────────────────────────────────────────────────────────────────────

def _upload_youtube(page: Page, video_path: Path, meta: Dict) -> str:
    """Загружает видео на YouTube. Возвращает публичный URL видео."""
    title       = (meta.get("title") or video_path.stem)[:100]
    description = (meta.get("description") or "")[:4900]

    page.goto("https://studio.youtube.com", wait_until="domcontentloaded", timeout=30_000)
    time.sleep(random.uniform(2, 4))
    check_and_handle_captcha(page, "youtube")

    for sel in ["ytcp-button#create-icon", "button[aria-label*='reate']", "#create-icon"]:
        try:
            if page.locator(sel).first.is_visible(timeout=5_000):
                page.locator(sel).first.click(); break
        except Exception:
            continue
    time.sleep(random.uniform(1, 2))

    for sel in ["tp-yt-paper-item#text-item-0", "[test-id='upload-beta']",
                "tp-yt-paper-listbox tp-yt-paper-item:first-child"]:
        try:
            if page.locator(sel).first.is_visible(timeout=3_000):
                page.locator(sel).first.click(); break
        except Exception:
            continue
    time.sleep(random.uniform(1, 2))

    page.locator("input[type=file]").first.set_input_files(str(video_path))
    page.wait_for_selector(
        "#title-textarea, ytcp-social-suggestions-textbox#title-textarea", timeout=60_000
    )
    time.sleep(random.uniform(1, 2))

    for sel in ["#title-textarea div[contenteditable='true']",
                "ytcp-social-suggestions-textbox#title-textarea div[contenteditable]"]:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=3_000):
                el.click(); page.keyboard.press("Control+a"); _human_type(page, title); break
        except Exception:
            continue
    time.sleep(random.uniform(0.5, 1.5))

    if description:
        for sel in ["#description-textarea div[contenteditable='true']",
                    "#description-container div[contenteditable]"]:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=3_000):
                    el.click(); _human_type(page, description); break
            except Exception:
                continue
        time.sleep(0.5)

    try:
        el = page.locator("tp-yt-paper-radio-button[name='VIDEO_MADE_FOR_KIDS_NOT_MFK']").first
        if el.is_visible(timeout=3_000): el.click()
    except Exception:
        pass

    for step in range(3):
        for sel in ["ytcp-button#next-button", "button[aria-label*='ext']"]:
            try:
                if page.locator(sel).first.is_visible(timeout=5_000):
                    page.locator(sel).first.click(); break
            except Exception:
                continue
        time.sleep(random.uniform(1.5, 2.5))
        check_and_handle_captcha(page, "youtube")

    # Ждём завершения загрузки файла
    deadline = time.time() + 600
    while time.time() < deadline:
        try:
            done = page.locator(
                "span.ytcp-video-upload-progress:has-text('Upload complete'),"
                "span.ytcp-video-upload-progress:has-text('Processing complete')"
            ).first
            if done.is_visible(timeout=3_000): break
        except Exception:
            pass
        time.sleep(5)

    # Публикация
    for sel in ["ytcp-button#done-button", "ytcp-button[test-id='publish-button']",
                "button[aria-label*='ublish']", "button[aria-label*='ave']"]:
        try:
            if page.locator(sel).first.is_visible(timeout=5_000):
                page.locator(sel).first.click(); break
        except Exception:
            continue

    # Ждём диалога успеха и захватываем URL видео
    video_url = ""
    try:
        page.wait_for_selector(
            "ytcp-video-upload-dialog[uploading='false'], [class*='success-dialog'], "
            "ytcp-uploads-still-processing-dialog",
            timeout=30_000,
        )
        # Пробуем найти ссылку на видео в диалоге
        video_url = _try_get_url(
            page,
            ["a[href*='/shorts/']", "a[href*='watch?v=']", "ytcp-video-upload-dialog a"],
            "https://www.youtube.com",
        )
    except Exception:
        pass

    # Fallback: берём первое видео из Studio
    if not video_url:
        try:
            page.goto("https://studio.youtube.com/videos/shorts", timeout=15_000,
                      wait_until="domcontentloaded")
            time.sleep(2)
            video_url = _try_get_url(
                page,
                ["ytd-grid-video-renderer a#video-title", "a[href*='/shorts/']"],
                "https://www.youtube.com",
            )
        except Exception:
            pass

    time.sleep(random.uniform(2, 3))
    logger.info("[youtube] Опубликовано: %s | URL: %s", video_path.name, video_url or "неизвестен")
    return video_url


# ─────────────────────────────────────────────────────────────────────────────
# TikTok
# ─────────────────────────────────────────────────────────────────────────────

def _upload_tiktok(page: Page, video_path: Path, meta: Dict) -> str:
    """Загружает видео на TikTok. Возвращает публичный URL видео."""
    title    = meta.get("title") or ""
    tags     = meta.get("tags", [])
    hashtags = " ".join(f"#{t.strip().replace(' ', '')}" for t in tags[:20])
    caption  = f"{title} {hashtags}".strip()[:2200]

    page.goto("https://www.tiktok.com/upload", wait_until="domcontentloaded", timeout=30_000)
    time.sleep(random.uniform(2, 4))
    check_and_handle_captcha(page, "tiktok")

    for sel in ["input[type='file']", "input[accept*='video']"]:
        try:
            page.locator(sel).first.set_input_files(str(video_path)); break
        except Exception:
            continue

    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            if page.locator(
                ".public-DraftEditor-content, [data-e2e='video-desc'] div[contenteditable]"
            ).first.is_visible(timeout=3_000):
                break
        except Exception:
            pass
        check_and_handle_captcha(page, "tiktok")
        time.sleep(4)
    time.sleep(random.uniform(1, 2))

    if caption:
        for sel in [".public-DraftEditor-content",
                    "[data-e2e='video-desc'] div[contenteditable]",
                    "div[contenteditable='true']"]:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=3_000):
                    el.click(); page.keyboard.press("Control+a"); _human_type(page, caption); break
            except Exception:
                continue
        time.sleep(random.uniform(0.5, 1.5))

    for sel in ["button.btn-post", "button[data-e2e='post_video_button']",
                "button:has-text('Post')"]:
        try:
            if page.locator(sel).first.is_visible(timeout=5_000):
                page.locator(sel).first.click(); break
        except Exception:
            continue

    # Ждём редиректа на страницу видео после публикации
    video_url = ""
    try:
        page.wait_for_url(r"*tiktok.com/@*\/video\/*", timeout=20_000)
        video_url = page.url
    except Exception:
        pass

    # Fallback: идём на профиль и берём первое видео
    if not video_url:
        try:
            page.goto("https://www.tiktok.com/upload", wait_until="domcontentloaded", timeout=10_000)
            # Берём URL из хлебных крошек или success-popup
            video_url = _try_get_url(
                page,
                ["a[href*='/video/']", "[class*='success'] a"],
                "https://www.tiktok.com",
            )
        except Exception:
            pass

    time.sleep(random.uniform(2, 3))
    logger.info("[tiktok] Опубликовано: %s | URL: %s", video_path.name, video_url or "неизвестен")
    return video_url


# ─────────────────────────────────────────────────────────────────────────────
# Instagram
# ─────────────────────────────────────────────────────────────────────────────

def _upload_instagram(page: Page, video_path: Path, meta: Dict) -> str:
    """Загружает Reel на Instagram. Возвращает публичный URL поста."""
    description = meta.get("description") or meta.get("title") or ""
    tags        = meta.get("tags", [])
    hashtags    = " ".join(f"#{t.strip().replace(' ', '')}" for t in tags[:30])
    caption     = f"{description} {hashtags}".strip()[:2200]

    page.set_viewport_size({"width": 390, "height": 844})
    page.evaluate(
        "Object.defineProperty(navigator, 'userAgent', {get: () => "
        "'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1'})"
    )

    page.goto("https://www.instagram.com", wait_until="domcontentloaded", timeout=30_000)
    time.sleep(random.uniform(2, 4))
    check_and_handle_captcha(page, "instagram")

    for sel in ["svg[aria-label='New post']", "a[href='/create/select/']", "svg[aria-label='New Post']"]:
        try:
            if page.locator(sel).first.is_visible(timeout=5_000):
                page.locator(sel).first.click(); break
        except Exception:
            continue
    time.sleep(random.uniform(1, 2))

    try:
        page.locator("input[type='file']").first.set_input_files(str(video_path))
    except Exception:
        for sel in ["button:has-text('Select from computer')"]:
            try:
                with page.expect_file_chooser() as fc:
                    page.locator(sel).first.click()
                fc.value.set_files(str(video_path))
            except Exception:
                pass
    time.sleep(random.uniform(2, 4))

    try:
        if page.locator("button:has-text('Reels')").first.is_visible(timeout=5_000):
            page.locator("button:has-text('Reels')").first.click()
            time.sleep(1)
    except Exception:
        pass

    for _ in range(3):
        try:
            btn = page.locator("button:has-text('Next'), [aria-label='Next']").first
            if btn.is_visible(timeout=5_000):
                btn.click(); time.sleep(random.uniform(1, 2))
                check_and_handle_captcha(page, "instagram")
            else:
                break
        except Exception:
            break

    if caption:
        for sel in ["textarea[aria-label*='caption']", "div[contenteditable='true'][aria-label*='caption']",
                    "div[role='textbox'][contenteditable='true']"]:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=3_000):
                    el.click(); _human_type(page, caption); break
            except Exception:
                continue
        time.sleep(random.uniform(0.5, 1.5))

    for sel in ["button:has-text('Share')", "button:has-text('Поделиться')", "[aria-label='Share']"]:
        try:
            if page.locator(sel).first.is_visible(timeout=5_000):
                page.locator(sel).first.click(); break
        except Exception:
            continue

    # Захватываем URL нового поста
    video_url = ""
    try:
        page.wait_for_selector(
            "span:has-text('Your reel has been shared'), div:has-text('Your video has been shared')",
            timeout=60_000,
        )
        video_url = _try_get_url(
            page,
            ["a[href*='/reel/']", "a[href*='/p/']", "button:has-text('View') ~ a"],
            "https://www.instagram.com",
        )
    except Exception:
        pass

    # Fallback: профиль → первый пост
    if not video_url:
        try:
            page.goto("https://www.instagram.com/", wait_until="domcontentloaded", timeout=10_000)
            time.sleep(2)
            video_url = _try_get_url(
                page, ["a[href*='/reel/']", "article a"], "https://www.instagram.com"
            )
        except Exception:
            pass

    time.sleep(random.uniform(2, 3))
    logger.info("[instagram] Опубликовано: %s | URL: %s", video_path.name, video_url or "неизвестен")
    return video_url


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
) -> Optional[str]:
    """
    Загружает видео на платформу с повторными попытками (до 5).
    Возвращает URL опубликованного видео при успехе, None при неудаче.
    Фикс #7: backoff ограничен 5 мин (min(2^n * 60, 300)).
    """
    uploader_fn = _PLATFORM_UPLOADERS.get(platform)
    if uploader_fn is None:
        logger.error("[%s] Неизвестная платформа.", platform)
        return None

    last_error: Optional[Exception] = None

    for attempt in range(5):
        page = context.new_page()
        try:
            video_url = uploader_fn(page, video_path, meta)
            send_telegram(f"✅ Загружено <b>{video_path.name}</b> на <b>{platform}</b>")
            return video_url or ""   # пустая строка = URL неизвестен, но успех
        except Exception as e:
            last_error = e
            backoff = min(2 ** attempt * 60, 300)   # cap: 5 мин
            logger.warning("[%s] Попытка %d/5: %s — пауза %.0f сек", platform, attempt + 1, e, backoff)
            time.sleep(backoff)
            send_telegram(
                f"⚠️ Повтор {attempt+1}/5 <b>{video_path.name}</b> / <b>{platform}</b>: {str(e)[:100]}"
            )
        finally:
            try:
                page.close()
            except Exception:
                pass

    failed_dir = Path(config.ACCOUNTS_ROOT) / account_name / "failed"
    failed_dir.mkdir(parents=True, exist_ok=True)
    shutil.move(str(video_path), str(failed_dir / video_path.name))
    utils.save_json(failed_dir / f"{video_path.stem}.error.json", {"error": str(last_error)})
    send_telegram(
        f"❌ Не удалось загрузить <b>{video_path.name}</b> на <b>{platform}</b> после 5 попыток."
    )
    return None


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

            # Фикс D: пропускаем аккаунт если он в карантине
            if is_quarantined(acc_name, platform):
                results.append({
                    "status": "quarantined", "platform": platform, "account_id": acc_name,
                    "error_msg": "Аккаунт в карантине",
                })
                continue

            queue = utils.get_upload_queue(acc_dir, platform)
            if not queue:
                logger.info("[%s][%s] Очередь пуста.", acc_name, platform)
                continue

            daily_limit   = config.PLATFORM_DAILY_LIMITS.get(platform, config.DAILY_UPLOAD_LIMIT)
            uploads_today = utils.get_uploads_today(acc_dir)
            if uploads_today >= daily_limit:
                logger.info("[%s][%s] Дневной лимит (%d/%d).", acc_name, platform, uploads_today, daily_limit)
                continue

            profile_dir = acc_dir / "browser_profile"
            try:
                pw, context = launch_browser(acc_cfg, profile_dir)
            except RuntimeError as proxy_err:
                logger.error("[%s][%s] Прокси недоступен: %s", acc_name, platform, proxy_err)
                send_telegram(f"⚠️ [{acc_name}] Прокси недоступен — {platform} пропущен.\n{proxy_err}")
                q_mark_error(acc_name, platform, reason="proxy_unavailable")
                results.append({
                    "status": "proxy_error", "platform": platform,
                    "account_id": acc_name, "error_msg": str(proxy_err),
                })
                continue

            try:
                # Фикс #6: continue (не return) при невалидной сессии
                if not ensure_session_fresh(context, acc_name, platform):
                    results.append({
                        "status": "not_logged_in", "platform": platform,
                        "account_id": acc_name, "error_msg": "Сессия недействительна",
                    })
                    continue
                mark_session_verified(acc_name, platform, valid=True)

                run_activity(context, platform, queue[0].get("meta", {}))

                for item in queue:
                    if utils.get_uploads_today(acc_dir) >= daily_limit:
                        break

                    video_path = item["video_path"]
                    # Фича B: A/B — берём назначенный вариант метаданных
                    meta = item.get("ab_meta") or item["meta"]

                    if dry_run:
                        logger.info("[dry_run] %s -> %s", video_path.name, platform)
                        results.append({
                            "status": "skipped", "platform": platform,
                            "account_id": acc_name, "source_path": str(video_path),
                        })
                        continue

                    clean_path = clean_video_metadata(video_path)
                    video_url  = upload_video(
                        context, platform, clean_path, meta,
                        account_name=acc_name, account_cfg=acc_cfg,
                    )

                    if video_url is not None:
                        utils.mark_uploaded(item)
                        utils.increment_upload_count(acc_dir)
                        q_mark_success(acc_name, platform)
                        # Фикс #1: передаём реальный URL в аналитику
                        register_upload(
                            video_stem=Path(video_path).stem,
                            platform=platform,
                            video_url=video_url,
                            meta=meta,
                            ab_variant=meta.get("ab_variant"),
                        )
                        results.append({
                            "status": "uploaded", "platform": platform,
                            "account_id": acc_name, "source_path": str(video_path),
                            "video_url": video_url,
                        })
                    else:
                        q_mark_error(acc_name, platform, reason="upload_failed")
                        results.append({
                            "status": "error", "platform": platform,
                            "account_id": acc_name, "source_path": str(video_path),
                            "error_msg": "upload_video вернул None после 5 попыток",
                        })
            finally:
                close_browser(pw, context)

    return results
