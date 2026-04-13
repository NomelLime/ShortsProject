"""
pipeline/uploader.py – Загрузка видео на YouTube, TikTok, Instagram Reels.

Изменения:
  - Фикс #1: каждый _upload_* возвращает str (URL видео после публикации)
  - Фикс #7: backoff capped at 5 мин (было до 16 мин на попытку)
  - Фича B:  A/B тестирование — метаданные берутся из meta["ab_variant"]
  - Фича D:  карантин — интеграция quarantine.mark_error / mark_success
"""

import contextvars
import logging
import random

import requests
import shutil
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterator, List, Optional

from rebrowser_playwright.sync_api import BrowserContext, Page

from pipeline import config, utils
from pipeline.humanize import HumanizeRisk, human_pause
from pipeline.activity import run_activity
from pipeline.browser import launch_browser, close_browser, check_session_valid
from pipeline.notifications import check_and_handle_captcha, send_telegram
from pipeline.session_manager import ensure_session_fresh, mark_session_verified
from pipeline.analytics import register_upload
from pipeline.agents.hook_lab import HookLabAgent
from pipeline.agents.risk_guard import RiskGuardAgent
from pipeline.agent_memory import get_memory
from pipeline.quarantine import is_quarantined, mark_error as q_mark_error, mark_success as q_mark_success
from pipeline.upload_warmup import is_upload_blocked, is_upload_warmup_active
from pipeline.locale_packaging import prepare_locale_pack_for_upload
from pipeline.publish_bridge import (
    MANUAL_REQUIRED_SENTINEL,
    STATUS_MANUAL_REQUIRED,
    bridge_enabled_for_platform,
    get_publish_handler_mode,
    queue_manual_publish,
)

logger = logging.getLogger(__name__)
_memory = get_memory()

_uploader_h_cfg: contextvars.ContextVar[Optional[Dict]] = contextvars.ContextVar(
    "uploader_humanize_cfg",
    default=None,
)


@contextmanager
def _uploader_humanize_scope(account_cfg: Optional[Dict]) -> Iterator[None]:
    tok = _uploader_h_cfg.set(account_cfg)
    try:
        yield
    finally:
        _uploader_h_cfg.reset(tok)


def _up_pause(
    lo: float,
    hi: float,
    *,
    ctx: str = "",
    risk: HumanizeRisk = HumanizeRisk.MEDIUM,
) -> None:
    human_pause(
        lo,
        hi,
        account_cfg=_uploader_h_cfg.get(),
        agent="UPLOADER",
        context=ctx,
        risk=risk,
    )


def _evaluate_prepublish_gate(meta: Dict) -> Dict[str, object]:
    hook_score = float(meta.get("hook_score") or HookLabAgent.score_hook(str(meta.get("hook_text") or "")))
    risk_score, blocked, reason = RiskGuardAgent.score_metadata_risk(meta)
    retention_prediction = round(min(0.95, max(0.05, 0.55 * hook_score + 0.45 * (1.0 - risk_score))), 4)
    return {
        "hook_score": round(hook_score, 4),
        "risk_score": float(risk_score),
        "retention_prediction": retention_prediction,
        "blocked": bool(blocked),
        "reason": reason,
    }


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


def _locale_from_meta(meta: Dict) -> str:
    return str((meta or {}).get("content_locale") or "en-US")


def _localized_texts(meta: Dict, key: str) -> List[str]:
    loc = _locale_from_meta(meta).lower()
    base = loc.split("-")[0]
    words: Dict[str, Dict[str, List[str]]] = {
        "next": {
            "en": ["Next"],
            "ru": ["Далее", "Следующий"],
            "es": ["Siguiente"],
            "pt": ["Avançar", "Próximo"],
            "de": ["Weiter"],
            "fr": ["Suivant"],
        },
        "share": {
            "en": ["Share"],
            "ru": ["Поделиться"],
            "es": ["Compartir"],
            "pt": ["Compartilhar"],
            "de": ["Teilen"],
            "fr": ["Partager"],
        },
        "post": {
            "en": ["Post"],
            "ru": ["Опубликовать"],
            "es": ["Publicar"],
            "pt": ["Publicar"],
            "de": ["Posten"],
            "fr": ["Publier"],
        },
        "reels": {
            "en": ["Reels"],
            "ru": ["Reels"],
            "es": ["Reels"],
            "pt": ["Reels"],
            "de": ["Reels"],
            "fr": ["Reels"],
        },
        "select_from_computer": {
            "en": ["Select from computer"],
            "ru": ["Выбрать с компьютера"],
            "es": ["Seleccionar desde la computadora"],
            "pt": ["Selecionar do computador"],
            "de": ["Vom Computer auswählen"],
            "fr": ["Sélectionner depuis l'ordinateur"],
        },
    }
    lang_words = words.get(key, {})
    return lang_words.get(base, lang_words.get("en", []))


def _btn_has_text_selector(text: str) -> str:
    safe = str(text).replace("\\", "\\\\").replace("'", "\\'")
    return f"button:has-text('{safe}')"


# ─────────────────────────────────────────────────────────────────────────────
# YouTube
# ─────────────────────────────────────────────────────────────────────────────

def _upload_youtube(page: Page, video_path: Path, meta: Dict) -> str:
    """Загружает видео на YouTube. Возвращает публичный URL видео."""
    title       = (meta.get("title") or video_path.stem)[:100]
    # FIX: prelend_url добавляется в конец description (только YouTube — кликабельно)
    prelend_url = meta.get("prelend_url", "")
    raw_desc    = meta.get("description") or ""
    if prelend_url:
        link_line = f"\n\n🔗 {prelend_url}"
        raw_desc  = raw_desc[:4990 - len(link_line)] + link_line
    description = raw_desc[:4990]

    page.goto("https://studio.youtube.com", wait_until="domcontentloaded", timeout=30_000)
    _up_pause(2, 4, ctx="yt_studio_open")
    check_and_handle_captcha(page, "youtube")

    for sel in ["ytcp-button#create-icon", "button[aria-label*='reate']", "#create-icon"]:
        try:
            if page.locator(sel).first.is_visible(timeout=5_000):
                page.locator(sel).first.click(); break
        except Exception:
            continue
    _up_pause(1, 2, ctx="yt_after_create_click")

    for sel in ["tp-yt-paper-item#text-item-0", "[test-id='upload-beta']",
                "tp-yt-paper-listbox tp-yt-paper-item:first-child"]:
        try:
            if page.locator(sel).first.is_visible(timeout=3_000):
                page.locator(sel).first.click(); break
        except Exception:
            continue
    _up_pause(1, 2, ctx="yt_after_upload_item")

    page.locator("input[type=file]").first.set_input_files(str(video_path))
    page.wait_for_selector(
        "#title-textarea, ytcp-social-suggestions-textbox#title-textarea", timeout=60_000
    )
    _up_pause(1, 2, ctx="yt_after_file_selected")

    for sel in ["#title-textarea div[contenteditable='true']",
                "ytcp-social-suggestions-textbox#title-textarea div[contenteditable]"]:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=3_000):
                el.click(); page.keyboard.press("Control+a"); _human_type(page, title); break
        except Exception:
            continue
    _up_pause(0.5, 1.5, ctx="yt_after_title")

    if description:
        for sel in ["#description-textarea div[contenteditable='true']",
                    "#description-container div[contenteditable]"]:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=3_000):
                    el.click(); _human_type(page, description); break
            except Exception:
                continue
        _up_pause(0.4, 0.65, ctx="yt_after_description")

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
        _up_pause(1.5, 2.5, ctx="yt_wizard_next")
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
        _up_pause(4.2, 5.8, ctx="yt_upload_progress_poll")

    # Публикация
    _up_pause(0.7, 1.4, ctx="yt_before_publish", risk=HumanizeRisk.HIGH)
    for sel in ["ytcp-button#done-button", "ytcp-button[test-id='publish-button']",
                "button[aria-label*='ublish']", "button[aria-label*='ave']"]:
        try:
            if page.locator(sel).first.is_visible(timeout=5_000):
                page.locator(sel).first.click(); break
        except Exception:
            continue
    _up_pause(0, 0, ctx="yt_after_publish_click", risk=HumanizeRisk.CRITICAL)

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
            _up_pause(1.7, 2.3, ctx="yt_fallback_shorts_list")
            video_url = _try_get_url(
                page,
                ["ytd-grid-video-renderer a#video-title", "a[href*='/shorts/']"],
                "https://www.youtube.com",
            )
        except Exception:
            pass

    _up_pause(2, 3, ctx="yt_finalize")
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
    _up_pause(2, 4, ctx="tiktok_upload_open")
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
        _up_pause(3.5, 4.5, ctx="tiktok_wait_editor")
    _up_pause(1, 2, ctx="tiktok_editor_ready")

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
        _up_pause(0.5, 1.5, ctx="tiktok_after_caption")

    _up_pause(0.7, 1.3, ctx="tiktok_before_post", risk=HumanizeRisk.HIGH)
    post_selectors = ["button.btn-post", "button[data-e2e='post_video_button']"]
    for txt in _localized_texts(meta, "post"):
        post_selectors.append(_btn_has_text_selector(txt))
    for sel in post_selectors:
        try:
            if page.locator(sel).first.is_visible(timeout=5_000):
                page.locator(sel).first.click(); break
        except Exception:
            continue
    _up_pause(0, 0, ctx="tiktok_after_post", risk=HumanizeRisk.CRITICAL)

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

    _up_pause(2, 3, ctx="tiktok_finalize")
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
    _up_pause(2, 4, ctx="ig_home")
    check_and_handle_captcha(page, "instagram")

    for sel in ["svg[aria-label='New post']", "a[href='/create/select/']", "svg[aria-label='New Post']"]:
        try:
            if page.locator(sel).first.is_visible(timeout=5_000):
                page.locator(sel).first.click(); break
        except Exception:
            continue
    _up_pause(1, 2, ctx="ig_after_new_post")

    try:
        page.locator("input[type='file']").first.set_input_files(str(video_path))
    except Exception:
        sc_selectors = [_btn_has_text_selector(x) for x in _localized_texts(meta, "select_from_computer")]
        for sel in sc_selectors:
            try:
                with page.expect_file_chooser() as fc:
                    page.locator(sel).first.click()
                fc.value.set_files(str(video_path))
            except Exception:
                pass
    _up_pause(2, 4, ctx="ig_after_file")

    try:
        for reels_txt in _localized_texts(meta, "reels"):
            reels_sel = _btn_has_text_selector(reels_txt)
            if page.locator(reels_sel).first.is_visible(timeout=5_000):
                page.locator(reels_sel).first.click()
                _up_pause(0.85, 1.15, ctx="ig_reels_select")
                break
    except Exception:
        pass

    for _ in range(3):
        try:
            next_selectors = [_btn_has_text_selector(x) for x in _localized_texts(meta, "next")]
            next_selectors.append("[aria-label='Next']")
            btn = page.locator(", ".join(next_selectors)).first
            if btn.is_visible(timeout=5_000):
                btn.click()
                _up_pause(1, 2, ctx="ig_wizard_next")
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
        _up_pause(0.5, 1.5, ctx="ig_after_caption")

    _up_pause(0.7, 1.3, ctx="ig_before_share", risk=HumanizeRisk.HIGH)
    share_selectors = [_btn_has_text_selector(x) for x in _localized_texts(meta, "share")]
    share_selectors += ["[aria-label='Share']", "button:has-text('Поделиться')"]
    for sel in share_selectors:
        try:
            if page.locator(sel).first.is_visible(timeout=5_000):
                page.locator(sel).first.click(); break
        except Exception:
            continue
    _up_pause(0, 0, ctx="ig_after_share", risk=HumanizeRisk.CRITICAL)

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
            _up_pause(1.7, 2.3, ctx="ig_fallback_home")
            video_url = _try_get_url(
                page, ["a[href*='/reel/']", "article a"], "https://www.instagram.com"
            )
        except Exception:
            pass

    _up_pause(2, 3, ctx="ig_finalize")
    logger.info("[instagram] Опубликовано: %s | URL: %s", video_path.name, video_url or "неизвестен")
    return video_url


# ─────────────────────────────────────────────────────────────────────────────
# Диспетчер
# ─────────────────────────────────────────────────────────────────────────────

_PLATFORM_UPLOADERS = {
    "vk":      _upload_youtube,
    "rutube":  _upload_tiktok,
    "ok":      _upload_instagram,
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

    bridge_mode, fail_open = get_publish_handler_mode(platform)
    if bridge_enabled_for_platform(platform) and bridge_mode in {"active", "fallback"}:
        manual_entry = queue_manual_publish(
            platform=platform,
            account_id=account_name or "unknown",
            video_path=video_path,
            meta=meta,
            reason=f"bridge_mode:{bridge_mode}",
        )
        send_telegram(
            f"🧩 Operator bridge: <b>{platform}</b> / "
            f"<b>{video_path.name}</b> -> {STATUS_MANUAL_REQUIRED} "
            f"(ticket: {manual_entry['ticket_id']})"
        )
        if bridge_mode == "active":
            return MANUAL_REQUIRED_SENTINEL
        if bridge_mode == "fallback" and not fail_open:
            return None

    last_error: Optional[Exception] = None

    with _uploader_humanize_scope(account_cfg):
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
                # Не human_pause: cap в humanize (_MAX_SINGLE_PAUSE_SEC) меньше backoff до 5 мин.
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

            wb, wr = is_upload_blocked(acc_name, platform)
            if wb:
                logger.info("[%s][%s] Прогрев — заливка отложена (%s).", acc_name, platform, wr)
                results.append({
                    "status": "warmup",
                    "platform": platform,
                    "account_id": acc_name,
                    "error_msg": wr,
                })
                continue

            profile_dir = acc_dir / "browser_profile"
            try:
                pw, context = launch_browser(acc_cfg, profile_dir, platform=platform)
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

                w_active, w_msg = is_upload_warmup_active(acc_dir, platform, acc_cfg)
                if w_active:
                    logger.info(
                        "[%s][%s] Прогрев после первой сессии — только активность, без заливки (%s)",
                        acc_name,
                        platform,
                        w_msg,
                    )
                    run_activity(
                        context,
                        platform,
                        queue[0].get("meta", {}),
                        acc_dir=acc_dir,
                        acc_cfg=acc_cfg,
                    )
                    results.append({
                        "status": "warmup",
                        "platform": platform,
                        "account_id": acc_name,
                        "error_msg": w_msg,
                    })
                    continue

                run_activity(
                    context,
                    platform,
                    queue[0].get("meta", {}),
                    acc_dir=acc_dir,
                    acc_cfg=acc_cfg,
                )

                for item in queue:
                    if utils.get_uploads_today(acc_dir) >= daily_limit:
                        break

                    video_path = item["video_path"]
                    # Фича B: A/B — берём назначенный вариант метаданных
                    meta = item.get("ab_meta") or item["meta"]
                    gate = _evaluate_prepublish_gate(meta)
                    if gate["blocked"] and bool(getattr(config, "RISK_GUARD_BLOCK_ON_HIGH_RISK", True)):
                        _memory.emit_agent_event(
                            "UPLOADER",
                            "prepublish_blocked",
                            {"platform": platform, "account": acc_name, "reason": gate["reason"]},
                            creative_id=str(meta.get("creative_id") or Path(video_path).stem),
                            hook_type=str(meta.get("hook_type") or "generic"),
                            experiment_id=str(meta.get("experiment_id") or meta.get("ab_variant") or "default"),
                            agent_run_id=f"uploader:{acc_name}:{platform}",
                            severity="warning",
                        )
                        results.append({
                            "status": "blocked_by_risk_guard",
                            "platform": platform,
                            "account_id": acc_name,
                            "source_path": str(video_path),
                            "error_msg": f"risk={gate['risk_score']} reason={gate['reason']}",
                        })
                        continue

                    if dry_run:
                        logger.info("[dry_run] %s -> %s", video_path.name, platform)
                        results.append({
                            "status": "skipped", "platform": platform,
                            "account_id": acc_name, "source_path": str(video_path),
                        })
                        continue

                    # Пробрасываем prelend_url только для YouTube (кликабельно в description)
                    # Используем per-platform URL с UTM если есть (Nginx /y/acc)
                    if platform == "youtube":
                        meta = dict(meta)
                        prelend_urls = acc_cfg.get("prelend_urls", {})
                        meta["prelend_url"] = (
                            prelend_urls.get("youtube") or acc_cfg.get("prelend_url", "")
                        )

                    # JIT locale-pack: подготавливаем языковую версию только если есть слот загрузки.
                    localized_video, localized_meta = prepare_locale_pack_for_upload(
                        video_path=Path(video_path),
                        base_meta=dict(meta or {}),
                        account_cfg=acc_cfg,
                        platform=platform,
                    )
                    localized_meta["hook_score"] = gate["hook_score"]
                    localized_meta["risk_score"] = gate["risk_score"]
                    localized_meta["retention_prediction"] = gate["retention_prediction"]
                    clean_path = clean_video_metadata(localized_video)
                    video_url  = upload_video(
                        context, platform, clean_path, localized_meta,
                        account_name=acc_name, account_cfg=acc_cfg,
                    )

                    if video_url == MANUAL_REQUIRED_SENTINEL:
                        results.append({
                            "status": STATUS_MANUAL_REQUIRED,
                            "platform": platform,
                            "account_id": acc_name,
                            "source_path": str(video_path),
                            "error_msg": "Задача поставлена в operator bridge",
                        })
                        continue

                    if video_url is not None:
                        utils.mark_uploaded(item)
                        utils.increment_upload_count(acc_dir, platform=platform)
                        q_mark_success(acc_name, platform)
                        # Фикс #1: передаём реальный URL в аналитику
                        register_upload(
                            video_stem=Path(video_path).stem,
                            platform=platform,
                            video_url=video_url,
                            meta=localized_meta,
                            ab_variant=localized_meta.get("ab_variant"),
                        )
                        if config.PRELEND_AUTO_LINK and video_url:
                            try:
                                headers = {"Content-Type": "application/json"}
                                if config.PRELEND_API_KEY:
                                    headers["X-API-Key"] = config.PRELEND_API_KEY
                                resp = requests.post(
                                    f"{config.PRELEND_API_URL}/register_video",
                                    headers=headers,
                                    json={
                                        "video_stem": video_path.stem,
                                        "platform": platform,
                                        "video_url": video_url,
                                        "account": acc_name,
                                        "creative_id": localized_meta.get("creative_id") or video_path.stem,
                                        "hook_type": localized_meta.get("hook_type") or "generic",
                                        "experiment_id": localized_meta.get("experiment_id") or (localized_meta.get("ab_variant") or "default"),
                                        "agent_run_id": f"uploader:{acc_name}:{platform}",
                                    },
                                    timeout=5,
                                )
                                if resp.ok:
                                    tracking = resp.json().get("tracking_url", "")
                                    logger.info("[Uploader] PreLend tracking URL: %s", tracking)
                            except Exception as e:
                                logger.warning("[Uploader] PreLend register_video failed: %s", e)
                        results.append({
                            "status": "uploaded", "platform": platform,
                            "account_id": acc_name, "source_path": str(video_path),
                            "video_url": video_url,
                            "hook_score": gate["hook_score"],
                            "risk_score": gate["risk_score"],
                            "retention_prediction": gate["retention_prediction"],
                        })
                        _memory.emit_agent_event(
                            "UPLOADER",
                            "uploaded",
                            {"platform": platform, "account": acc_name, "video_url": video_url},
                            creative_id=str(localized_meta.get("creative_id") or Path(video_path).stem),
                            hook_type=str(localized_meta.get("hook_type") or "generic"),
                            experiment_id=str(localized_meta.get("experiment_id") or localized_meta.get("ab_variant") or "default"),
                            agent_run_id=f"uploader:{acc_name}:{platform}",
                        )
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
