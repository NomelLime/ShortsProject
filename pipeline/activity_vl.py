"""
pipeline/activity_vl.py — VL-powered activity simulation for ShortsProject.

Drop-in replacement for activity.py that uses Qwen2.5-VL instead of CSS selectors
for feed intelligence. CSS selectors are still used for the actual button clicks
(VL decides what to do; CSS executes it).

What VL handles:
  • Per-feed analysis: one screenshot → decisions for all visible content
  • CAPTCHA detection: visual check replacing fragile CSS selectors
  • Content selection: matches account niche (config["niche"]) to feed content
  • Comment language: auto-detected from video/post content by VL

What CSS still handles:
  • Clicking like/comment buttons (coordinates would break on layout changes too)
  • Search field input

GPU coordination:
  • acquire_gpu_lock() prevents VRAM conflict with Orchestrator LLM calls

Account niche source:
  config.json["niche"] or ["topic"] or ["channel_topic"] → defaults to "general content"
  Add a "niche" field to account config.json to enable niche-aware content selection.

Exports:
    run_activity_vl(context, platform, account_cfg) → None
"""

from __future__ import annotations

import json
import logging
import random
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from rebrowser_playwright.sync_api import BrowserContext, Page

from pipeline import config
from pipeline.config import (
    ACTIVITY_DURATION_MIN_SEC, ACTIVITY_DURATION_MAX_SEC,
    WATCH_TIME_MIN_SEC, WATCH_TIME_MAX_SEC,
    CLICK_DELAY_MIN_SEC, CLICK_DELAY_MAX_SEC,
    PLATFORM_URLS, OLLAMA_MODEL, CAPTCHA_WAIT_TIMEOUT_SEC,
)
from pipeline.humanize import human_scroll_burst
from pipeline.utils import human_sleep
from pipeline.notifications import send_telegram_alert, check_and_handle_captcha
from pipeline.ai import ollama_generate_with_timeout
from pipeline.shared_gpu_lock import acquire_gpu_lock
from pipeline.niche import detect_and_cache_niche


# ─────────────────────────────────────────────────────────────────────────────
# Санитизация комментариев от VL (FIX#V3-1)
# ─────────────────────────────────────────────────────────────────────────────

def _sanitize_comment(text: str, max_len: int = 100) -> str:
    """
    Очищает VL-сгенерированный комментарий перед отправкой на платформу.

    Убирает URL, HTML-теги, @mentions, лишние пробелы.
    Возвращает пустую строку если результат слишком короткий (< 3 символов).

    Args:
        text:    сырой текст от VL-модели
        max_len: максимальная длина результата

    Returns:
        Очищенная строка или "" если небезопасно/слишком коротко.
    """
    if not text:
        return ""
    # URL (http/https и www.)
    text = re.sub(r'https?://\S+', '', text)
    text = re.sub(r'www\.\S+', '', text)
    # HTML / script теги
    text = re.sub(r'<[^>]+>', '', text)
    # @mentions (чтобы не спамить других пользователей)
    text = re.sub(r'@\w+', '', text)
    # Множественные пробелы → один
    text = re.sub(r'\s+', ' ', text).strip()
    # Обрезаем до max_len
    text = text[:max_len]
    # Слишком короткий результат — не отправляем
    return text if len(text) >= 3 else ""


logger = logging.getLogger(__name__)

# Seconds to wait for VL inference per call
_VL_TIMEOUT_SEC = 45

# Re-analyze feed every N scroll iterations (VL calls are GPU-heavy)
_VL_ANALYZE_EVERY_N = 3

# CSS selectors for like buttons (execution, not detection)
_LIKE_SELECTORS: Dict[str, str] = {
    "youtube":   "ytd-toggle-button-renderer#top-level-buttons-computed button[aria-label*='like']",
    "tiktok":    "[data-e2e='like-icon']",
    "instagram": "svg[aria-label='Like']",
}

# CSS selectors for comment input and submit
_COMMENT_INPUT_SELECTORS: Dict[str, str] = {
    "youtube":   "#simplebox-placeholder, #contenteditable-root",
    "tiktok":    "[data-e2e='comment-input']",
    "instagram": "textarea[aria-label*='comment' i], textarea[placeholder*='comment' i]",
}
_COMMENT_SUBMIT_SELECTORS: Dict[str, str] = {
    "youtube":   "#submit-button",
    "tiktok":    "[data-e2e='comment-post']",
    "instagram": "div[role='button'][tabindex='0']",
}

# CSS selectors for search inputs
_SEARCH_INPUT_SELECTORS: Dict[str, str] = {
    "youtube":   "input#search",
    "tiktok":    "input[type='search']",
    "instagram": "input[placeholder='Search']",
}
_SEARCH_SUBMIT_SELECTORS: Dict[str, Optional[str]] = {
    "youtube":   "button#search-icon-legacy",
    "tiktok":    None,
    "instagram": None,
}


# ─────────────────────────────────────────────────────────────────────────────
# Screenshot helper
# ─────────────────────────────────────────────────────────────────────────────

def _screenshot_jpeg(page: Page, quality: int = 75) -> bytes:
    """Captures the current page as JPEG bytes for VL inference."""
    return page.screenshot(type="jpeg", quality=quality)


# ─────────────────────────────────────────────────────────────────────────────
# VL feed analysis
# ─────────────────────────────────────────────────────────────────────────────

def vl_analyze_feed(
    page: Page,
    platform: str,
    account_niche: str,
) -> Dict[str, Any]:
    """
    Analyzes the current feed screenshot with Qwen2.5-VL.

    Returns dict:
      interactions:      list of {action, rank, comment} decisions
      captcha_detected:  bool — true if CAPTCHA / login wall is visible
      search_query:      str or None — niche-relevant search phrase

    Falls back to empty decisions on screenshot or VL error.
    """
    try:
        screenshot = _screenshot_jpeg(page)
    except Exception as e:
        logger.warning("[VL][%s] Screenshot failed: %s", platform, e)
        return _empty_vl_result()

    prompt = (
        f"You are analyzing a {platform} feed screenshot for an account "
        f"focused on the niche: \"{account_niche}\".\n\n"
        "Return ONLY a valid JSON object (no markdown, no extra text):\n"
        "{\n"
        "  \"captcha_detected\": false,\n"
        "  \"interactions\": [\n"
        "    {\"action\": \"like\", \"rank\": 1, \"comment\": null}\n"
        "  ],\n"
        "  \"search_query\": null\n"
        "}\n\n"
        "Field rules:\n"
        f"- captcha_detected: true if you see CAPTCHA, reCAPTCHA, login wall, "
        "or any verification that blocks browsing\n"
        f"- interactions: up to 3 decisions for visible videos/posts; "
        f"ONLY interact with content matching niche \"{account_niche}\"\n"
        "  action: \"like\" | \"comment\" | \"skip\"\n"
        "  rank: 1=topmost visible post, 2=second, etc.\n"
        "  comment: write in the SAME language as the post content, "
        "natural casual tone, max 10 words; null if action != comment\n"
        "  Include at most ONE comment action total\n"
        f"- search_query: 2-4 word phrase to explore more \"{account_niche}\" content, "
        "or null if not needed"
    )

    try:
        with acquire_gpu_lock(consumer=f"VL-Feed-{platform}", timeout=90):
            response = ollama_generate_with_timeout(
                model=OLLAMA_MODEL,
                prompt=prompt,
                images=[screenshot],
                timeout=_VL_TIMEOUT_SEC,
            )
        raw = response.get("response", "") if isinstance(response, dict) else str(response)
        result = _parse_vl_json(raw)
        logger.debug(
            "[VL][%s] Feed analysis: captcha=%s interactions=%d query=%s",
            platform, result["captcha_detected"],
            len(result["interactions"]), result["search_query"],
        )
        return result
    except TimeoutError:
        logger.info("[VL][%s] GPU busy — skipping feed analysis this iteration", platform)
        return _empty_vl_result()
    except Exception as e:
        logger.warning("[VL][%s] Feed analysis failed: %s", platform, e)
        return _empty_vl_result()


def _empty_vl_result() -> Dict[str, Any]:
    return {"interactions": [], "captcha_detected": False, "search_query": None}


# Whitelist разрешённых action-значений — LLM не может вернуть произвольное действие
_VALID_ACTIONS = frozenset({"like", "comment", "skip"})


def _validate_vl_result(raw_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Валидирует и нормализует структуру VL-ответа (FIX#V3-3).

    Фильтрует невалидные action, ограничивает rank до [1, 10],
    обрезает comment, нормализует captcha_detected и search_query.

    Args:
        raw_result: сырой dict от json.loads()

    Returns:
        Безопасный dict с валидированными полями.
    """
    validated: Dict[str, Any] = {
        "captcha_detected": bool(raw_result.get("captcha_detected", False)),
        "interactions":     [],
        "search_query":     None,
    }

    # search_query — строка 2–50 символов или None
    sq = raw_result.get("search_query")
    if isinstance(sq, str):
        sq = sq.strip()[:50]
        if 2 <= len(sq):
            validated["search_query"] = sq

    # interactions — список, max 5 элементов (LLM иногда возвращает больше)
    interactions = raw_result.get("interactions")
    if not isinstance(interactions, list):
        return validated

    for item in interactions[:5]:
        if not isinstance(item, dict):
            continue

        action = str(item.get("action", "skip")).lower()
        if action not in _VALID_ACTIONS:
            logger.debug("[VL] Неизвестный action '%s' → заменяем на skip", action)
            action = "skip"

        try:
            rank = max(1, min(int(item.get("rank") or 1), 10))
        except (TypeError, ValueError):
            rank = 1

        comment = ""
        if action == "comment":
            comment = str(item.get("comment") or "")[:150]

        validated["interactions"].append({
            "action":  action,
            "rank":    rank,
            "comment": comment,
        })

    return validated


def _parse_vl_json(raw: str) -> Dict[str, Any]:
    """Extracts and validates JSON from VL response; tolerant of markdown wrappers."""
    default = _empty_vl_result()
    try:
        clean = re.sub(r"```(?:json)?", "", raw).strip().rstrip("`").strip()
        start = clean.find("{")
        if start == -1:
            return default
        depth = 0
        for i, ch in enumerate(clean[start:], start):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    data = json.loads(clean[start:i + 1])
                    # FIX#V3-3: валидируем структуру ответа
                    return _validate_vl_result(data)
    except Exception as e:
        logger.debug("[VL] JSON parse error: %s | raw: %s", e, raw[:200])
    return default


# ─────────────────────────────────────────────────────────────────────────────
# CAPTCHA handling (VL-enhanced)
# ─────────────────────────────────────────────────────────────────────────────

def _handle_captcha_detected(page: Page, platform: str) -> None:
    """
    Sends Telegram alert and waits for manual CAPTCHA resolution.
    Polls with VL every 15s to detect when CAPTCHA is gone.
    Same UX as original check_and_handle_captcha(), but VL-based re-check.
    """
    msg = (
        f"⚠️ <b>CAPTCHA (VL-detected)</b>\n"
        f"Платформа: {platform}\n"
        f"URL: {page.url}\n"
        f"Ожидаю ручного решения (макс. {CAPTCHA_WAIT_TIMEOUT_SEC // 60} мин)..."
    )
    logger.warning("[VL-CAPTCHA][%s] CAPTCHA detected at %s", platform, page.url)
    send_telegram_alert(msg)

    poll_prompt = (
        "Is there a CAPTCHA, reCAPTCHA, login wall, or verification challenge visible? "
        "Reply ONLY: YES or NO"
    )
    deadline = time.time() + CAPTCHA_WAIT_TIMEOUT_SEC
    while time.time() < deadline:
        time.sleep(15)
        try:
            scr = _screenshot_jpeg(page)
            with acquire_gpu_lock(consumer=f"VL-Captcha-{platform}", timeout=30):
                resp = ollama_generate_with_timeout(
                    model=OLLAMA_MODEL,
                    prompt=poll_prompt,
                    images=[scr],
                    timeout=20,
                )
            answer = (
                resp.get("response", "") if isinstance(resp, dict) else str(resp)
            ).strip().upper()
            if not answer.startswith("YES"):
                logger.info("[VL-CAPTCHA][%s] CAPTCHA resolved.", platform)
                return
        except Exception:
            return  # If poll fails, assume resolved and continue

    logger.error(
        "[VL-CAPTCHA][%s] CAPTCHA not resolved within %ds.", platform, CAPTCHA_WAIT_TIMEOUT_SEC
    )


# ─────────────────────────────────────────────────────────────────────────────
# Interaction execution (VL decides, CSS executes)
# ─────────────────────────────────────────────────────────────────────────────

def _scroll_to_rank(
    page: Page,
    rank: int,
    *,
    account_cfg: Optional[Dict[str, Any]] = None,
) -> None:
    """Scrolls the page to bring the video at the given rank into view."""
    if rank <= 1:
        return
    scroll_px = random.randint(400, 750) * (rank - 1)
    page.mouse.wheel(0, scroll_px)
    human_sleep(0.5, 1.2, account_cfg=account_cfg, agent="ACTIVITY_VL", context="scroll_to_rank")


def _try_like(
    page: Page,
    platform: str,
    *,
    account_cfg: Optional[Dict[str, Any]] = None,
) -> bool:
    """Clicks the like button using CSS selector. Returns True on success."""
    sel = _LIKE_SELECTORS.get(platform)
    if not sel:
        return False
    try:
        btn = page.locator(sel).first
        if btn.is_visible(timeout=3_000):
            btn.click()
            human_sleep(0.5, 1.5, account_cfg=account_cfg, agent="ACTIVITY_VL", context="vl_like")
            logger.debug("[VL][%s] Like clicked", platform)
            return True
    except Exception:
        pass
    return False


def _try_comment(
    page: Page,
    platform: str,
    comment_text: str,
    *,
    account_cfg: Optional[Dict[str, Any]] = None,
) -> bool:
    """Types and submits a comment. Returns True on success."""
    input_sel = _COMMENT_INPUT_SELECTORS.get(platform)
    submit_sel = _COMMENT_SUBMIT_SELECTORS.get(platform)
    if not input_sel or not comment_text:
        return False
    try:
        inp = page.locator(input_sel).first
        if not inp.is_visible(timeout=3_000):
            return False
        inp.click()
        human_sleep(0.3, 0.8, account_cfg=account_cfg, agent="ACTIVITY_VL", context="comment_focus")
        for ch in comment_text:
            page.keyboard.type(ch)
            time.sleep(random.uniform(0.04, 0.12))
        human_sleep(0.5, 1.5, account_cfg=account_cfg, agent="ACTIVITY_VL", context="comment_typed")
        if submit_sel:
            page.locator(submit_sel).first.click()
        else:
            page.keyboard.press("Enter")
        human_sleep(1, 2, account_cfg=account_cfg, agent="ACTIVITY_VL", context="comment_submit")
        logger.info("[VL][%s] Comment posted: %s", platform, comment_text[:50])
        return True
    except Exception as e:
        logger.debug("[VL][%s] Comment failed: %s", platform, e)
        return False


def _execute_interactions(
    page: Page,
    platform: str,
    interactions: List[Dict],
    like_budget: int,
    comment_done: bool,
    *,
    account_cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[int, bool]:
    """
    Executes VL-decided interactions on the current feed state.
    Returns (remaining_like_budget, comment_done).
    """
    for decision in interactions:
        action = decision.get("action", "skip")
        rank = int(decision.get("rank") or 1)
        comment_text = (decision.get("comment") or "").strip()

        if action == "skip":
            continue

        _scroll_to_rank(page, rank, account_cfg=account_cfg)
        human_sleep(0.3, 0.8, account_cfg=account_cfg, agent="ACTIVITY_VL", context="vl_after_scroll")

        if action == "like" and like_budget > 0:
            if _try_like(page, platform, account_cfg=account_cfg):
                like_budget -= 1
                human_sleep(1, 2, account_cfg=account_cfg, agent="ACTIVITY_VL", context="vl_after_like")

        elif action == "comment" and not comment_done and comment_text:
            safe_comment = _sanitize_comment(comment_text)
            if safe_comment and _try_comment(page, platform, safe_comment, account_cfg=account_cfg):
                comment_done = True
            elif not safe_comment:
                logger.info("[VL][%s] Comment sanitized to empty — skipping", platform)

    return like_budget, comment_done


# ─────────────────────────────────────────────────────────────────────────────
# Search
# ─────────────────────────────────────────────────────────────────────────────

def _perform_search(
    page: Page,
    platform: str,
    query: str,
    *,
    account_cfg: Optional[Dict[str, Any]] = None,
) -> None:
    """Performs a search using the VL-suggested query."""
    sel = _SEARCH_INPUT_SELECTORS.get(platform)
    if not sel or not query:
        return
    logger.info("[VL][%s] Searching: «%s»", platform, query)
    try:
        page.locator(sel).first.click(timeout=5_000)
        human_sleep(0.4, 1.0, account_cfg=account_cfg, agent="ACTIVITY_VL", context="vl_search_focus")
        for ch in query:
            page.keyboard.type(ch)
            time.sleep(random.uniform(0.05, 0.15))
        human_sleep(0.5, 1.5, account_cfg=account_cfg, agent="ACTIVITY_VL", context="vl_search_typed")
        sub = _SEARCH_SUBMIT_SELECTORS.get(platform)
        if sub:
            page.locator(sub).click()
        else:
            page.keyboard.press("Enter")
        human_sleep(2, 4, account_cfg=account_cfg, agent="ACTIVITY_VL", context="vl_search_submit")
        _random_scroll(page, scrolls=random.randint(2, 4), account_cfg=account_cfg)
    except Exception as e:
        logger.warning("[VL][%s] Search failed: %s", platform, e)


def _random_scroll(
    page: Page,
    scrolls: int = None,
    *,
    account_cfg: Optional[Dict[str, Any]] = None,
) -> None:
    """Random human-like scroll."""
    human_scroll_burst(
        page,
        scrolls=scrolls or random.randint(3, 7),
        account_cfg=account_cfg,
        agent="ACTIVITY_VL",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main activity function
# ─────────────────────────────────────────────────────────────────────────────

def run_activity_vl(
    context: BrowserContext,
    platform: str,
    account: Dict[str, Any],
) -> None:
    """
    VL-powered activity simulation. Drop-in replacement for run_activity().

    account — полный объект из get_all_accounts(): {name, dir, config, platforms}.
    Читает account["config"]["niche"] для выбора контента.
    Если ниша не задана — определяет автоматически через niche.py (B → C).

    Добавьте в config.json для явного задания ниши:
        "niche": "fitness and gym workouts"
    """
    account_cfg = account["config"]

    try:
        from pipeline.vl_warm import warm_vl_model

        warm_vl_model()
    except Exception as exc:
        logger.debug("[VL-Activity] warm_vl_model: %s", exc)

    urls = PLATFORM_URLS.get(platform, {})
    feed_url = (
        urls.get("shorts")
        or urls.get("feed")
        or urls.get("reels")
        or urls.get("home")
    )
    if not feed_url:
        logger.warning("[VL-Activity][%s] No feed_url configured — skipping", platform)
        return

    # Авто-определение ниши если не задана (B → C → "general content")
    # detect_and_cache_niche сохраняет результат в config.json["niche"]
    account_niche = detect_and_cache_niche(account)

    from pathlib import Path as _Path

    from pipeline.upload_warmup import is_upload_warmup_active

    acc_dir = _Path(account["dir"])
    in_warmup, _ = is_upload_warmup_active(acc_dir, platform, account_cfg)
    wmult = float(getattr(config, "ACTIVITY_WARMUP_DURATION_MULT", 1.0) or 1.0)
    if in_warmup and 0 < wmult < 1.0:
        lo = max(60, int(ACTIVITY_DURATION_MIN_SEC * wmult))
        hi = max(lo + 30, int(ACTIVITY_DURATION_MAX_SEC * wmult))
        duration = random.randint(lo, hi)
        logger.info(
            "[VL-Activity][%s] Прогрев заливки — сокращённая сессия (~%.0f%% длительности)",
            platform,
            wmult * 100,
        )
    else:
        duration = random.randint(ACTIVITY_DURATION_MIN_SEC, ACTIVITY_DURATION_MAX_SEC)
    logger.info(
        "[VL-Activity][%s] Starting (niche: %s, duration: %d min)",
        platform, account_niche, duration // 60,
    )

    page = context.new_page()
    try:
        page.goto(feed_url, wait_until="domcontentloaded", timeout=30_000)
        human_sleep(2, 4, account_cfg=account_cfg, agent="ACTIVITY_VL", context="vl_feed_open")

        deadline = time.time() + duration
        like_budget = random.randint(1, 4)
        comment_done = False
        search_done = False
        iteration = 0
        vl_result: Optional[Dict[str, Any]] = None

        while time.time() < deadline:
            # CSS CAPTCHA fast check (existing, cheap)
            check_and_handle_captcha(page, platform)

            # VL feed analysis every N iterations
            if iteration % _VL_ANALYZE_EVERY_N == 0:
                vl_result = vl_analyze_feed(page, platform, account_niche)

                # VL detected CAPTCHA (catches cases CSS selectors miss)
                if vl_result.get("captcha_detected"):
                    _handle_captcha_detected(page, platform)
                    if time.time() >= deadline:
                        break
                    # Re-navigate and re-analyze after CAPTCHA
                    page.goto(feed_url, wait_until="domcontentloaded", timeout=30_000)
                    human_sleep(2, 4, account_cfg=account_cfg, agent="ACTIVITY_VL", context="vl_after_captcha")
                    vl_result = None
                    iteration = 0
                    continue

            # Execute VL interaction decisions
            if vl_result and vl_result.get("interactions"):
                like_budget, comment_done = _execute_interactions(
                    page, platform,
                    vl_result["interactions"],
                    like_budget, comment_done,
                    account_cfg=account_cfg,
                )
                vl_result["interactions"] = []  # consume — don't repeat on next loop

            # One-time search using VL-suggested query (50% chance)
            if (
                not search_done
                and vl_result
                and vl_result.get("search_query")
                and random.random() < 0.5
            ):
                _perform_search(page, platform, vl_result["search_query"], account_cfg=account_cfg)
                search_done = True
                human_sleep(3, 6, account_cfg=account_cfg, agent="ACTIVITY_VL", context="vl_after_search")
                page.goto(feed_url, wait_until="domcontentloaded", timeout=30_000)
                human_sleep(1, 3, account_cfg=account_cfg, agent="ACTIVITY_VL", context="vl_back_feed")
                vl_result = None  # re-analyze fresh feed after navigation
                iteration = 0
                continue

            _random_scroll(page, account_cfg=account_cfg)
            watch_time = random.randint(WATCH_TIME_MIN_SEC, WATCH_TIME_MAX_SEC)
            time.sleep(watch_time)
            human_sleep(
                CLICK_DELAY_MIN_SEC,
                CLICK_DELAY_MAX_SEC,
                account_cfg=account_cfg,
                agent="ACTIVITY_VL",
                context="vl_tick",
            )
            iteration += 1

    except Exception as e:
        logger.error("[VL-Activity][%s] Error: %s", platform, e)
    finally:
        page.close()

    logger.info("[VL-Activity][%s] Completed.", platform)
