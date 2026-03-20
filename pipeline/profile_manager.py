"""
pipeline/profile_manager.py — Управление ссылками в профилях аккаунтов.

Автоматизирует размещение и проверку PreLend URL в:
  - YouTube: About канала (раздел Links, через Studio)
  - TikTok: Bio → Website (при 1000+ подписчиках или бизнес-аккаунте)
  - Instagram: Bio → Website (всегда доступно)

Self-healing через VL: если CSS-селекторы устарели после обновления вёрстки —
_find_element_with_fallback() делает скриншот и спрашивает Ollama VL где элемент.
_verify_page_context() перед редактированием убеждается что мы на нужной странице.

VL-fallback graceful: если Ollama недоступен — работает только через CSS-селекторы.

Экспортирует:
    setup_profile_link(context, platform, prelend_url, bio_text) → bool
    verify_profile_link(context, platform, expected_url) → bool
    setup_all_links(account_cfg, profile_dir) → dict[str, bool]
    verify_all_links(account_cfg, profile_dir) → dict[str, bool]
"""
from __future__ import annotations

import logging
import random
import re
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Утилиты ввода / пауз
# ─────────────────────────────────────────────────────────────────────────────

def _human_type(page, text: str) -> None:
    """Посимвольный ввод с человекоподобными задержками."""
    for char in text:
        page.keyboard.type(char)
        time.sleep(random.uniform(0.04, 0.14))


def _human_pause(lo: float = 1.0, hi: float = 3.0) -> None:
    """Пауза как у живого человека."""
    time.sleep(random.uniform(lo, hi))


# ─────────────────────────────────────────────────────────────────────────────
# Profile lock: предотвращает одновременный launch_browser на одном профиле
# ─────────────────────────────────────────────────────────────────────────────

@contextmanager
def _profile_lock(profile_dir: Path, timeout: float = 30.0):
    """
    File lock на browser profile_dir.

    Предотвращает одновременный launch_browser() на одном профиле — если
    Guardian и Publisher запустятся одновременно, второй пропустит операцию
    (не крашится). Использует portalocker (уже в requirements.txt).

    Args:
        profile_dir: директория профиля браузера
        timeout:     секунды ожидания блокировки

    Yields:
        True если блокировка получена, False если timeout.
    """
    try:
        import portalocker as _portalocker
    except ImportError:
        logger.debug("[profile] portalocker не установлен — lock пропущен")
        yield True
        return

    lock_path = profile_dir / ".profile.lock"
    lock_file = None
    acquired  = False
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_file = open(str(lock_path), "w")
        _portalocker.lock(lock_file, _portalocker.LOCK_EX, timeout=timeout)
        acquired = True
        yield True
    except (_portalocker.LockException, _portalocker.AlreadyLocked):
        logger.warning(
            "[profile] Profile %s заблокирован — пропуск (другой процесс активен)",
            profile_dir.name,
        )
        yield False
    except Exception as exc:
        logger.warning("[profile] profile_lock ошибка: %s", exc)
        yield True  # fallback: не блокируем при неожиданной ошибке
    finally:
        if lock_file:
            try:
                if acquired:
                    _portalocker.unlock(lock_file)
                lock_file.close()
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────────────────
# Self-healing: CSS-first → VL-fallback
# ─────────────────────────────────────────────────────────────────────────────

def _find_element_with_fallback(
    page,
    css_selectors: list[str],
    vl_prompt: str,
    description: str = "element",
    timeout: int = 5_000,
):
    """
    Ищет элемент на странице: CSS-first, затем VL-fallback по скриншоту.

    Шаг 1 — CSS (быстро, 0 GPU): перебирает селекторы, возвращает первый видимый.
    Шаг 2 — VL (если CSS не нашёл): скриншот → Ollama VL → координаты → клик.

    VL graceful: при недоступном Ollama — возвращает None без ошибки.

    Args:
        page:          Playwright Page
        css_selectors: список CSS-селекторов (в порядке приоритета)
        vl_prompt:     описание элемента для VL («найди поле Website на странице ...»)
        description:   имя элемента для логов
        timeout:       таймаут видимости каждого CSS-селектора (мс)

    Returns:
        Locator или None
    """
    # ── Шаг 1: CSS ──────────────────────────────────────────────────────────
    for sel in css_selectors:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=timeout):
                logger.debug("[selector] CSS: %s → %s", description, sel)
                return el
        except Exception:
            continue

    logger.info("[selector] CSS miss для '%s' — VL fallback", description)

    # ── Шаг 2: VL fallback ──────────────────────────────────────────────────
    try:
        screenshot = page.screenshot(type="jpeg", quality=80)

        from pipeline.ai import ollama_generate_with_timeout, OLLAMA_MODEL

        response = ollama_generate_with_timeout(
            model=OLLAMA_MODEL,
            prompt=(
                f"{vl_prompt}\n\n"
                "Ответь ТОЛЬКО координатами центра элемента в формате: X,Y\n"
                "X и Y — пиксели от левого верхнего угла скриншота.\n"
                "Если элемент не найден — ответь: NOT_FOUND"
            ),
            images=[screenshot],
            timeout=30,
        )

        raw = (response.get("response", "") if isinstance(response, dict) else str(response)).strip()

        if "NOT_FOUND" in raw.upper():
            logger.warning("[selector] VL не нашёл '%s'", description)
            return None

        match = re.search(r"(\d+)\s*[,;]\s*(\d+)", raw)
        if not match:
            logger.warning("[selector] VL невалидные координаты: %s", raw)
            return None

        x, y = int(match.group(1)), int(match.group(2))
        logger.info("[selector] VL нашёл '%s' на (%d, %d)", description, x, y)

        page.mouse.click(x, y)
        _human_pause(0.5, 1.0)

        return page.locator(":focus").first

    except Exception as exc:
        logger.warning("[selector] VL fallback ошибка для '%s': %s", description, exc)
        return None


def _verify_page_context(page, platform: str, expected_type: str) -> bool:
    """
    VL-проверка: мы на правильной странице?

    Защищает от CAPTCHA, редиректов и popup'ов перед редактированием профиля.
    При ошибке VL возвращает True (graceful — не блокирует CSS-only режим).

    Args:
        page:          Playwright Page
        platform:      youtube | tiktok | instagram
        expected_type: описание ожидаемой страницы («edit profile settings»)

    Returns:
        True если VL подтвердил правильную страницу, или VL недоступен.
    """
    try:
        screenshot = page.screenshot(type="jpeg", quality=60)

        from pipeline.ai import ollama_generate_with_timeout, OLLAMA_MODEL

        response = ollama_generate_with_timeout(
            model=OLLAMA_MODEL,
            prompt=(
                f"Это скриншот страницы {platform}.\n"
                f"Это страница {expected_type}?\n"
                "Ответь ТОЛЬКО: YES или NO"
            ),
            images=[screenshot],
            timeout=15,
        )

        answer = (response.get("response", "") if isinstance(response, dict) else str(response)).strip().upper()
        ok = "YES" in answer
        if not ok:
            logger.warning(
                "[context] VL: страница ≠ '%s' (ответ: %s)", expected_type, answer
            )
        return ok

    except Exception as exc:
        logger.warning("[context] VL проверка страницы упала: %s — продолжаем в CSS-режиме", exc)
        return True  # graceful: не блокируем при недоступном VL


# ─────────────────────────────────────────────────────────────────────────────
# YouTube: About → Links (через Studio Customization)
# ─────────────────────────────────────────────────────────────────────────────

def _setup_youtube_about(page, prelend_url: str, bio_text: str = "") -> bool:
    """
    Размещает ссылку в разделе About/Links канала YouTube через Studio.

    Studio-путь (studio.youtube.com/channel/editing/basic) надёжнее публичного
    UI — не зависит от редизайна About-страницы.

    Returns:
        True если ссылка успешно добавлена или уже была на месте.
    """
    try:
        page.goto(
            "https://studio.youtube.com/channel/editing/basic",
            wait_until="domcontentloaded",
            timeout=30_000,
        )
        _human_pause(3, 5)

        if "studio.youtube.com" not in page.url:
            logger.warning("[profile][youtube] Нет доступа к Studio — не залогинен?")
            return False

        # VL-проверка: мы на странице кастомизации?
        if not _verify_page_context(page, "YouTube Studio", "channel customization basic info page"):
            return False

        # ── Описание канала ───────────────────────────────────────────────
        if bio_text:
            desc_el = _find_element_with_fallback(
                page,
                css_selectors=[
                    "textarea#description-container",
                    "div#description-container textarea",
                    "#description-textarea div[contenteditable='true']",
                ],
                vl_prompt="Найди текстовое поле описания канала (Channel description) на странице настроек YouTube Studio",
                description="youtube_channel_description",
            )
            if desc_el:
                desc_el.click()
                page.keyboard.press("Control+a")
                _human_type(page, bio_text)
                _human_pause(0.5, 1.5)

        # ── Ссылки (Links section) ────────────────────────────────────────
        # Скролл к секции
        for _ in range(5):
            page.mouse.wheel(0, 400)
            _human_pause(0.3, 0.7)

        # Проверяем: ссылка уже есть?
        try:
            link_inputs = page.locator(
                "#links-section input[type='text'], "
                "input[placeholder*='URL'], "
                ".link-input input"
            ).all()
            for el in link_inputs:
                val = el.input_value(timeout=2_000) or ""
                if prelend_url.rstrip("/") in val.rstrip("/"):
                    logger.info("[profile][youtube] Ссылка уже есть в About")
                    return True
        except Exception:
            pass

        # Кнопка "Add link"
        add_btn = _find_element_with_fallback(
            page,
            css_selectors=[
                "button:has-text('Add link')",
                "ytcp-button:has-text('Add link')",
                "#links-section button",
                "button[aria-label*='Add link']",
            ],
            vl_prompt="Найди кнопку 'Add link' или 'Добавить ссылку' в разделе Links на странице настроек YouTube Studio",
            description="youtube_add_link_button",
        )
        if not add_btn:
            logger.warning("[profile][youtube] Кнопка 'Add link' не найдена")
            return False

        add_btn.click()
        _human_pause(1, 2)

        # Title поле
        title_el = _find_element_with_fallback(
            page,
            css_selectors=[
                "input[placeholder*='Link title']",
                "input[placeholder*='Title']",
                "#links-section input:first-of-type",
            ],
            vl_prompt="Найди поле 'Link title' или 'Заголовок ссылки' в форме добавления ссылки YouTube Studio",
            description="youtube_link_title",
        )
        if title_el:
            title_el.click()
            _human_type(page, "🔗 Link in Bio")
            _human_pause(0.5, 1.0)

        # URL поле
        url_el = _find_element_with_fallback(
            page,
            css_selectors=[
                "input[placeholder*='URL']",
                "input[type='url']",
                "#links-section input:last-of-type",
            ],
            vl_prompt="Найди поле ввода URL ссылки в форме добавления ссылки YouTube Studio",
            description="youtube_link_url",
        )
        if not url_el:
            logger.warning("[profile][youtube] Поле URL не найдено")
            return False

        url_el.click()
        _human_type(page, prelend_url)
        _human_pause(0.5, 1.0)

        # Publish / Save
        save_btn = _find_element_with_fallback(
            page,
            css_selectors=[
                "ytcp-button#publish-button",
                "button:has-text('Publish')",
                "button:has-text('Save')",
                "ytcp-button:has-text('Publish')",
            ],
            vl_prompt="Найди кнопку Publish или Save для сохранения изменений в YouTube Studio",
            description="youtube_publish_button",
        )
        if save_btn:
            save_btn.click()
            _human_pause(2, 4)
            logger.info("[profile][youtube] About ссылка сохранена: %s", prelend_url)
            return True

        logger.warning("[profile][youtube] Кнопка Publish/Save не найдена")
        return False

    except Exception as exc:
        logger.error("[profile][youtube] Ошибка setup_about: %s", exc)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# TikTok: Bio → Website
# ─────────────────────────────────────────────────────────────────────────────

def _setup_tiktok_bio(page, prelend_url: str, bio_text: str = "") -> bool:
    """
    Размещает ссылку в TikTok профиле (поле Website → Edit profile).

    ОГРАНИЧЕНИЕ: поле Website доступно только при 1000+ подписчиках или
    бизнес-аккаунте. Если поле не найдено — возвращает False без ошибки.

    Returns:
        True если ссылка успешно установлена.
    """
    try:
        page.goto(
            "https://www.tiktok.com/setting/edit-profile",
            wait_until="domcontentloaded",
            timeout=30_000,
        )
        _human_pause(2, 4)

        if "/login" in page.url:
            logger.warning("[profile][tiktok] Не авторизован")
            return False

        if not _verify_page_context(page, "TikTok", "edit profile settings page"):
            return False

        # ── Bio текст ────────────────────────────────────────────────────
        if bio_text:
            bio_el = _find_element_with_fallback(
                page,
                css_selectors=[
                    "textarea[placeholder*='Bio']",
                    "textarea[name='bio']",
                    "[data-e2e='bio-input'] textarea",
                    "div[contenteditable='true'][aria-label*='Bio']",
                ],
                vl_prompt="Найди поле Bio/Biography для редактирования описания профиля TikTok",
                description="tiktok_bio_textarea",
            )
            if bio_el:
                bio_el.click()
                page.keyboard.press("Control+a")
                _human_type(page, bio_text)
                _human_pause(0.5, 1.0)

        # ── Website ──────────────────────────────────────────────────────
        website_el = _find_element_with_fallback(
            page,
            css_selectors=[
                "input[placeholder*='Website']",
                "input[name='website']",
                "input[type='url']",
                "[data-e2e='website-input'] input",
                "input[placeholder*='www']",
            ],
            vl_prompt="Найди поле ввода Website URL на странице Edit profile TikTok",
            description="tiktok_website_input",
        )

        if not website_el:
            logger.info(
                "[profile][tiktok] Поле Website не найдено — "
                "вероятно, аккаунт < 1000 подписчиков или не бизнес-аккаунт"
            )
            return False

        # Проверяем текущее значение
        try:
            current = website_el.input_value(timeout=2_000) or ""
            if prelend_url.rstrip("/") in current.rstrip("/"):
                logger.info("[profile][tiktok] Website уже установлен: %s", current)
                return True
        except Exception:
            pass

        website_el.click()
        page.keyboard.press("Control+a")
        _human_type(page, prelend_url)
        _human_pause(0.5, 1.0)

        # Сохранение
        save_btn = _find_element_with_fallback(
            page,
            css_selectors=[
                "button:has-text('Save')",
                "button[type='submit']",
                "[data-e2e='save-btn']",
            ],
            vl_prompt="Найди кнопку Save или сохранить на странице редактирования профиля TikTok",
            description="tiktok_save_button",
        )
        if save_btn:
            save_btn.click()
            _human_pause(2, 4)
            logger.info("[profile][tiktok] Bio/Website сохранены: %s", prelend_url)
            return True

        logger.warning("[profile][tiktok] Кнопка Save не найдена")
        return False

    except Exception as exc:
        logger.error("[profile][tiktok] Ошибка setup_bio: %s", exc)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Instagram: Bio → Website
# ─────────────────────────────────────────────────────────────────────────────

def _setup_instagram_bio(page, prelend_url: str, bio_text: str = "") -> bool:
    """
    Размещает ссылку в Instagram профиле (поле Website → Edit Profile).

    Instagram Website field доступен всегда — нет порога подписчиков.

    Returns:
        True если ссылка успешно установлена.
    """
    try:
        page.goto(
            "https://www.instagram.com/accounts/edit/",
            wait_until="domcontentloaded",
            timeout=30_000,
        )
        _human_pause(2, 4)

        if "/login" in page.url or "/accounts/login" in page.url:
            logger.warning("[profile][instagram] Не авторизован")
            return False

        if not _verify_page_context(page, "Instagram", "edit profile settings page"):
            return False

        # ── Bio текст ────────────────────────────────────────────────────
        if bio_text:
            bio_el = _find_element_with_fallback(
                page,
                css_selectors=[
                    "textarea#pepBio",
                    "textarea[name='biography']",
                    "textarea[aria-label*='Bio']",
                ],
                vl_prompt="Найди текстовое поле Bio/Biography на странице редактирования профиля Instagram",
                description="instagram_bio_textarea",
            )
            if bio_el:
                bio_el.click()
                page.keyboard.press("Control+a")
                _human_type(page, bio_text)
                _human_pause(0.5, 1.0)

        # ── Website ──────────────────────────────────────────────────────
        website_el = _find_element_with_fallback(
            page,
            css_selectors=[
                "input#pepUrl",
                "input[name='external_url']",
                "input[type='url']",
                "input[aria-label*='Website']",
                "input[placeholder*='Website']",
            ],
            vl_prompt="Найди поле ввода Website URL на странице редактирования профиля Instagram",
            description="instagram_website_input",
        )
        if not website_el:
            logger.warning("[profile][instagram] Поле Website не найдено (ни CSS, ни VL)")
            return False

        # Проверяем текущее значение
        try:
            current = website_el.input_value(timeout=2_000) or ""
            if prelend_url.rstrip("/") in current.rstrip("/"):
                logger.info("[profile][instagram] Website уже установлен: %s", current)
                return True
        except Exception:
            pass

        website_el.click()
        page.keyboard.press("Control+a")
        _human_type(page, prelend_url)
        _human_pause(0.5, 1.0)

        # Сохранение
        save_btn = _find_element_with_fallback(
            page,
            css_selectors=[
                "button:has-text('Submit')",
                "div[role='button']:has-text('Submit')",
                "button[type='submit']",
                "button:has-text('Отправить')",
            ],
            vl_prompt="Найди кнопку Submit/Save/Сохранить на странице редактирования профиля Instagram",
            description="instagram_save_button",
        )
        if save_btn:
            save_btn.click()
            _human_pause(2, 4)
            logger.info("[profile][instagram] Bio/Website сохранены: %s", prelend_url)
            return True

        logger.warning("[profile][instagram] Кнопка Submit не найдена")
        return False

    except Exception as exc:
        logger.error("[profile][instagram] Ошибка setup_bio: %s", exc)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Верификация ссылок
# ─────────────────────────────────────────────────────────────────────────────

def _verify_youtube_link(page, expected_url: str) -> bool:
    """Проверяет наличие ссылки в разделе Links (Studio)."""
    try:
        page.goto(
            "https://studio.youtube.com/channel/editing/basic",
            wait_until="domcontentloaded",
            timeout=30_000,
        )
        _human_pause(2, 4)

        for _ in range(5):
            page.mouse.wheel(0, 400)
            _human_pause(0.3, 0.5)

        link_inputs = page.locator(
            "#links-section input[type='text'], input[placeholder*='URL'], .link-input input"
        ).all()
        for el in link_inputs:
            try:
                val = el.input_value(timeout=2_000) or ""
                if expected_url.rstrip("/") in val.rstrip("/"):
                    return True
            except Exception:
                continue
        return False

    except Exception as exc:
        logger.warning("[profile][youtube] Verify error: %s", exc)
        return False


def _verify_tiktok_link(page, expected_url: str) -> bool:
    """Проверяет наличие ссылки в TikTok профиле."""
    try:
        page.goto(
            "https://www.tiktok.com/setting/edit-profile",
            wait_until="domcontentloaded",
            timeout=20_000,
        )
        _human_pause(2, 3)

        website_el = _find_element_with_fallback(
            page,
            css_selectors=[
                "input[name='website']",
                "input[type='url']",
                "input[placeholder*='Website']",
            ],
            vl_prompt="Найди поле Website URL на странице Edit profile TikTok",
            description="tiktok_website_verify",
            timeout=3_000,
        )
        if not website_el:
            return False

        val = website_el.input_value(timeout=2_000) or ""
        return expected_url.rstrip("/") in val.rstrip("/")

    except Exception as exc:
        logger.warning("[profile][tiktok] Verify error: %s", exc)
        return False


def _verify_instagram_link(page, expected_url: str) -> bool:
    """Проверяет наличие ссылки в Instagram профиле."""
    try:
        page.goto(
            "https://www.instagram.com/accounts/edit/",
            wait_until="domcontentloaded",
            timeout=20_000,
        )
        _human_pause(2, 3)

        website_el = _find_element_with_fallback(
            page,
            css_selectors=[
                "input#pepUrl",
                "input[name='external_url']",
                "input[type='url']",
            ],
            vl_prompt="Найди поле Website URL на странице редактирования профиля Instagram",
            description="instagram_website_verify",
            timeout=3_000,
        )
        if not website_el:
            return False

        val = website_el.input_value(timeout=2_000) or ""
        return expected_url.rstrip("/") in val.rstrip("/")

    except Exception as exc:
        logger.warning("[profile][instagram] Verify error: %s", exc)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Диспетчеры
# ─────────────────────────────────────────────────────────────────────────────

_SETUP_HANDLERS = {
    "youtube":   _setup_youtube_about,
    "tiktok":    _setup_tiktok_bio,
    "instagram": _setup_instagram_bio,
}

_VERIFY_HANDLERS = {
    "youtube":   _verify_youtube_link,
    "tiktok":    _verify_tiktok_link,
    "instagram": _verify_instagram_link,
}


def setup_profile_link(
    context,
    platform: str,
    prelend_url: str,
    bio_text: str = "",
) -> bool:
    """
    Размещает PreLend-ссылку в профиле указанной платформы.

    Args:
        context:     авторизованный Playwright BrowserContext
        platform:    youtube | tiktok | instagram
        prelend_url: URL PreLend лендинга
        bio_text:    текст для описания профиля (опционально)

    Returns:
        True если ссылка успешно размещена или уже была на месте.
    """
    if not prelend_url:
        logger.warning("[profile][%s] prelend_url пуст — пропуск", platform)
        return False

    handler = _SETUP_HANDLERS.get(platform)
    if not handler:
        logger.warning("[profile] Неизвестная платформа: %s", platform)
        return False

    page = context.new_page()
    try:
        return handler(page, prelend_url, bio_text)
    finally:
        try:
            page.close()
        except Exception:
            pass


def verify_profile_link(
    context,
    platform: str,
    expected_url: str,
) -> bool:
    """
    Проверяет что ссылка на месте в профиле платформы.

    Returns:
        True если ссылка найдена. Неизвестная платформа → True (не блокируем).
    """
    handler = _VERIFY_HANDLERS.get(platform)
    if not handler:
        return True

    page = context.new_page()
    try:
        return handler(page, expected_url)
    finally:
        try:
            page.close()
        except Exception:
            pass


def setup_all_links(
    account_cfg: dict,
    profile_dir: Path,
) -> Dict[str, bool]:
    """
    Размещает PreLend-ссылку во всех платформах аккаунта.

    Использует per-platform URL с UTM (prelend_urls[platform]) если доступен,
    иначе — общий prelend_url. Per-platform URL генерируется setup_account.py
    и содержит Nginx-rewrite путь (/t/, /i/, /y/) для UTM-аналитики.

    Returns:
        dict вида {"youtube": True, "tiktok": False, "instagram": True}
    """
    from pipeline.browser import launch_browser, close_browser

    prelend_url  = account_cfg.get("prelend_url", "")
    prelend_urls = account_cfg.get("prelend_urls", {})

    if not prelend_url and not prelend_urls:
        logger.warning("[profile] prelend_url не задан в config.json — пропуск")
        return {}

    platforms = account_cfg.get("platforms", [])
    if isinstance(platforms, str):
        platforms = [platforms]

    bio_text = account_cfg.get("bio_text", "")
    results: Dict[str, bool] = {}

    with _profile_lock(profile_dir) as acquired:
        if not acquired:
            return {p: False for p in platforms}

        try:
            pw, context = launch_browser(account_cfg, profile_dir)
        except Exception as exc:
            logger.error("[profile] Браузер не запустился: %s", exc)
            return {p: False for p in platforms}

        try:
            for platform in platforms:
                # Per-platform URL с UTM (приоритет) → fallback на общий prelend_url
                url_for_platform = prelend_urls.get(platform) or prelend_url
                platform_bio     = account_cfg.get(f"bio_text_{platform}", bio_text)

                logger.info("[profile] Установка ссылки: %s → %s", platform, url_for_platform)

                ok = setup_profile_link(context, platform, url_for_platform, platform_bio)
                results[platform] = ok

                status = "✅" if ok else "❌"
                logger.info("[profile][%s] %s Ссылка %s", platform, status,
                            "установлена" if ok else "НЕ установлена")
                _human_pause(3, 6)
        finally:
            close_browser(pw, context)

    return results


def verify_all_links(
    account_cfg: dict,
    profile_dir: Path,
) -> Dict[str, bool]:
    """
    Проверяет наличие ссылок во всех платформах аккаунта.

    Проверяет per-platform URL (с UTM) если доступен, иначе — общий.

    Returns:
        dict вида {"youtube": True, "tiktok": True, "instagram": False}
    """
    from pipeline.browser import launch_browser, close_browser

    prelend_url  = account_cfg.get("prelend_url", "")
    prelend_urls = account_cfg.get("prelend_urls", {})

    if not prelend_url and not prelend_urls:
        return {}

    platforms = account_cfg.get("platforms", [])
    if isinstance(platforms, str):
        platforms = [platforms]

    results: Dict[str, bool] = {}

    with _profile_lock(profile_dir) as acquired:
        if not acquired:
            return {p: False for p in platforms}

        try:
            pw, context = launch_browser(account_cfg, profile_dir)
        except Exception as exc:
            logger.error("[profile] Браузер не запустился для verify: %s", exc)
            return {p: False for p in platforms}

        try:
            for platform in platforms:
                # Per-platform URL с UTM → fallback на общий
                expected = prelend_urls.get(platform) or prelend_url
                ok = verify_profile_link(context, platform, expected)
                results[platform] = ok
                _human_pause(2, 4)
        finally:
            close_browser(pw, context)

    return results
