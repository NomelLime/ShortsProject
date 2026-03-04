"""
pipeline/analytics.py — Сбор аналитики после загрузки видео.

Через 24–72 часа после публикации заходит на платформу и собирает:
  - просмотры (views)
  - лайки (likes)
  - комментарии (comments)

Данные сохраняются в data/analytics.json и позволяют понять,
какие теги, ключевые слова и темы дают лучшие результаты.

Структура analytics.json:
  {
    "video_stem": {
      "title": "...",
      "tags": [...],
      "uploads": {
        "youtube": {
          "url":          "https://...",
          "uploaded_at":  "2024-01-15T10:00:00",
          "collected_at": "2024-01-16T12:00:00",
          "views": 1500, "likes": 80, "comments": 12
        },
        "tiktok":    {...},
        "instagram": {...}
      }
    }
  }

Интеграция:
  - Запись о загруженном видео добавляется в analytics.json в uploader.py
    через register_upload().
  - Сбор статистики вызывается из scheduler.py или вручную:
      from pipeline.analytics import collect_pending_analytics
      collect_pending_analytics()
"""

from __future__ import annotations

import json
import logging
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from rebrowser_playwright.sync_api import BrowserContext, Page

from pipeline import config, utils
from pipeline.browser import launch_browser, close_browser
from pipeline.notifications import send_telegram

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Хранилище
# ─────────────────────────────────────────────────────────────────────────────

def _load_analytics() -> Dict:
    if not config.ANALYTICS_FILE.exists():
        return {}
    try:
        return json.loads(config.ANALYTICS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_analytics(data: Dict) -> None:
    try:
        config.ANALYTICS_FILE.parent.mkdir(parents=True, exist_ok=True)
        config.ANALYTICS_FILE.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.error("Не удалось сохранить analytics.json: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Регистрация загрузки
# ─────────────────────────────────────────────────────────────────────────────

def register_upload(
    video_stem: str,
    platform: str,
    video_url: str,
    meta: Dict,
    ab_variant: Optional[str] = None,
) -> None:
    """
    Регистрирует факт загрузки видео в analytics.json.
    ab_variant — метка варианта ("A", "B", ...) для A/B тестирования.
    """
    data  = _load_analytics()
    entry = data.setdefault(video_stem, {
        "title":   meta.get("title", ""),
        "tags":    meta.get("tags", []),
        "uploads": {},
    })

    entry["uploads"][platform] = {
        "url":          video_url,
        "uploaded_at":  datetime.now().isoformat(timespec="seconds"),
        "collected_at": None,
        "views":        None,
        "likes":        None,
        "comments":     None,
        "ab_variant":   ab_variant,
    }

    _save_analytics(data)
    logger.debug("[analytics] Зарегистрирована загрузка: %s / %s (A/B: %s)", video_stem, platform, ab_variant)


def get_pending_collection() -> List[Dict]:
    """
    Возвращает список записей, для которых пора собирать аналитику:
      - загружены > ANALYTICS_COLLECT_AFTER_HOURS назад
      - загружены < ANALYTICS_COLLECT_MAX_HOURS назад (не слишком старые)
      - статистика ещё не собрана (collected_at is None)
    """
    data    = _load_analytics()
    pending = []
    now     = datetime.now()

    for stem, entry in data.items():
        for platform, upload in entry.get("uploads", {}).items():
            if upload.get("collected_at") is not None:
                continue  # уже собрано
            uploaded_at_str = upload.get("uploaded_at")
            if not uploaded_at_str:
                continue
            try:
                uploaded_at = datetime.fromisoformat(uploaded_at_str)
            except Exception:
                continue
            age_hours = (now - uploaded_at).total_seconds() / 3600
            if age_hours < config.ANALYTICS_COLLECT_AFTER_HOURS:
                continue  # ещё рано
            if age_hours > config.ANALYTICS_COLLECT_MAX_HOURS:
                logger.debug("[analytics] Пропуск старой записи: %s/%s (%.0f ч)", stem, platform, age_hours)
                continue  # слишком старое
            pending.append({
                "stem":     stem,
                "platform": platform,
                "url":      upload.get("url", ""),
                "title":    entry.get("title", ""),
                "tags":     entry.get("tags", []),
            })

    return pending


# ─────────────────────────────────────────────────────────────────────────────
# Парсеры статистики по платформам
# ─────────────────────────────────────────────────────────────────────────────

def _collect_youtube_stats(page: Page, video_url: str) -> Optional[Dict]:
    """
    Собирает статистику YouTube через YouTube Studio.
    Если video_url — публичный URL вида youtube.com/shorts/ID или /watch?v=ID,
    конвертирует в URL студии.
    """
    # Извлекаем video_id из URL
    video_id = None
    if "shorts/" in video_url:
        video_id = video_url.split("shorts/")[-1].split("?")[0].split("/")[0]
    elif "watch?v=" in video_url:
        video_id = video_url.split("watch?v=")[-1].split("&")[0]
    elif "youtu.be/" in video_url:
        video_id = video_url.split("youtu.be/")[-1].split("?")[0]

    if not video_id:
        logger.warning("[analytics][youtube] Не удалось извлечь video_id из URL: %s", video_url)
        return None

    studio_url = f"https://studio.youtube.com/video/{video_id}/analytics/tab-overview/period-default"
    logger.info("[analytics][youtube] Собираем статистику: %s", studio_url)

    try:
        page.goto(studio_url, wait_until="domcontentloaded", timeout=30_000)
        time.sleep(random.uniform(3, 5))
    except Exception as exc:
        logger.warning("[analytics][youtube] Не удалось открыть страницу: %s", exc)
        return None

    stats: Dict = {}

    # Просмотры
    try:
        views_el = page.locator(
            "span.style-scope.ytcp-ve:has-text('views'), "
            "[class*='metric-value']:first-of-type, "
            "#primary-metric span"
        ).first
        views_text = views_el.inner_text(timeout=5_000).strip().replace(",", "").replace(" ", "")
        stats["views"] = _parse_number(views_text)
    except Exception:
        stats["views"] = None

    # Лайки — YouTube Studio не всегда показывает напрямую,
    # пробуем через engagement section
    try:
        likes_el = page.locator(
            "[aria-label*='ike'], [class*='likes'] span, "
            "ytd-sentiment-bar-renderer span"
        ).first
        likes_text = likes_el.inner_text(timeout=3_000).strip()
        stats["likes"] = _parse_number(likes_text)
    except Exception:
        stats["likes"] = None

    # Комментарии — из вкладки engagement или публичной страницы
    try:
        comments_el = page.locator(
            "[class*='comment'] [class*='count'], "
            "#comments-count, ytcp-ve[class*='comment']"
        ).first
        comments_text = comments_el.inner_text(timeout=3_000).strip()
        stats["comments"] = _parse_number(comments_text)
    except Exception:
        stats["comments"] = None

    logger.info("[analytics][youtube] Собрано: %s", stats)
    return stats


def _collect_tiktok_stats(page: Page, video_url: str) -> Optional[Dict]:
    """Собирает статистику TikTok из публичной страницы видео."""
    if not video_url:
        return None

    logger.info("[analytics][tiktok] Собираем статистику: %s", video_url)
    try:
        page.goto(video_url, wait_until="domcontentloaded", timeout=30_000)
        time.sleep(random.uniform(3, 5))
    except Exception as exc:
        logger.warning("[analytics][tiktok] Не удалось открыть страницу: %s", exc)
        return None

    stats: Dict = {}

    try:
        views_el = page.locator(
            "[data-e2e='video-views'], strong[data-e2e='browse-video-play-count'], "
            "[class*='video-count']"
        ).first
        stats["views"] = _parse_number(views_el.inner_text(timeout=5_000))
    except Exception:
        stats["views"] = None

    try:
        likes_el = page.locator(
            "[data-e2e='like-count'], strong[data-e2e='browse-like-count']"
        ).first
        stats["likes"] = _parse_number(likes_el.inner_text(timeout=3_000))
    except Exception:
        stats["likes"] = None

    try:
        comments_el = page.locator(
            "[data-e2e='comment-count'], strong[data-e2e='browse-comment-count']"
        ).first
        stats["comments"] = _parse_number(comments_el.inner_text(timeout=3_000))
    except Exception:
        stats["comments"] = None

    logger.info("[analytics][tiktok] Собрано: %s", stats)
    return stats


def _collect_instagram_stats(page: Page, video_url: str) -> Optional[Dict]:
    """Собирает статистику Instagram Reels из публичной страницы поста."""
    if not video_url:
        return None

    logger.info("[analytics][instagram] Собираем статистику: %s", video_url)
    # Мобильный viewport для корректного отображения Instagram
    page.set_viewport_size({"width": 390, "height": 844})

    try:
        page.goto(video_url, wait_until="domcontentloaded", timeout=30_000)
        time.sleep(random.uniform(3, 5))
    except Exception as exc:
        logger.warning("[analytics][instagram] Не удалось открыть страницу: %s", exc)
        return None

    stats: Dict = {}

    try:
        views_el = page.locator(
            "span[class*='view'], [aria-label*='view'], "
            "span:has-text('views')"
        ).first
        stats["views"] = _parse_number(views_el.inner_text(timeout=5_000))
    except Exception:
        stats["views"] = None

    try:
        likes_el = page.locator(
            "section span[class*='like'], "
            "a[href*='liked_by'] span, "
            "[aria-label*='like']"
        ).first
        stats["likes"] = _parse_number(likes_el.inner_text(timeout=3_000))
    except Exception:
        stats["likes"] = None

    try:
        comments_el = page.locator(
            "a[href*='/comments/'] span, "
            "[aria-label*='comment'] span"
        ).first
        stats["comments"] = _parse_number(comments_el.inner_text(timeout=3_000))
    except Exception:
        stats["comments"] = None

    logger.info("[analytics][instagram] Собрано: %s", stats)
    return stats


_PLATFORM_COLLECTORS = {
    "youtube":   _collect_youtube_stats,
    "tiktok":    _collect_tiktok_stats,
    "instagram": _collect_instagram_stats,
}


def _parse_number(text: str) -> Optional[int]:
    """
    Парсит числа вида '1.5K', '2.3M', '150', '1,500'.
    Возвращает None если не удалось распознать.
    """
    if not text:
        return None
    text = text.strip().replace(",", "").replace(" ", "").upper()
    try:
        if text.endswith("K"):
            return int(float(text[:-1]) * 1_000)
        if text.endswith("M"):
            return int(float(text[:-1]) * 1_000_000)
        if text.endswith("B"):
            return int(float(text[:-1]) * 1_000_000_000)
        return int(float(text))
    except (ValueError, TypeError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Поиск аккаунта для платформы
# ─────────────────────────────────────────────────────────────────────────────

def _find_account_for_platform(platform: str) -> Optional[Dict]:
    """
    Возвращает первый аккаунт, у которого есть данная платформа.
    Используется для открытия браузера при сборе аналитики.
    """
    accounts = utils.get_all_accounts()
    for acc in accounts:
        if platform in acc.get("platforms", []):
            return acc
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Основная точка входа
# ─────────────────────────────────────────────────────────────────────────────

def collect_pending_analytics(dry_run: bool = False) -> int:
    """
    Собирает аналитику для всех видео, у которых подошло время сбора.

    Возвращает количество успешно обработанных записей.
    Результаты записываются в data/analytics.json.
    """
    pending = get_pending_collection()

    if not pending:
        logger.info("[analytics] Нет записей для сбора аналитики.")
        return 0

    logger.info("[analytics] Записей к обработке: %d", len(pending))

    if dry_run:
        for item in pending:
            logger.info("[dry_run][analytics] %s / %s — %s", item["stem"], item["platform"], item["url"])
        return len(pending)

    # Группируем по платформе — открываем один браузер на платформу
    by_platform: Dict[str, List[Dict]] = {}
    for item in pending:
        by_platform.setdefault(item["platform"], []).append(item)

    data    = _load_analytics()
    success = 0

    for platform, items in by_platform.items():
        collector_fn = _PLATFORM_COLLECTORS.get(platform)
        if not collector_fn:
            logger.warning("[analytics] Нет коллектора для платформы: %s", platform)
            continue

        account = _find_account_for_platform(platform)
        if not account:
            logger.warning("[analytics][%s] Нет аккаунта — пропуск.", platform)
            continue

        profile_dir = account["dir"] / "browser_profile"
        try:
            pw, context = launch_browser(account["config"], profile_dir)
        except RuntimeError as exc:
            logger.error("[analytics][%s] Прокси недоступен: %s", platform, exc)
            continue

        try:
            for item in items:
                stem = item["stem"]
                url  = item["url"]

                if not url:
                    logger.warning(
                        "[analytics][%s] URL не указан для %s — пропуск.", platform, stem
                    )
                    continue

                page = context.new_page()
                try:
                    stats = collector_fn(page, url)
                finally:
                    try:
                        page.close()
                    except Exception:
                        pass

                if stats is None:
                    logger.warning("[analytics][%s] Не удалось собрать данные для %s", platform, stem)
                    continue

                # Сохраняем в analytics.json
                if stem in data and platform in data[stem].get("uploads", {}):
                    data[stem]["uploads"][platform].update({
                        "collected_at": datetime.now().isoformat(timespec="seconds"),
                        "views":    stats.get("views"),
                        "likes":    stats.get("likes"),
                        "comments": stats.get("comments"),
                    })
                    _save_analytics(data)
                    success += 1
                    logger.info(
                        "[analytics][%s] %s — 👁 %s | 👍 %s | 💬 %s",
                        platform, stem,
                        stats.get("views"), stats.get("likes"), stats.get("comments"),
                    )

                time.sleep(random.uniform(3, 7))

        finally:
            close_browser(pw, context)

    if success:
        _send_analytics_report(data, success)

    return success


def _send_analytics_report(data: Dict, collected: int) -> None:
    """Отправляет краткий отчёт об аналитике в Telegram."""
    lines = [f"📊 <b>Аналитика собрана:</b> {collected} записей\n"]

    # Топ-5 по просмотрам
    all_records = []
    for stem, entry in data.items():
        total_views = sum(
            u.get("views") or 0
            for u in entry.get("uploads", {}).values()
            if u.get("views") is not None
        )
        if total_views > 0:
            all_records.append((total_views, entry.get("title") or stem, entry.get("tags", [])))

    all_records.sort(reverse=True)

    if all_records:
        lines.append("🏆 <b>Топ по просмотрам:</b>")
        for views, title, tags in all_records[:5]:
            tag_str = ", ".join(f"#{t}" for t in tags[:3])
            lines.append(f"  • <b>{title[:40]}</b> — {views:,} 👁  {tag_str}")

    send_telegram("\n".join(lines))


# ─────────────────────────────────────────────────────────────────────────────
# A/B тестирование (фича B)
# ─────────────────────────────────────────────────────────────────────────────

def assign_ab_variants(video_stem: str, meta_variants: List[Dict]) -> None:
    """
    Назначает A/B варианты метаданных видео в analytics.json.
    Вызывается из distributor.py при распределении видео на несколько аккаунтов
    одной платформы — каждый аккаунт получает свой вариант.

    Структура в analytics.json:
      "ab_test": {
        "youtube": {
          "A": {"title": "...", "tags": [...]},
          "B": {"title": "...", "tags": [...]}
        }
      }
    """
    if not config.AB_TEST_ENABLED or not meta_variants:
        return

    labels = [chr(ord("A") + i) for i in range(len(meta_variants))]
    data   = _load_analytics()
    entry  = data.setdefault(video_stem, {"title": "", "tags": [], "uploads": {}})
    ab     = entry.setdefault("ab_test", {})

    # Сохраняем варианты (не привязываем к платформе — платформ может быть несколько)
    for label, meta in zip(labels, meta_variants):
        ab[label] = {
            "title": meta.get("title", ""),
            "tags":  meta.get("tags", []),
            "description": meta.get("description", ""),
        }

    _save_analytics(data)
    logger.info("[ab_test] Назначено %d вариантов для %s", len(labels), video_stem)


def get_ab_meta_for_account(video_stem: str, platform: str, account_index: int) -> Optional[Dict]:
    """
    Возвращает метаданные для конкретного аккаунта (по индексу) в рамках A/B теста.
    account_index=0 → вариант A, 1 → B, 2 → C и т.д.
    Возвращает None если A/B не настроен для этого видео.
    """
    data  = _load_analytics()
    entry = data.get(video_stem, {})
    ab    = entry.get("ab_test")
    if not ab:
        return None
    labels = sorted(ab.keys())
    label  = labels[account_index % len(labels)]
    meta   = dict(ab[label])
    meta["ab_variant"] = label
    return meta


def compare_ab_results() -> List[Dict]:
    """
    Сравнивает результаты A/B тестов для всех видео, у которых накопились данные.
    Определяет победителя по средним просмотрам.
    Отправляет отчёт в Telegram.
    Возвращает список результатов.
    """
    if not config.AB_TEST_ENABLED:
        return []

    data    = _load_analytics()
    results = []
    now     = datetime.now()

    for stem, entry in data.items():
        ab = entry.get("ab_test")
        if not ab:
            continue

        # Собираем статистику по вариантам
        variant_stats: dict = {}
        for platform, upload in entry.get("uploads", {}).items():
            variant = upload.get("ab_variant")
            views   = upload.get("views")
            if not variant or views is None:
                continue

            # Проверяем что прошло достаточно времени
            uploaded_at = upload.get("uploaded_at")
            if uploaded_at:
                try:
                    age_h = (now - datetime.fromisoformat(uploaded_at)).total_seconds() / 3600
                    if age_h < config.AB_TEST_COMPARE_AFTER_H:
                        continue
                except Exception:
                    pass

            variant_stats.setdefault(variant, []).append(views)

        if len(variant_stats) < 2:
            continue  # нет данных для сравнения

        avg_by_variant = {v: sum(vs) / len(vs) for v, vs in variant_stats.items()}
        winner         = max(avg_by_variant, key=avg_by_variant.get)

        result = {
            "stem":            stem,
            "title":           entry.get("title", stem),
            "winner":          winner,
            "winner_avg_views": avg_by_variant[winner],
            "variants":        avg_by_variant,
            "winner_meta":     ab.get(winner, {}),
        }
        results.append(result)
        logger.info(
            "[ab_test] %s — победитель: %s (%.0f просмотров vs %s)",
            stem, winner, avg_by_variant[winner],
            {k: f"{v:.0f}" for k, v in avg_by_variant.items() if k != winner},
        )

    if results:
        _send_ab_report(results)

    return results


def _send_ab_report(results: List[Dict]) -> None:
    lines = [f"🧪 <b>A/B тест — итоги ({len(results)} видео):</b>\n"]
    for r in results[:10]:
        variants_str = " | ".join(
            f"{'🏆' if k == r['winner'] else '  '}{k}: {v:,.0f} 👁"
            for k, v in sorted(r["variants"].items())
        )
        lines.append(f"• <b>{r['title'][:35]}</b>\n  {variants_str}")
        w_meta = r.get("winner_meta", {})
        if w_meta.get("title"):
            lines.append(f"  → Лучший заголовок: <i>{w_meta['title'][:50]}</i>")
    send_telegram("\n".join(lines))


# ─────────────────────────────────────────────────────────────────────────────
# Авто-репост слабых видео (фича A)
# ─────────────────────────────────────────────────────────────────────────────

def get_repost_candidates() -> List[Dict]:
    """
    Возвращает список видео-кандидатов на репост:
      - просмотров < REPOST_MIN_VIEWS
      - прошло > REPOST_AFTER_HOURS с момента публикации
      - количество попыток репоста < REPOST_MAX_ATTEMPTS

    Возвращает список dict с ключами: stem, platform, original_meta, repost_attempt.
    """
    if not config.REPOST_ENABLED:
        return []

    data       = _load_analytics()
    candidates = []
    now        = datetime.now()

    for stem, entry in data.items():
        for platform, upload in entry.get("uploads", {}).items():
            views       = upload.get("views")
            uploaded_at = upload.get("uploaded_at")
            if views is None or not uploaded_at:
                continue
            try:
                age_h = (now - datetime.fromisoformat(uploaded_at)).total_seconds() / 3600
            except Exception:
                continue

            if age_h < config.REPOST_AFTER_HOURS:
                continue
            if views >= config.REPOST_MIN_VIEWS:
                continue

            attempts = upload.get("repost_attempts", 0)
            if attempts >= config.REPOST_MAX_ATTEMPTS:
                continue

            # Ищем исходный архив видео
            archive_path = _find_archived_video(stem)
            if not archive_path:
                continue

            candidates.append({
                "stem":           stem,
                "platform":       platform,
                "archive_path":   archive_path,
                "original_meta":  {
                    "title":       entry.get("title", ""),
                    "tags":        entry.get("tags", []),
                    "description": entry.get("title", ""),
                },
                "repost_attempt": attempts + 1,
                "original_views": views,
            })

    if candidates:
        logger.info("[repost] Кандидатов на репост: %d", len(candidates))

    return candidates


def mark_repost_queued(video_stem: str, platform: str) -> None:
    """Увеличивает счётчик попыток репоста в analytics.json."""
    data  = _load_analytics()
    entry = data.get(video_stem, {})
    if platform in entry.get("uploads", {}):
        entry["uploads"][platform]["repost_attempts"] = \
            entry["uploads"][platform].get("repost_attempts", 0) + 1
        _save_analytics(data)



def _make_unique_variant(src: Path, out_dir: Path) -> Optional[Path]:
    """
    Создаёт уникализированную версию видео для репоста через ffmpeg.

    Применяет лёгкие вариации, чтобы платформы не определили повтор:
      - случайная скорость ±3%
      - случайная яркость/контраст
      - горизонтальный флип (50%)
      - случайный шум
    Лёгче полного клонера: работает в одном процессе, нет фона/музыки.
    Возвращает путь к новому файлу или None при ошибке.
    """
    import subprocess as _sp, random as _rnd, tempfile as _tmp

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"_unique_{src.stem}.mp4"

    speed      = _rnd.uniform(0.97, 1.03)
    brightness = _rnd.uniform(-0.03, 0.03)
    contrast   = _rnd.uniform(0.96, 1.04)
    saturation = _rnd.uniform(0.90, 1.10)
    hue        = _rnd.uniform(-8.0, 8.0)
    noise      = _rnd.randint(3, 8)
    do_hflip   = _rnd.random() < 0.5

    vf_parts = [
        f"setpts={1/speed:.4f}*PTS",
        f"eq=brightness={brightness:.4f}:contrast={contrast:.4f}:saturation={saturation:.4f}",
        f"hue=h={hue:.2f}",
        f"noise=alls={noise}:allf=t",
    ]
    if do_hflip:
        vf_parts.append("hflip")

    cmd = [
        "ffmpeg", "-y", "-i", str(src),
        "-vf", ",".join(vf_parts),
        "-af", f"atempo={speed:.4f}",
        "-c:v", "libx264", "-crf", "23",
        "-c:a", "aac", "-b:a", "192k",
        "-map_metadata", "-1",
        str(out_path),
    ]

    try:
        _sp.run(cmd, check=True, capture_output=True, timeout=300)
        logger.debug("[repost] Уникализировано: %s -> %s", src.name, out_path.name)
        return out_path
    except Exception as exc:
        logger.warning("[repost] Уникализация не удалась (%s) — используем оригинал", exc)
        if out_path.exists():
            out_path.unlink(missing_ok=True)
        return None


def _find_archived_video(stem: str) -> Optional[Path]:
    """Ищет архивированное видео по stem во всех папках архива."""
    archive_root = config.ARCHIVE_DIR
    if not archive_root.exists():
        return None
    for video_path in archive_root.rglob(f"{stem}*.mp4"):
        return video_path
    return None


def queue_reposts(dry_run: bool = False) -> int:
    """
    Добавляет слабые видео в очереди загрузки с изменёнными тегами.
    Возвращает количество поставленных в очередь видео.
    """
    from pipeline import utils as _utils

    candidates = get_repost_candidates()
    if not candidates:
        return 0

    accounts = _utils.get_all_accounts()
    queued   = 0

    for candidate in candidates:
        platform      = candidate["platform"]
        archive_path  = candidate["archive_path"]
        original_meta = candidate["original_meta"]
        stem          = candidate["stem"]

        # Ротируем теги: переставляем порядок для «свежести»
        tags = original_meta.get("tags", [])
        if len(tags) > 3:
            import random as _r
            suffix = _r.sample(tags, min(5, len(tags)))
            tags   = suffix + [t for t in tags if t not in suffix]

        repost_meta = dict(original_meta)
        repost_meta["tags"]  = tags
        repost_meta["title"] = f"{original_meta.get('title', '')} 🔁".strip()

        # Находим аккаунты для этой платформы
        target_accounts = [a for a in accounts if platform in a.get("platforms", [])]
        if not target_accounts:
            continue

        import random as _r
        acc = _r.choice(target_accounts)

        queue_dir = acc["dir"] / "upload_queue" / platform
        queue_dir.mkdir(parents=True, exist_ok=True)

        dest_video = queue_dir / f"repost_{archive_path.name}"
        dest_meta  = queue_dir / f"repost_{archive_path.stem}_meta.json"

        if dest_video.exists():
            continue  # уже в очереди

        if dry_run:
            logger.info("[repost][dry_run] %s -> %s/%s", archive_path.name, acc["name"], platform)
        else:
            try:
                import shutil as _sh, json as _j
                # Уникализация: прогоняем через лёгкую вариацию (ffmpeg)
                # чтобы платформа не определила повторную загрузку того же контента
                unique_path = _make_unique_variant(archive_path, dest_video.parent)
                if unique_path:
                    _sh.move(str(unique_path), str(dest_video))
                else:
                    _sh.copy2(archive_path, dest_video)

                dest_meta.write_text(
                    _j.dumps(repost_meta, ensure_ascii=False, indent=2), encoding="utf-8"
                )
                mark_repost_queued(stem, platform)
                queued += 1
                logger.info(
                    "[repost] ✅ %s -> %s/%s (было %d просмотров, уникализировано: %s)",
                    archive_path.name, acc["name"], platform,
                    candidate["original_views"], unique_path is not None,
                )
            except Exception as exc:
                logger.error("[repost] Ошибка: %s", exc)

    if queued:
        send_telegram(
            f"🔁 <b>Авто-репост:</b> поставлено в очередь {queued} видео\n"
            f"(просмотров < {config.REPOST_MIN_VIEWS} за {config.REPOST_AFTER_HOURS} ч)"
        )

    return queued
