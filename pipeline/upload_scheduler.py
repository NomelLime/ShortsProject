"""
pipeline/upload_scheduler.py — Планировщик публикаций по расписанию.

Позволяет задать время загрузки для каждого аккаунта в config.json:

  {
    "platforms": ["youtube", "tiktok"],
    "upload_schedule": {
      "youtube":   ["09:00", "19:00"],
      "tiktok":    ["10:00", "20:00"],
      "instagram": ["11:00", "21:00"]
    }
  }

Если upload_schedule не задан — используется глобальное расписание
из переменной окружения UPLOAD_TIMES (формат "09:00,19:00")
или дефолтное из DEFAULT_UPLOAD_TIMES в config.

Запуск (вместо run_pipeline.py для непрерывной работы):

    python run_scheduled.py

Этот файл только отвечает за тайминг. Когда наступает нужное время —
запускает полный пайплайн (или только этап загрузки).

Интеграция с фоновыми процессами:
  - ActivityScheduler (scheduler.py) — независимый, управляет прогревом
  - SessionHealthMonitor (session_manager.py) — независимый, следит за cookies
  - UploadScheduler (этот файл) — управляет временем публикации
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from pipeline import config, utils
from pipeline.content_locale import resolve_content_locale_for_account
from pipeline.notifications import send_telegram

logger = logging.getLogger(__name__)

# Дефолтные времена публикации если не заданы в конфиге аккаунта
# Можно переопределить через env: UPLOAD_TIMES="09:00,12:00,19:00"
_DEFAULT_UPLOAD_TIMES_STR = os.getenv("UPLOAD_TIMES", "09:00,19:00")
DEFAULT_UPLOAD_TIMES: List[str] = [
    t.strip() for t in _DEFAULT_UPLOAD_TIMES_STR.split(",") if t.strip()
]

# Дефолтные прайм-тайм окна по локали аккаунта (если нет кастомного и smart-данных).
_LOCALE_PRIME_TIMES: Dict[str, List[str]] = {
    "ru-RU": ["18:00", "21:00"],
    "en-US": ["12:00", "19:00"],
    "en-GB": ["18:00", "21:00"],
    "es-ES": ["19:00", "22:00"],
    "es-419": ["19:00", "22:00"],
    "pt-BR": ["18:00", "21:00"],
    "pt-PT": ["19:00", "22:00"],
    "de-DE": ["18:00", "21:00"],
    "fr-FR": ["18:00", "21:00"],
}


# ─────────────────────────────────────────────────────────────────────────────
# Утилиты
# ─────────────────────────────────────────────────────────────────────────────

def _parse_time(time_str: str) -> Optional[Tuple[int, int]]:
    """Парсит строку "HH:MM" → (hour, minute) или None при ошибке."""
    try:
        parts = time_str.strip().split(":")
        return int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        logger.warning("[upload_scheduler] Невалидное время: %r", time_str)
        return None


def _seconds_until(hour: int, minute: int) -> float:
    """Возвращает секунды до следующего наступления HH:MM (сегодня или завтра)."""
    now = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def _next_upload_delay(times: List[str], jitter_sec: int = 120) -> Tuple[float, str]:
    """
    Вычисляет задержку до ближайшего времени из списка.
    Добавляет случайный jitter ±jitter_sec для натуральности.
    Возвращает (delay_seconds, time_str).
    """
    parsed = [_parse_time(t) for t in times]
    parsed = [(h, m, t) for (h, m), t in zip(  # type: ignore[misc]
        [p for p in parsed if p], times
    ) if _parse_time(t)]

    if not parsed:
        return 3600.0, "??:??"  # fallback — раз в час

    best_delay = None
    best_time  = None

    for h, m, t in parsed:
        delay = _seconds_until(h, m)
        if best_delay is None or delay < best_delay:
            best_delay = delay
            best_time  = t

    jitter = random.uniform(-jitter_sec, jitter_sec)
    return max(60.0, best_delay + jitter), best_time


def get_account_upload_times(account_cfg: Dict, platform: str) -> List[str]:
    """
    Возвращает список времён загрузки для аккаунта и платформы.

    Приоритет:
      1. account_cfg["upload_schedule"][platform] — явное расписание в конфиге
      2. Умное расписание из analytics.json (если SMART_SCHEDULE_ENABLED)
      3. DEFAULT_UPLOAD_TIMES — глобальный дефолт
    """
    # 1. Явное расписание в конфиге аккаунта
    schedule = account_cfg.get("upload_schedule", {})
    if isinstance(schedule, dict) and platform in schedule:
        times = schedule[platform]
        if isinstance(times, list) and times:
            return times
    if isinstance(schedule, list) and schedule:
        return schedule

    # 2. Умное расписание из аналитики
    if config.SMART_SCHEDULE_ENABLED:
        smart = _get_smart_upload_times(platform)
        if smart:
            return smart

    # 3. Базовый prime-time по locale аккаунта (полная локаль -> язык).
    try:
        loc = resolve_content_locale_for_account(account_cfg or {})
        if loc in _LOCALE_PRIME_TIMES:
            return _LOCALE_PRIME_TIMES[loc]
        base = (loc or "").split("-")[0].lower()
        for k, times in _LOCALE_PRIME_TIMES.items():
            if k.lower().split("-")[0] == base:
                return times
    except Exception:
        pass

    # 4. Глобальный fallback.
    return DEFAULT_UPLOAD_TIMES


def _get_smart_upload_times(platform: str) -> List[str]:
    """
    Анализирует analytics.json и возвращает 2 лучших часа публикации
    для данной платформы на основе средних просмотров.

    Требует минимум SMART_SCHEDULE_MIN_SAMPLES записей.
    Возвращает пустой список если данных недостаточно.
    """
    if not config.ANALYTICS_FILE.exists():
        return []

    try:
        import json as _json
        data = _json.loads(config.ANALYTICS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []

    # Собираем: час публикации → список просмотров
    from collections import defaultdict
    from datetime import datetime as _dt

    hour_views: dict = defaultdict(list)

    for entry in data.values():
        upload = entry.get("uploads", {}).get(platform)
        if not upload:
            continue
        views       = upload.get("views")
        uploaded_at = upload.get("uploaded_at")
        if views is None or not uploaded_at:
            continue
        try:
            hour = _dt.fromisoformat(uploaded_at).hour
            hour_views[hour].append(views)
        except Exception:
            continue

    if not hour_views:
        return []

    # Нужен минимум MIN_SAMPLES точек данных суммарно
    total_samples = sum(len(v) for v in hour_views.values())
    if total_samples < config.SMART_SCHEDULE_MIN_SAMPLES:
        logger.debug(
            "[smart_schedule][%s] Данных недостаточно: %d / %d",
            platform, total_samples, config.SMART_SCHEDULE_MIN_SAMPLES,
        )
        return []

    # Средние просмотры по часам, сортируем по убыванию
    avg_by_hour = {h: sum(v) / len(v) for h, v in hour_views.items()}
    best_hours  = sorted(avg_by_hour, key=avg_by_hour.get, reverse=True)[:2]
    best_hours.sort()  # хронологически

    times = [f"{h:02d}:00" for h in best_hours]
    logger.info(
        "[smart_schedule][%s] Лучшие часы публикации: %s (из %d образцов)",
        platform, ", ".join(times), total_samples,
    )
    return times


# ─────────────────────────────────────────────────────────────────────────────
# Job на один аккаунт × платформа
# ─────────────────────────────────────────────────────────────────────────────

class _UploadJob:
    """
    Запускает загрузку для одного аккаунта / платформы в нужное время.
    После выполнения сам себя перепланирует на следующий слот.
    """

    def __init__(self, account: Dict, platform: str) -> None:
        self._account  = account
        self._platform = platform
        self._timer: Optional[threading.Timer] = None
        self._running  = False
        self._lock     = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._running = True
        self._schedule_next()

    def stop(self) -> None:
        with self._lock:
            self._running = False
            if self._timer:
                self._timer.cancel()
                self._timer = None

    def _schedule_next(self) -> None:
        times = get_account_upload_times(self._account["config"], self._platform)
        delay, time_str = _next_upload_delay(times)

        logger.info(
            "[upload_scheduler] [%s][%s] Следующая загрузка в ~%s (через %.0f мин)",
            self._account["name"], self._platform, time_str, delay / 60,
        )

        with self._lock:
            if not self._running:
                return
            self._timer = threading.Timer(delay, self._run)
            self._timer.daemon = True
            self._timer.start()

    def _run(self) -> None:
        acc_name  = self._account["name"]
        platform  = self._platform

        logger.info(
            "[upload_scheduler] ⏰ Время публикации! [%s][%s]",
            acc_name, platform,
        )
        send_telegram(f"⏰ <b>[{acc_name}][{platform}]</b> Запуск загрузки по расписанию.")

        try:
            # Запускаем только этап загрузки для конкретного аккаунта и платформы
            _run_upload_for(self._account, platform)
        except Exception as exc:
            logger.error(
                "[upload_scheduler] [%s][%s] Ошибка при загрузке: %s",
                acc_name, platform, exc, exc_info=True,
            )
            send_telegram(
                f"❌ [{acc_name}][{platform}] Ошибка загрузки по расписанию: {str(exc)[:200]}"
            )

        # Перепланируем на следующий слот
        with self._lock:
            if self._running:
                self._schedule_next()


def _run_upload_for(account: Dict, platform: str) -> None:
    """
    Запускает загрузку очереди для конкретного аккаунта и платформы.

    Fix #5: если очередь пуста — ждём до QUEUE_WAIT_MAX_MIN минут,
    повторно проверяя каждые QUEUE_WAIT_INTERVAL_SEC секунд.
    Это решает гонку когда пайплайн подготовки ещё не завершился
    в момент наступления времени публикации.
    """
    from pipeline.browser import launch_browser, close_browser
    from pipeline.uploader import upload_video, clean_video_metadata
    from pipeline.locale_packaging import prepare_locale_pack_for_upload
    from pipeline.activity import run_activity
    from pipeline.session_manager import ensure_session_fresh, mark_session_verified
    from pipeline.analytics import register_upload
    from pipeline.quarantine import is_quarantined, mark_error as q_err, mark_success as q_ok
    from pathlib import Path

    QUEUE_WAIT_MAX_MIN      = 30   # максимум ждём 30 мин
    QUEUE_WAIT_INTERVAL_SEC = 60   # проверяем каждые 60 сек

    acc_name    = account["name"]
    acc_dir     = account["dir"]
    acc_cfg     = account["config"]
    daily_limit = config.PLATFORM_DAILY_LIMITS.get(platform, config.DAILY_UPLOAD_LIMIT)

    # Fix #5: ждём пока очередь не заполнится
    queue = utils.get_upload_queue(acc_dir, platform)
    if not queue:
        deadline = time.time() + QUEUE_WAIT_MAX_MIN * 60
        logger.info(
            "[upload_scheduler] [%s][%s] Очередь пуста — ждём до %d мин...",
            acc_name, platform, QUEUE_WAIT_MAX_MIN,
        )
        while time.time() < deadline:
            time.sleep(QUEUE_WAIT_INTERVAL_SEC)
            queue = utils.get_upload_queue(acc_dir, platform)
            if queue:
                logger.info("[upload_scheduler] [%s][%s] Очередь готова (%d видео).", acc_name, platform, len(queue))
                break
        else:
            logger.warning("[upload_scheduler] [%s][%s] Очередь так и не появилась — пропуск.", acc_name, platform)
            send_telegram(f"⏳ [{acc_name}][{platform}] Очередь пуста через {QUEUE_WAIT_MAX_MIN} мин — загрузка отложена.")
            return

    if utils.get_uploads_today(acc_dir) >= daily_limit:
        logger.info("[upload_scheduler] [%s][%s] Дневной лимит исчерпан.", acc_name, platform)
        return

    if is_quarantined(acc_name, platform):
        logger.info("[upload_scheduler] [%s][%s] Аккаунт в карантине — пропуск.", acc_name, platform)
        return

    from pipeline.upload_warmup import is_upload_blocked

    wb, wr = is_upload_blocked(acc_name, platform)
    if wb:
        logger.info("[upload_scheduler] [%s][%s] Прогрев — заливка отложена (%s).", acc_name, platform, wr)
        return

    profile_dir = acc_dir / "browser_profile"
    try:
        pw, context = launch_browser(acc_cfg, profile_dir, platform=platform)
    except RuntimeError as exc:
        logger.error("[upload_scheduler] [%s] Прокси недоступен: %s", acc_name, exc)
        send_telegram(f"⚠️ [{acc_name}][{platform}] Прокси недоступен — загрузка пропущена.")
        q_err(acc_name, platform, reason="proxy_unavailable")
        return

    try:
        if not ensure_session_fresh(context, acc_name, platform):
            logger.error("[upload_scheduler] [%s][%s] Сессия невалидна.", acc_name, platform)
            return
        mark_session_verified(acc_name, platform, valid=True)

        from pipeline.upload_warmup import is_upload_warmup_active

        w_active, w_msg = is_upload_warmup_active(acc_dir, platform, acc_cfg)
        if w_active:
            logger.info(
                "[upload_scheduler] [%s][%s] После входа — только активность, без заливки (%s)",
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
            return

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
            # A/B: берём назначенный вариант если есть
            meta       = item.get("ab_meta") or item["meta"]

            localized_video, localized_meta = prepare_locale_pack_for_upload(
                video_path=Path(video_path),
                base_meta=dict(meta or {}),
                account_cfg=acc_cfg,
                platform=platform,
            )
            clean_path = clean_video_metadata(localized_video)

            video_url = upload_video(
                context, platform, clean_path, localized_meta,
                account_name=acc_name, account_cfg=acc_cfg,
            )

            if video_url is not None:
                utils.mark_uploaded(item)
                utils.increment_upload_count(acc_dir)
                q_ok(acc_name, platform)
                register_upload(
                    video_stem=Path(video_path).stem,
                    platform=platform,
                    video_url=video_url,
                    meta=localized_meta,
                    ab_variant=localized_meta.get("ab_variant"),
                )
                logger.info("[upload_scheduler] ✅ [%s][%s] %s", acc_name, platform, video_path.name)
            else:
                q_err(acc_name, platform, reason="upload_failed")
    finally:
        close_browser(pw, context)


# ─────────────────────────────────────────────────────────────────────────────
# Публичный планировщик
# ─────────────────────────────────────────────────────────────────────────────

class UploadScheduler:
    """
    Планировщик загрузок по расписанию для всех аккаунтов.

    Создаёт по одному _UploadJob на каждую пару (аккаунт, платформа).
    Каждый job самостоятельно вычисляет время до следующего слота
    из config.json аккаунта или из DEFAULT_UPLOAD_TIMES.

    Использование как контекстный менеджер (в run_scheduled.py):
        with UploadScheduler():
            # держим процесс живым
            while True:
                time.sleep(60)

    Или явно:
        s = UploadScheduler()
        s.start()
        ...
        s.stop()
    """

    def __init__(self) -> None:
        self._jobs: List[_UploadJob] = []
        self._started = False

    def start(self) -> None:
        if self._started:
            return

        accounts = utils.get_all_accounts()
        if not accounts:
            logger.warning("[upload_scheduler] Аккаунты не найдены — планировщик не запущен.")
            return

        for account in accounts:
            for platform in account["platforms"]:
                job = _UploadJob(account=account, platform=platform)
                self._jobs.append(job)
                job.start()

        self._started = True
        logger.info(
            "[upload_scheduler] Запущено %d job(s) для %d аккаунтов. "
            "Дефолтные времена: %s",
            len(self._jobs), len(accounts), ", ".join(DEFAULT_UPLOAD_TIMES),
        )
        send_telegram(
            f"🗓 Планировщик загрузок запущен.\n"
            f"Аккаунтов: {len(accounts)} | Job-ов: {len(self._jobs)}\n"
            f"Дефолтные слоты: {', '.join(DEFAULT_UPLOAD_TIMES)}"
        )

    def stop(self) -> None:
        for job in self._jobs:
            job.stop()
        self._jobs.clear()
        self._started = False
        logger.info("[upload_scheduler] Остановлен.")

    def __enter__(self) -> "UploadScheduler":
        self.start()
        return self

    def __exit__(self, *_) -> None:
        self.stop()
