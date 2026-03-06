"""
pipeline/agents/publisher.py — PUBLISHER: умная загрузка на платформы.

Полный цикл загрузки с координацией:
  — GUARDIAN    → is_account_safe(), get_safe_delay(), report_*()
  — ACCOUNTANT  → is_limit_reached(), get_available_accounts()
  — uploader.py → upload_all(), upload_video()

Особенности:
  1. Параллельная загрузка нескольких аккаунтов (ThreadPoolExecutor)
  2. Умная очередь: сначала аккаунты не в карантине и не на лимите
  3. Антибан задержки через Guardian.get_safe_delay()
  4. Retry с exponential backoff (встроен в upload_video)
  5. Подробная статистика в AgentMemory
  6. Поддержка dry_run для тестирования
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

_UPLOAD_INTERVAL    = 1800  # 30 минут между циклами
_MAX_PARALLEL       = 3     # максимум параллельных браузеров (RAM ограничение)
_MIN_QUEUE_INTERVAL = 5     # минимум сек между проверкой очереди


class Publisher(BaseAgent):
    """
    Менеджер загрузки видео.

    Получает задачи из очередей аккаунтов (utils.get_upload_queue),
    проверяет безопасность через Guardian, соблюдает лимиты через Accountant,
    загружает через uploader.upload_all() или собственным параллельным циклом.
    """

    def __init__(
        self,
        memory: Optional[AgentMemory] = None,
        notify: Any = None,
        guardian: Any = None,
        accountant: Any = None,
    ) -> None:
        super().__init__("PUBLISHER", memory or get_memory(), notify)
        self._guardian   = guardian
        self._accountant = accountant
        self._uploaded   = 0
        self._failed     = 0
        self._skipped    = 0
        self._session_start = datetime.now()

    # ------------------------------------------------------------------
    # run() — главный цикл
    # ------------------------------------------------------------------

    def run(self) -> None:
        logger.info("[PUBLISHER] Запущен, интервал=%ds", _UPLOAD_INTERVAL)
        while not self.should_stop:
            self._upload_cycle()
            if not self.sleep(_UPLOAD_INTERVAL):
                break

    # ------------------------------------------------------------------
    # Цикл загрузки
    # ------------------------------------------------------------------

    def _upload_cycle(self) -> None:
        self._set_status(AgentStatus.RUNNING, "сканирование очередей")
        try:
            from pipeline.utils import get_all_accounts, get_upload_queue

            accounts = get_all_accounts()
            if not accounts:
                logger.debug("[PUBLISHER] Нет аккаунтов")
                self._set_status(AgentStatus.IDLE)
                return

            # Строим список задач: [(account, platform, items), ...]
            tasks = self._build_task_list(accounts)
            if not tasks:
                logger.debug("[PUBLISHER] Все очереди пусты или заблокированы")
                self._set_status(AgentStatus.IDLE)
                return

            logger.info("[PUBLISHER] Задач загрузки: %d", len(tasks))
            self._set_status(AgentStatus.RUNNING, f"загрузка ({len(tasks)} задач)")

            # Параллельная загрузка
            results = self._run_parallel(tasks)
            self._process_results(results)

        except Exception as e:
            logger.error("[PUBLISHER] Ошибка цикла: %s", e)
            self._set_status(AgentStatus.ERROR, str(e))
            raise
        finally:
            if self.status not in (AgentStatus.ERROR,):
                self._set_status(AgentStatus.IDLE)

    # ------------------------------------------------------------------
    # Построение очереди задач
    # ------------------------------------------------------------------

    def _build_task_list(self, accounts: List[Dict]) -> List[Dict]:
        """
        Формирует список задач с приоритизацией:
          1. Пропускаем аккаунты в карантине
          2. Пропускаем аккаунты на дневном лимите
          3. Сортируем: меньше загрузок сегодня → выше приоритет
        """
        from pipeline.utils import get_upload_queue, get_uploads_today
        from pipeline import config

        tasks = []
        for acc in accounts:
            acc_name  = acc["name"]
            acc_dir   = Path(acc["dir"])
            platforms = acc.get("platforms", [])

            uploads_today = get_uploads_today(acc_dir)

            for platform in platforms:
                # Проверка Guardian (карантин)
                if self._guardian:
                    safe, reason = self._guardian.is_account_safe(acc_name, platform)
                    if not safe:
                        logger.debug("[PUBLISHER] Пропуск %s/%s: %s", acc_name, platform, reason)
                        self._skipped += 1
                        continue

                # Проверка лимита (Accountant или напрямую)
                if self._is_at_limit(acc_dir, platform, uploads_today):
                    logger.debug("[PUBLISHER] %s/%s на дневном лимите", acc_name, platform)
                    continue

                # Очередь видео
                queue = get_upload_queue(acc_dir, platform)
                if not queue:
                    continue

                tasks.append({
                    "account":      acc,
                    "acc_name":     acc_name,
                    "acc_dir":      acc_dir,
                    "acc_cfg":      acc.get("config", {}),
                    "platform":     platform,
                    "queue":        queue,
                    "uploads_today": uploads_today,
                    "priority":     uploads_today,  # меньше = выше приоритет
                })

        # Сортируем: аккаунты с наименьшим числом загрузок сегодня — первые
        tasks.sort(key=lambda t: t["priority"])
        return tasks

    def _is_at_limit(self, acc_dir: Path, platform: str, uploads_today: int) -> bool:
        """Проверяет дневной лимит через Accountant или напрямую."""
        if self._accountant:
            try:
                from pipeline.utils import get_all_accounts
                # Ищем имя аккаунта по директории
                for acc in get_all_accounts():
                    if Path(acc["dir"]) == acc_dir:
                        return self._accountant.is_limit_reached(acc["name"], platform)
            except Exception:
                pass

        # Прямая проверка
        try:
            from pipeline.utils import is_daily_limit_reached
            return is_daily_limit_reached(acc_dir)
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Параллельная загрузка
    # ------------------------------------------------------------------

    def _run_parallel(self, tasks: List[Dict]) -> List[Dict]:
        """Запускает загрузку параллельно (не более _MAX_PARALLEL браузеров)."""
        all_results = []

        # Разбиваем на батчи
        for i in range(0, len(tasks), _MAX_PARALLEL):
            batch = tasks[i:i + _MAX_PARALLEL]

            with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                futures = {
                    executor.submit(self._upload_account, task): task
                    for task in batch
                }
                for future in as_completed(futures, timeout=600):
                    task = futures[future]
                    try:
                        results = future.result()
                        all_results.extend(results)
                    except Exception as e:
                        logger.error(
                            "[PUBLISHER] Поток загрузки %s/%s упал: %s",
                            task["acc_name"], task["platform"], e,
                        )
                        all_results.append({
                            "status": "error",
                            "account_id": task["acc_name"],
                            "platform":   task["platform"],
                            "error_msg":  str(e),
                        })

        return all_results

    # ------------------------------------------------------------------
    # Загрузка одного аккаунта / платформы
    # ------------------------------------------------------------------

    def _upload_account(self, task: Dict) -> List[Dict]:
        """
        Запускает браузер и загружает все видео из очереди одного аккаунта.
        Выполняется в отдельном потоке.
        """
        acc_name = task["acc_name"]
        platform = task["platform"]
        acc_cfg  = task["acc_cfg"]
        acc_dir  = task["acc_dir"]
        queue    = task["queue"]
        results  = []

        from pipeline import config
        from pipeline.browser import launch_browser, close_browser
        from pipeline.session_manager import ensure_session_fresh, mark_session_verified
        from pipeline.uploader import upload_video, clean_video_metadata
        from pipeline.analytics import register_upload
        from pipeline.utils import mark_uploaded, increment_upload_count, get_uploads_today

        profile_dir = acc_dir / "browser_profile"
        pw = context = None

        try:
            # Запуск браузера
            logger.info("[PUBLISHER] Запуск браузера: %s / %s", acc_name, platform)
            try:
                pw, context = launch_browser(acc_cfg, profile_dir)
            except RuntimeError as e:
                logger.error("[PUBLISHER] Браузер не запущен %s/%s: %s", acc_name, platform, e)
                if self._guardian:
                    self._guardian.report_upload_error(acc_name, platform, "proxy_unavailable")
                return [{"status": "proxy_error", "account_id": acc_name,
                         "platform": platform, "error_msg": str(e)}]

            # Проверка сессии
            if not ensure_session_fresh(context, acc_name, platform):
                logger.warning("[PUBLISHER] Сессия мертва: %s/%s", acc_name, platform)
                return [{"status": "not_logged_in", "account_id": acc_name,
                         "platform": platform, "error_msg": "Сессия недействительна"}]

            mark_session_verified(acc_name, platform, valid=True)

            # Определяем лимит
            daily_limit = (
                self.memory.get("custom_limits", {}).get(platform)
                or config.PLATFORM_DAILY_LIMITS.get(platform)
                or config.DAILY_UPLOAD_LIMIT
            )

            # Загружаем видео из очереди
            for item in queue:
                uploads_today = get_uploads_today(acc_dir)
                if uploads_today >= daily_limit:
                    logger.info("[PUBLISHER] %s/%s лимит (%d/%d)",
                                acc_name, platform, uploads_today, daily_limit)
                    break

                video_path = item["video_path"]
                meta = item.get("ab_meta") or item.get("meta", {})

                logger.info("[PUBLISHER] Загрузка: %s → %s/%s",
                            Path(video_path).name, acc_name, platform)

                # Антибан задержка
                delay = self._guardian.get_safe_delay(acc_name) if self._guardian else 30.0
                logger.debug("[PUBLISHER] Антибан пауза: %.0f сек", delay)
                time.sleep(delay)

                clean_path = clean_video_metadata(Path(video_path))
                video_url  = upload_video(
                    context, platform, clean_path, meta,
                    account_name=acc_name, account_cfg=acc_cfg,
                )

                if video_url is not None:
                    mark_uploaded(item)
                    increment_upload_count(acc_dir)
                    register_upload(
                        video_stem=Path(video_path).stem,
                        platform=platform,
                        video_url=video_url,
                        meta=meta,
                        ab_variant=meta.get("ab_variant"),
                    )
                    if self._guardian:
                        self._guardian.report_upload_success(acc_name, platform)
                    results.append({
                        "status":      "ok",
                        "account_id":  acc_name,
                        "platform":    platform,
                        "source_path": str(video_path),
                        "video_url":   video_url,
                    })
                    logger.info("[PUBLISHER] ✅ %s → %s/%s",
                                Path(video_path).name, acc_name, platform)
                else:
                    if self._guardian:
                        self._guardian.report_upload_error(acc_name, platform, "upload_failed")
                    results.append({
                        "status":      "error",
                        "account_id":  acc_name,
                        "platform":    platform,
                        "source_path": str(video_path),
                        "error_msg":   "upload_video вернул None",
                    })
                    logger.warning("[PUBLISHER] ❌ %s → %s/%s",
                                   Path(video_path).name, acc_name, platform)

        except Exception as e:
            logger.exception("[PUBLISHER] Непредвиденная ошибка %s/%s: %s", acc_name, platform, e)
            if self._guardian:
                self._guardian.report_upload_error(acc_name, platform, str(e)[:100])
            results.append({
                "status":     "error",
                "account_id": acc_name,
                "platform":   platform,
                "error_msg":  str(e),
            })
        finally:
            if context or pw:
                try:
                    from pipeline.browser import close_browser
                    close_browser(pw, context)
                except Exception:
                    pass

        return results

    # ------------------------------------------------------------------
    # Обработка результатов
    # ------------------------------------------------------------------

    def _process_results(self, results: List[Dict]) -> None:
        ok     = [r for r in results if r.get("status") == "ok"]
        errors = [r for r in results if r.get("status") == "error"]
        other  = [r for r in results if r.get("status") not in ("ok", "error")]

        self._uploaded += len(ok)
        self._failed   += len(errors)

        if not results:
            return

        # Детальная статистика
        platforms: Dict[str, int] = {}
        for r in ok:
            p = r.get("platform", "?")
            platforms[p] = platforms.get(p, 0) + 1

        summary = {
            "ts":              datetime.now().isoformat(timespec="seconds"),
            "batch_ok":        len(ok),
            "batch_errors":    len(errors),
            "batch_other":     len(other),
            "total_uploaded":  self._uploaded,
            "total_failed":    self._failed,
            "by_platform":     platforms,
        }

        self.memory.log_event("PUBLISHER", "batch_done", summary)
        self.report(summary)

        # Telegram уведомление
        parts = [f"✅ {len(ok)}"]
        if errors:
            parts.append(f"❌ {len(errors)}")
        by_p = ", ".join(f"{p}:{n}" for p, n in platforms.items())
        msg = f"📤 [PUBLISHER] {' / '.join(parts)}"
        if by_p:
            msg += f" ({by_p})"
        self._send(msg)

        logger.info("[PUBLISHER] Батч: ok=%d, errors=%d, итого=%d/%d",
                    len(ok), len(errors), self._uploaded, self._uploaded + self._failed)

    # ------------------------------------------------------------------
    # Публичный API
    # ------------------------------------------------------------------

    def get_stats(self) -> Dict:
        """Текущая статистика загрузок."""
        return {
            "uploaded": self._uploaded,
            "failed":   self._failed,
            "skipped":  self._skipped,
            "uptime_h": round((datetime.now() - self._session_start).total_seconds() / 3600, 1),
        }

    def trigger_now(self) -> None:
        """Принудительно запустить цикл загрузки (из COMMANDER)."""
        logger.info("[PUBLISHER] Принудительный запуск загрузки")
        self._upload_cycle()
