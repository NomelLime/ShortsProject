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

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

_UPLOAD_INTERVAL    = 1800  # 30 минут между циклами
_MAX_PARALLEL       = 3     # максимум параллельных браузеров (RAM ограничение)
_MIN_QUEUE_INTERVAL = 5     # минимум сек между проверкой очереди
_MAX_RETRY_ATTEMPTS = 3     # максимум попыток для retry queue


# ── Upload Retry Queue helpers ─────────────────────────────────────────────────

def _load_retry_queue() -> List[Dict]:
    """Читает очередь повторных загрузок из UPLOAD_RETRY_QUEUE."""
    from pipeline import config as _cfg
    path = _cfg.UPLOAD_RETRY_QUEUE
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []


def _save_retry_queue(queue: List[Dict]) -> None:
    """Атомарная запись retry queue."""
    import os, tempfile
    from pipeline import config as _cfg
    path = _cfg.UPLOAD_RETRY_QUEUE
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(queue, ensure_ascii=False, indent=2)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        os.write(fd, text.encode("utf-8"))
        os.close(fd)
        os.replace(tmp, str(path))
    except Exception:
        try: os.close(fd)
        except OSError: pass
        try: os.unlink(tmp)
        except OSError: pass


def _enqueue_retry(file_path: str, platform: str, account_id: str, error: str) -> None:
    """Добавляет неудачную загрузку в retry queue."""
    queue = _load_retry_queue()
    # Не добавляем дубли
    for item in queue:
        if item.get("file") == file_path and item.get("platform") == platform:
            item["attempts"] = item.get("attempts", 0) + 1
            item["last_error"] = error[:200]
            item["next_retry_at"] = datetime.now(timezone.utc).isoformat()
            _save_retry_queue(queue)
            return
    queue.append({
        "file":          file_path,
        "platform":      platform,
        "account_id":    account_id,
        "attempts":      1,
        "last_error":    error[:200],
        "next_retry_at": datetime.now(timezone.utc).isoformat(),
    })
    _save_retry_queue(queue)
    logger.info("[PUBLISHER] Добавлено в retry queue: %s → %s", Path(file_path).name, platform)


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

            # Перед основной загрузкой — обрабатываем retry queue
            self._process_retry_queue()

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
        from pipeline.upload_warmup import is_upload_blocked

        tasks = []
        for acc in accounts:
            acc_name  = acc["name"]
            acc_dir   = Path(acc["dir"])
            platforms = acc.get("platforms", [])

            for platform in platforms:
                uploads_today = get_uploads_today(acc_dir, platform=platform)
                # Проверка Guardian (карантин)
                if self._guardian:
                    safe, reason = self._guardian.is_account_safe(acc_name, platform)
                    if not safe:
                        logger.debug("[PUBLISHER] Пропуск %s/%s: %s", acc_name, platform, reason)
                        self._skipped += 1
                        continue

                warm_block, warm_reason = is_upload_blocked(acc_name, platform)
                if warm_block:
                    logger.debug("[PUBLISHER] Пропуск %s/%s: %s", acc_name, platform, warm_reason)
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

    def _maybe_setup_profile_links(
        self,
        acc_name: str,
        acc_cfg: dict,
        profile_dir: "Path",
    ) -> None:
        """
        Один раз после первой загрузки — убеждается что ссылка в профиле на месте.

        Флаг profile_links_set:{acc_name} в AgentMemory.
        False / отсутствует → вызывает setup_all_links().
        Не бросает исключения (все ошибки логируются).
        """
        prelend_url = acc_cfg.get("prelend_url", "")
        if not prelend_url:
            return

        flag_key   = f"profile_links_set:{acc_name}"
        already_set = self.memory.get(flag_key)
        if already_set:
            return

        logger.info("[PUBLISHER] Первая загрузка %s — настраиваю ссылки в профилях", acc_name)
        try:
            from pipeline.profile_manager import setup_all_links
            results = setup_all_links(acc_cfg, profile_dir)

            self.memory.set(flag_key, results or {"attempted": True})

            success = [p for p, ok in results.items() if ok]
            failed  = [p for p, ok in results.items() if not ok]

            if success:
                self._send(f"🔗 [{acc_name}] Ссылки установлены в профилях: {', '.join(success)}")
            if failed:
                self._send(
                    f"⚠️ [{acc_name}] Не удалось установить ссылки: {', '.join(failed)}
"
                    f"(TikTok требует 1000+ подписчиков)"
                )
        except Exception as exc:
            logger.error("[PUBLISHER] Ошибка setup profile links для %s: %s", acc_name, exc)

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

            from pipeline.upload_warmup import is_upload_warmup_active

            w_block, w_reason = is_upload_warmup_active(acc_dir, platform, acc_cfg)
            if w_block:
                logger.info("[PUBLISHER] %s/%s — заливка на паузе (%s)", acc_name, platform, w_reason)
                return [{
                    "status":      "warmup",
                    "account_id":  acc_name,
                    "platform":    platform,
                    "error_msg":   w_reason,
                }]

            # Setup ссылки в профиле (один раз — после первой загрузки)
            self._maybe_setup_profile_links(acc_name, acc_cfg, profile_dir)

            # Определяем лимит
            daily_limit = (
                self.memory.get("custom_limits", {}).get(platform)
                or config.PLATFORM_DAILY_LIMITS.get(platform)
                or config.DAILY_UPLOAD_LIMIT
            )

            # Загружаем видео из очереди
            for item in queue:
                uploads_today = get_uploads_today(acc_dir, platform=platform)
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
                    increment_upload_count(acc_dir, platform=platform)
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
                    error_msg = "upload_video вернул None"
                    results.append({
                        "status":      "error",
                        "account_id":  acc_name,
                        "platform":    platform,
                        "source_path": str(video_path),
                        "error_msg":   error_msg,
                    })
                    _enqueue_retry(str(video_path), platform, acc_name, error_msg)
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
            _enqueue_retry(str(video_path) if 'video_path' in dir() else "unknown",
                           platform, acc_name, str(e)[:200])
        finally:
            if context or pw:
                try:
                    from pipeline.browser import close_browser
                    close_browser(pw, context)
                except Exception:
                    pass

        return results

    # ------------------------------------------------------------------
    # Retry Queue
    # ------------------------------------------------------------------

    def _process_retry_queue(self) -> None:
        """
        Обрабатывает очередь неудачных загрузок (data/upload_retry_queue.json).
        Попытки с attempts >= _MAX_RETRY_ATTEMPTS удаляются без retry.
        """
        queue = _load_retry_queue()
        if not queue:
            return

        logger.info("[PUBLISHER] Retry queue: %d элементов", len(queue))
        remaining: List[Dict] = []

        for item in queue:
            attempts = item.get("attempts", 1)
            if attempts >= _MAX_RETRY_ATTEMPTS:
                logger.warning(
                    "[PUBLISHER] Retry limit (%d) для %s/%s — удаляем из очереди",
                    _MAX_RETRY_ATTEMPTS, item.get("platform"), Path(item.get("file", "?")).name,
                )
                continue

            file_path = Path(item.get("file", ""))
            platform  = item.get("platform", "")
            acc_id    = item.get("account_id", "")

            if not file_path.exists():
                logger.warning("[PUBLISHER] Retry: файл не найден %s — пропуск", file_path)
                continue

            from pipeline.upload_warmup import is_upload_blocked

            rb, _ = is_upload_blocked(acc_id, platform)
            if rb:
                remaining.append(item)
                continue

            logger.info("[PUBLISHER] Retry попытка %d/%d: %s → %s",
                        attempts + 1, _MAX_RETRY_ATTEMPTS, file_path.name, platform)
            try:
                from pipeline.uploader import upload_video
                from pipeline.utils import get_all_accounts
                accounts = get_all_accounts()
                account  = next((a for a in accounts if a.get("name") == acc_id), None)
                if not account:
                    logger.warning("[PUBLISHER] Retry: аккаунт %s не найден — пропуск", acc_id)
                    item["attempts"] = attempts + 1
                    remaining.append(item)
                    continue

                result = upload_video(account, platform, str(file_path))
                if result:
                    logger.info("[PUBLISHER] Retry ✅ %s → %s/%s", file_path.name, acc_id, platform)
                    # Успех — не добавляем обратно в очередь
                else:
                    item["attempts"] = attempts + 1
                    item["last_error"] = "upload_video вернул None (retry)"
                    item["next_retry_at"] = datetime.now(timezone.utc).isoformat()
                    remaining.append(item)
            except Exception as exc:
                logger.warning("[PUBLISHER] Retry ошибка %s: %s", file_path.name, exc)
                item["attempts"] = attempts + 1
                item["last_error"] = str(exc)[:200]
                item["next_retry_at"] = datetime.now(timezone.utc).isoformat()
                remaining.append(item)

        _save_retry_queue(remaining)
        removed = len(queue) - len(remaining)
        if removed:
            logger.info("[PUBLISHER] Retry queue: %d успешно / %d осталось", removed, len(remaining))

    # ------------------------------------------------------------------
    # Обработка результатов
    # ------------------------------------------------------------------

    def _process_results(self, results: List[Dict]) -> None:
        ok      = [r for r in results if r.get("status") == "ok"]
        errors  = [r for r in results if r.get("status") == "error"]
        warmup  = [r for r in results if r.get("status") == "warmup"]
        other   = [
            r for r in results
            if r.get("status") not in ("ok", "error", "warmup")
        ]

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
            "batch_warmup":    len(warmup),
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
        if warmup:
            parts.append(f"🧊 прогрев {len(warmup)}")
        by_p = ", ".join(f"{p}:{n}" for p, n in platforms.items())
        msg = f"📤 [PUBLISHER] {' / '.join(parts)}"
        if by_p:
            msg += f" ({by_p})"
        self._send(msg)

        logger.info(
            "[PUBLISHER] Батч: ok=%d, errors=%d, warmup=%d, итого=%d/%d",
            len(ok),
            len(errors),
            len(warmup),
            self._uploaded,
            self._uploaded + self._failed,
        )

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
