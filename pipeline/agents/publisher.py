"""
pipeline/agents/publisher.py — PUBLISHER: загрузка видео на платформы.

Оборачивает pipeline/uploader.py:
  - upload_all()      → загрузка по всем аккаунтам
  - upload_video()    → одна загрузка с retry

Интегрируется с GUARDIAN (карантин) и ACCOUNTANT (лимиты).
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

_UPLOAD_INTERVAL  = 1800  # 30 минут между циклами загрузки
_MAX_RETRY        = 3
_RETRY_DELAY      = 60    # секунды между попытками


class Publisher(BaseAgent):
    """
    Управляет очередями загрузки для всех аккаунтов и платформ.

    Уведомляет:
      - GUARDIAN при ошибках загрузки
      - ACCOUNTANT при успешных загрузках (для лимитов)
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
        self._set_status(AgentStatus.RUNNING, "проверка очереди загрузки")
        try:
            from pipeline import config
            from pipeline.utils import get_all_accounts, get_upload_queue

            accounts = get_all_accounts()
            if not accounts:
                logger.debug("[PUBLISHER] Аккаунтов нет — пропускаю")
                self._set_status(AgentStatus.IDLE)
                return

            # Проверяем наличие файлов в очередях
            has_work = False
            for acc in accounts:
                for platform in acc.get("platforms", []):
                    queue = get_upload_queue(acc["dir"], platform)
                    if queue:
                        has_work = True
                        break
                if has_work:
                    break

            if not has_work:
                logger.debug("[PUBLISHER] Очереди загрузки пусты")
                self._set_status(AgentStatus.IDLE)
                return

            logger.info("[PUBLISHER] Запуск загрузки...")
            self._set_status(AgentStatus.RUNNING, "загрузка видео")

            results = self._run_upload_all()
            self._process_results(results)

        except Exception as e:
            logger.error("[PUBLISHER] Ошибка цикла загрузки: %s", e)
            self._set_status(AgentStatus.ERROR, str(e))
            raise
        finally:
            if self._status != AgentStatus.ERROR:
                self._set_status(AgentStatus.IDLE)

    def _run_upload_all(self) -> List[Dict]:
        """Запускает upload_all() с retry при временных ошибках."""
        for attempt in range(1, _MAX_RETRY + 1):
            try:
                from pipeline.uploader import upload_all
                results = upload_all(dry_run=False)
                return results or []
            except Exception as e:
                if attempt < _MAX_RETRY:
                    logger.warning(
                        "[PUBLISHER] Ошибка загрузки (попытка %d/%d): %s — жду %ds",
                        attempt, _MAX_RETRY, e, _RETRY_DELAY,
                    )
                    time.sleep(_RETRY_DELAY)
                else:
                    logger.error("[PUBLISHER] Загрузка не удалась после %d попыток: %s", _MAX_RETRY, e)
                    raise
        return []

    def _process_results(self, results: List[Dict]) -> None:
        """Обрабатывает результаты загрузки, уведомляет GUARDIAN/ACCOUNTANT."""
        ok = [r for r in results if r.get("status") == "ok"]
        fail = [r for r in results if r.get("status") not in ("ok", "quarantined", "skipped")]

        self._uploaded += len(ok)
        self._failed   += len(fail)

        # Уведомляем Guardian о результатах
        for r in ok:
            if self._guardian:
                self._guardian.report_upload_success(
                    r.get("account_id", "?"), r.get("platform", "?")
                )
        for r in fail:
            if self._guardian:
                self._guardian.report_upload_error(
                    r.get("account_id", "?"), r.get("platform", "?"),
                    r.get("error", "upload_failed"),
                )

        if ok or fail:
            logger.info("[PUBLISHER] Загружено: %d, ошибок: %d", len(ok), len(fail))
            self.memory.log_event("PUBLISHER", "upload_batch_done", {
                "ok": len(ok), "failed": len(fail),
                "total_uploaded": self._uploaded,
            })
            self.report({
                "last_ok":        len(ok),
                "last_failed":    len(fail),
                "total_uploaded": self._uploaded,
                "total_failed":   self._failed,
            })

            msg_parts = [f"✅ загружено: {len(ok)}"]
            if fail:
                msg_parts.append(f"❌ ошибок: {len(fail)}")
            self._send(f"📤 [PUBLISHER] {', '.join(msg_parts)}")
