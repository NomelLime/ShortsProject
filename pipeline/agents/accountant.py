"""
pipeline/agents/accountant.py — ACCOUNTANT: лимиты, карантин, статистика.

Оборачивает pipeline/utils.py:
  - is_daily_limit_reached()  → проверка лимитов
  - get_uploads_today()       → загрузок сегодня
  - increment_upload_count()  → счётчик загрузок

Читает рекомендации STRATEGIST и кастомные лимиты от COMMANDER.
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

_CHECK_INTERVAL = 3600  # 1 час


class Accountant(BaseAgent):
    """
    Следит за дневными лимитами загрузок и статистикой аккаунтов.

    Хранит в AgentMemory:
      - account_stats:     {acc_name: {platform: {uploads_today, limit, ok}}}
      - daily_summary:     общая статистика за день
    """

    def __init__(
        self,
        memory: Optional[AgentMemory] = None,
        notify: Any = None,
    ) -> None:
        super().__init__("ACCOUNTANT", memory or get_memory(), notify)
        self._last_summary_date: Optional[date] = None

    def run(self) -> None:
        logger.info("[ACCOUNTANT] Запущен, интервал=%ds", _CHECK_INTERVAL)
        self._check_limits()
        while not self.should_stop:
            if not self.sleep(_CHECK_INTERVAL):
                break
            self._check_limits()

    # ------------------------------------------------------------------
    # Основная проверка
    # ------------------------------------------------------------------

    def _check_limits(self) -> None:
        self._set_status(AgentStatus.RUNNING, "проверка лимитов")
        try:
            from pipeline.utils import get_all_accounts, get_uploads_today, is_daily_limit_reached
            from pipeline import config

            # Применяем кастомные лимиты от COMMANDER
            custom_limits = self.memory.get("custom_limits", {})

            accounts    = get_all_accounts()
            stats       = {}
            at_limit    = []
            total_today = 0

            for acc in accounts:
                acc_name  = acc["name"]
                acc_dir   = Path(acc["dir"])
                platforms = acc.get("platforms", [])
                stats[acc_name] = {}

                for platform in platforms:
                    uploads_today = get_uploads_today(acc_dir)
                    limit = (
                        custom_limits.get(platform)
                        or custom_limits.get("all")
                        or config.PLATFORM_DAILY_LIMITS.get(platform)
                        or config.DAILY_UPLOAD_LIMIT
                    )
                    limit_reached = is_daily_limit_reached(acc_dir)

                    stats[acc_name][platform] = {
                        "uploads_today": uploads_today,
                        "limit":         limit,
                        "at_limit":      limit_reached,
                    }
                    total_today += uploads_today
                    if limit_reached:
                        at_limit.append(f"{acc_name}/{platform}")

            # Дневной сбросить счётчики если новый день
            today = date.today()
            if self._last_summary_date != today:
                logger.info(
                    "[ACCOUNTANT] Новый день (%s). Всего загрузок вчера: %d",
                    today, self.memory.get("daily_total_yesterday", 0),
                )
                self.memory.set("daily_total_yesterday", total_today)
                self._last_summary_date = today

            # Сохраняем в memory
            summary = {
                "date":         str(today),
                "total_today":  total_today,
                "at_limit_count": len(at_limit),
                "at_limit":     at_limit,
                "accounts":     len(accounts),
            }
            self.memory.set("account_stats", stats)
            self.memory.set("daily_summary", summary)
            self.memory.log_event("ACCOUNTANT", "limits_checked", summary)
            self.report(summary)

            if at_limit:
                logger.info("[ACCOUNTANT] Достигли лимита: %s", at_limit)

            logger.info(
                "[ACCOUNTANT] Загрузок сегодня: %d, аккаунтов на лимите: %d/%d",
                total_today, len(at_limit), len(accounts),
            )

        except Exception as e:
            logger.error("[ACCOUNTANT] Ошибка: %s", e)
        finally:
            self._set_status(AgentStatus.IDLE)

    # ------------------------------------------------------------------
    # Публичный API
    # ------------------------------------------------------------------

    def is_limit_reached(self, account_name: str, platform: str) -> bool:
        """Проверить лимит для конкретного аккаунта (вызывается PUBLISHER'ом)."""
        try:
            from pipeline.utils import get_all_accounts, is_daily_limit_reached
            for acc in get_all_accounts():
                if acc["name"] == account_name:
                    return is_daily_limit_reached(Path(acc["dir"]))
            return False
        except Exception as e:
            logger.warning("[ACCOUNTANT] is_limit_reached error: %s", e)
            return False

    def get_daily_summary(self) -> Dict:
        """Возвращает дневную статистику из памяти."""
        return self.memory.get("daily_summary", {})

    def get_available_accounts(self, platform: str) -> List[str]:
        """Возвращает аккаунты которые ещё не достигли лимита."""
        stats = self.memory.get("account_stats", {})
        available = []
        for acc_name, platforms in stats.items():
            pdata = platforms.get(platform, {})
            if not pdata.get("at_limit", True):
                available.append(acc_name)
        return available
