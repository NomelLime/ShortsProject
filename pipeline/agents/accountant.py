"""
pipeline/agents/accountant.py — ACCOUNTANT: лимиты, расписание, статистика.

Оборачивает pipeline/utils.py:
  - is_daily_limit_reached()  → проверка лимитов
  - get_uploads_today()       → загрузок сегодня

Интеграция с UploadScheduler:
  - get_next_upload_times(platform) → расписание из config.json аккаунтов
  - get_account_capacity(platform)  → (доступных, всего) для PUBLISHER
  - set_custom_limit()              → вызывается COMMANDER'ом
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

_CHECK_INTERVAL = 3600  # 1 час


class Accountant(BaseAgent):
    """
    Следит за дневными лимитами загрузок и статистикой аккаунтов.

    Хранит в AgentMemory:
      - account_stats:  {acc_name: {platform: {uploads_today, limit, at_limit}}}
      - daily_summary:  общая статистика за день
      - custom_limits:  {platform|"acc.platform": limit}
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
            from pipeline.utils import get_all_accounts, get_uploads_today
            from pipeline import config

            custom_limits = self.memory.get("custom_limits", {})
            accounts      = get_all_accounts()
            stats: Dict   = {}
            at_limit      = []
            total_today   = 0

            for acc in accounts:
                acc_name  = acc["name"]
                acc_dir   = Path(acc["dir"])
                platforms = acc.get("platforms", [])
                acc_cfg   = acc.get("config", {})
                stats[acc_name] = {}

                for platform in platforms:
                    uploads_today = get_uploads_today(acc_dir)

                    # Приоритет: кастомный аккаунт → кастомный платформа
                    # → кастомный "all" → в config аккаунта → платформенный → глобальный
                    limit = (
                        custom_limits.get(f"{acc_name}.{platform}")
                        or custom_limits.get(platform)
                        or custom_limits.get("all")
                        or acc_cfg.get("daily_limits", {}).get(platform)
                        or getattr(config, "PLATFORM_DAILY_LIMITS", {}).get(platform)
                        or config.DAILY_UPLOAD_LIMIT
                    )
                    limit_reached = uploads_today >= limit

                    stats[acc_name][platform] = {
                        "uploads_today": uploads_today,
                        "limit":         limit,
                        "at_limit":      limit_reached,
                    }
                    total_today += uploads_today
                    if limit_reached:
                        at_limit.append(f"{acc_name}/{platform}")

            # Новый день — логируем вчерашнее
            today = date.today()
            if self._last_summary_date != today:
                logger.info(
                    "[ACCOUNTANT] Новый день (%s). Всего загрузок вчера: %d",
                    today, self.memory.get("daily_total_yesterday", 0),
                )
                self.memory.set("daily_total_yesterday", total_today)
                self._last_summary_date = today

            summary = {
                "date":           str(today),
                "total_today":    total_today,
                "at_limit_count": len(at_limit),
                "at_limit":       at_limit,
                "accounts":       len(accounts),
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
    # Публичный API для PUBLISHER
    # ------------------------------------------------------------------

    def is_limit_reached(self, account_name: str, platform: str) -> bool:
        """Проверить лимит для конкретного аккаунта/платформы."""
        stats = self.memory.get("account_stats", {})
        pdata = stats.get(account_name, {}).get(platform, {})
        if pdata:
            return pdata.get("at_limit", False)
        # Fallback — читаем напрямую
        try:
            from pipeline.utils import get_all_accounts, get_uploads_today
            from pipeline import config
            custom = self.memory.get("custom_limits", {})
            for acc in get_all_accounts():
                if acc["name"] == account_name:
                    limit = (
                        custom.get(f"{account_name}.{platform}")
                        or custom.get(platform)
                        or custom.get("all")
                        or config.DAILY_UPLOAD_LIMIT
                    )
                    return get_uploads_today(Path(acc["dir"])) >= limit
        except Exception as e:
            logger.warning("[ACCOUNTANT] is_limit_reached fallback: %s", e)
        return False

    def get_available_accounts(self, platform: str) -> List[str]:
        """Аккаунты, которые ещё не достигли лимита на платформе."""
        stats = self.memory.get("account_stats", {})
        return [
            acc_name
            for acc_name, platforms in stats.items()
            if not platforms.get(platform, {}).get("at_limit", True)
        ]

    def get_account_capacity(self, platform: str) -> Tuple[int, int]:
        """
        Возвращает (доступных, всего) аккаунтов для платформы.
        Используется PUBLISHER'ом для планирования очереди.
        """
        stats     = self.memory.get("account_stats", {})
        total     = 0
        available = 0
        for _name, platforms in stats.items():
            if platform in platforms:
                total += 1
                if not platforms[platform].get("at_limit", True):
                    available += 1
        return available, total

    def get_daily_summary(self) -> Dict:
        """Возвращает дневную статистику из памяти."""
        return self.memory.get("daily_summary", {})

    # ------------------------------------------------------------------
    # Расписание — интеграция с UploadScheduler
    # ------------------------------------------------------------------

    def get_next_upload_times(self, platform: str) -> List[str]:
        """
        Собирает расписание загрузок по всем аккаунтам для платформы.
        Берёт upload_schedule из config.json каждого аккаунта.
        Если нигде не задано — возвращает глобальный DEFAULT_UPLOAD_TIMES.
        """
        try:
            from pipeline.utils import get_all_accounts
            from pipeline.upload_scheduler import DEFAULT_UPLOAD_TIMES

            all_times: List[str] = []
            for acc in get_all_accounts():
                schedule = acc.get("config", {}).get("upload_schedule", {})
                all_times.extend(schedule.get(platform, []))

            if not all_times:
                all_times = DEFAULT_UPLOAD_TIMES

            # Дедупликация с сохранением порядка
            seen: set = set()
            unique = [t for t in all_times if not (t in seen or seen.add(t))]
            return sorted(unique)

        except Exception as e:
            logger.warning("[ACCOUNTANT] get_next_upload_times: %s", e)
            return []

    # ------------------------------------------------------------------
    # Кастомные лимиты — вызывается COMMANDER'ом
    # ------------------------------------------------------------------

    def set_custom_limit(
        self, platform: str, limit: int, account_name: str = ""
    ) -> None:
        """
        Устанавливает кастомный дневной лимит.

        Args:
            platform:     "youtube" / "tiktok" / "instagram" / "all"
            limit:        новый лимит (штук в день)
            account_name: если задан → лимит только для этого аккаунта
        """
        if limit < 0:
            logger.warning("[ACCOUNTANT] Недопустимый лимит: %d", limit)
            return

        custom_limits = self.memory.get("custom_limits", {})
        key = f"{account_name}.{platform}" if account_name else platform
        custom_limits[key] = limit
        self.memory.set("custom_limits", custom_limits)

        logger.info(
            "[ACCOUNTANT] Лимит установлен: %s%s → %d/день",
            f"аккаунт {account_name} " if account_name else "",
            platform, limit,
        )
        self.memory.log_event(
            "ACCOUNTANT", "custom_limit_set", {"key": key, "limit": limit}
        )

    def get_custom_limits(self) -> Dict[str, int]:
        """Возвращает текущие кастомные лимиты (для COMMANDER/статуса)."""
        return self.memory.get("custom_limits", {})
