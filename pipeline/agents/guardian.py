"""
pipeline/agents/guardian.py — GUARDIAN: прокси, сессии, антибан.

Оборачивает:
  pipeline/session_manager.py → ensure_session_fresh(), is_session_stale()
  pipeline/quarantine.py      → get_status(), mark_error(), mark_success()
  pipeline/utils.py           → check_proxy_health(), load_proxy()

Цикл каждые 5 минут:
  1. Проверяет здоровье прокси
  2. Обнаруживает устаревшие сессии
  3. Проверяет статус карантина
  4. Уведомляет DIRECTOR при проблемах
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

_CHECK_INTERVAL = 300  # 5 минут


class Guardian(BaseAgent):
    """
    Обеспечивает безопасность и здоровье аккаунтов.

    Автономно:
      - ротирует прокси при сбоях
      - обнаруживает устаревшие сессии
      - отслеживает карантин аккаунтов
      - уведомляет о проблемах
    """

    def __init__(
        self,
        memory: Optional[AgentMemory] = None,
        notify: Any = None,
    ) -> None:
        super().__init__("GUARDIAN", memory or get_memory(), notify)
        self._stale_sessions: List[Dict] = []
        self._quarantined: Dict = {}
        self._proxy_healthy: Optional[bool] = None

    def run(self) -> None:
        logger.info("[GUARDIAN] Запущен, интервал=%ds", _CHECK_INTERVAL)
        self._full_check()
        while not self.should_stop:
            if not self.sleep(_CHECK_INTERVAL):
                break
            self._full_check()

    # ------------------------------------------------------------------
    # Полная проверка
    # ------------------------------------------------------------------

    def _full_check(self) -> None:
        self._set_status(AgentStatus.RUNNING, "проверка безопасности")
        try:
            issues = []

            # 1. Прокси
            proxy_issue = self._check_proxy()
            if proxy_issue:
                issues.append(proxy_issue)

            # 2. Сессии
            stale = self._check_sessions()
            if stale:
                issues.append(f"устаревших сессий: {len(stale)}")

            # 3. Карантин
            quarantine_info = self._check_quarantine()
            if quarantine_info.get("quarantined_count", 0):
                issues.append(f"в карантине: {quarantine_info['quarantined_count']}")

            if issues:
                logger.warning("[GUARDIAN] Проблемы: %s", "; ".join(issues))
                self._send("⚠️ [GUARDIAN] " + "; ".join(issues))

            self.report({
                "proxy_healthy":      self._proxy_healthy,
                "stale_sessions":     len(self._stale_sessions),
                "quarantine":         quarantine_info,
            })
            self.memory.log_event("GUARDIAN", "health_check", {
                "issues": len(issues), "details": issues,
            })

        except Exception as e:
            logger.error("[GUARDIAN] Ошибка проверки: %s", e)
        finally:
            self._set_status(AgentStatus.IDLE)

    # ------------------------------------------------------------------
    # Проверка прокси
    # ------------------------------------------------------------------

    def _check_proxy(self) -> Optional[str]:
        try:
            from pipeline.utils import load_proxy, check_proxy_health

            raw_proxy = load_proxy()
            if not raw_proxy:
                self._proxy_healthy = None  # прокси не настроен
                return None

            # Парсируем строку вида "host:port:user:pass"
            parts = raw_proxy.split(":")
            if len(parts) >= 2:
                proxy_cfg = {
                    "host": parts[0],
                    "port": parts[1],
                    "username": parts[2] if len(parts) > 2 else None,
                    "password": parts[3] if len(parts) > 3 else None,
                }
            else:
                proxy_cfg = {"host": raw_proxy, "port": "80"}

            healthy = check_proxy_health(proxy_cfg)
            self._proxy_healthy = healthy
            self.memory.set("proxy_healthy", healthy)

            if not healthy:
                logger.warning("[GUARDIAN] Прокси недоступен: %s", raw_proxy[:30])
                return "прокси недоступен"

            logger.debug("[GUARDIAN] Прокси ОК")
            return None

        except Exception as e:
            logger.warning("[GUARDIAN] Проверка прокси не удалась: %s", e)
            return None

    # ------------------------------------------------------------------
    # Проверка сессий
    # ------------------------------------------------------------------

    def _check_sessions(self) -> List[Dict]:
        stale = []
        try:
            from pipeline.utils import get_all_accounts
            from pipeline.session_manager import is_session_stale, get_session_age_hours

            accounts = get_all_accounts()
            for acc in accounts:
                for platform in acc.get("platforms", []):
                    if is_session_stale(acc["name"], platform):
                        age = get_session_age_hours(acc["name"], platform)
                        stale.append({
                            "account":  acc["name"],
                            "platform": platform,
                            "age_h":    round(age, 1) if age else None,
                        })
                        logger.info(
                            "[GUARDIAN] Устаревшая сессия: %s / %s (%.1f ч)",
                            acc["name"], platform, age or 0,
                        )

            self._stale_sessions = stale
            self.memory.set("stale_sessions", stale)

        except Exception as e:
            logger.warning("[GUARDIAN] Проверка сессий не удалась: %s", e)

        return stale

    def refresh_session(self, account_name: str, platform: str) -> bool:
        """Принудительно обновить сессию аккаунта."""
        try:
            from pipeline.session_manager import ensure_session_fresh
            ensure_session_fresh(account_name, platform)
            logger.info("[GUARDIAN] Сессия обновлена: %s / %s", account_name, platform)
            return True
        except Exception as e:
            logger.error("[GUARDIAN] Ошибка обновления сессии %s/%s: %s", account_name, platform, e)
            return False

    # ------------------------------------------------------------------
    # Карантин
    # ------------------------------------------------------------------

    def _check_quarantine(self) -> Dict:
        try:
            from pipeline.quarantine import get_status
            status = get_status()

            quarantined = {
                k: v for k, v in status.items()
                if isinstance(v, dict) and v.get("quarantined", False)
            }
            self._quarantined = quarantined
            self.memory.set("quarantine_status", {
                "quarantined_count": len(quarantined),
                "accounts":          list(quarantined.keys()),
            })
            return {"quarantined_count": len(quarantined), "accounts": list(quarantined.keys())}

        except Exception as e:
            logger.warning("[GUARDIAN] Проверка карантина не удалась: %s", e)
            return {"quarantined_count": 0}

    def report_upload_error(self, account_name: str, platform: str, reason: str = "") -> None:
        """Сообщить об ошибке загрузки — Guardian решает ставить ли в карантин."""
        try:
            from pipeline.quarantine import mark_error
            mark_error(account_name, platform, reason or "upload_failed")
            logger.info("[GUARDIAN] Ошибка загрузки записана: %s/%s (%s)", account_name, platform, reason)
        except Exception as e:
            logger.warning("[GUARDIAN] mark_error не удался: %s", e)

    def report_upload_success(self, account_name: str, platform: str) -> None:
        """Сообщить об успешной загрузке — Guardian снимает ошибки."""
        try:
            from pipeline.quarantine import mark_success
            mark_success(account_name, platform)
        except Exception as e:
            logger.warning("[GUARDIAN] mark_success не удался: %s", e)
