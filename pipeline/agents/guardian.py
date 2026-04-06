"""
pipeline/agents/guardian.py — GUARDIAN: прокси, сессии, антибан, карантин.

Автономно управляет безопасностью всех аккаунтов:

  1. Ротация прокси
     — проверяет основной + fallback прокси каждые 5 мин
     — переключает на рабочий, уведомляет если все недоступны

  2. Мониторинг сессий
     — is_session_stale() для всех аккаунтов раз в час
     — хранит список для PUBLISHER (skip если сессия мертва)
     — НЕ запускает браузер сам (это делает uploader при загрузке)

  3. Карантин
     — отслеживает ошибки загрузки через report_upload_error()
     — публичный is_account_safe() для PUBLISHER перед загрузкой
     — авто-снятие карантина по таймеру (quarantine.lift_quarantine)

  4. Антибан меры
     — сохраняет ротационный список прокси по аккаунтам в AgentMemory
     — записывает ban-сигналы (HTTP 429/403) для стратегического анализа
     — рекомендует паузу между загрузками одного аккаунта
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agents.gpu_manager import get_gpu_manager, GPUPriority
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)


def _sanitize_llm_input(text: str, max_len: int = 300) -> str:
    """Санитизирует строку из AgentMemory перед включением в LLM-промпт.

    Рекомендации могут содержать данные от SCOUT (заголовки из внешних источников) →
    STRATEGIST → GUARDIAN. Prompt injection нейтрализуется здесь.
    """
    import re
    if not text or not isinstance(text, str):
        return ""
    text = re.sub(r"[\r\n\t]+", " ", text)
    text = re.sub(r"[`*#<>{}|\[\]\\]", "", text)
    text = re.sub(
        r"\b(ignore|forget|disregard|override|bypass|jailbreak|pretend|roleplay)\b\s+\S+",
        "[filtered]",
        text,
        flags=re.IGNORECASE,
    )
    return text.strip()[:max_len]

_PROXY_CHECK_INTERVAL   = 300   # 5 минут
_SESSION_CHECK_INTERVAL  = 3600   # 1 час
_PROFILE_CHECK_INTERVAL  = 86400  # 24 часа
_WARMUP_REMINDER_INTERVAL = 3600  # напоминания о конце прогрева (upload_warmup)


class Guardian(BaseAgent):
    """
    Хранитель безопасности аккаунтов.

    Публичные методы (вызываются PUBLISHER'ом):
      is_account_safe(acc_name, platform)   → bool
      report_upload_error(acc, platform, reason)
      report_upload_success(acc, platform)
      get_safe_delay(acc_name)              → float (сек паузы между загрузками)
    """

    def __init__(
        self,
        memory: Optional[AgentMemory] = None,
        notify: Any = None,
    ) -> None:
        super().__init__("GUARDIAN", memory or get_memory(), notify)
        self._gpu                = get_gpu_manager()
        self._proxy_status: Dict[str, bool]  = {}   # "host:port" → healthy
        self._stale_sessions: List[Dict]      = []
        self._quarantined: Dict[str, List]    = {}  # acc_name → [platforms]
        self._last_proxy_check  = 0.0
        self._last_session_check  = 0.0
        self._last_profile_check  = 0.0
        self._last_warmup_reminder_check = 0.0
        # Кеш LLM-задержки: acc_name → (delay_sec, computed_at monotonic)
        # TTL = 30 мин — иначе каждая загрузка захватывает GPU и шлёт промпт
        self._delay_cache: Dict[str, Tuple[float, float]] = {}
        self._DELAY_CACHE_TTL = 1800  # 30 минут

    # ------------------------------------------------------------------
    # run() — фоновый цикл проверок
    # ------------------------------------------------------------------

    def run(self) -> None:
        logger.info("[GUARDIAN] Запущен")
        # Первая полная проверка сразу
        self._proxy_cycle()
        self._session_cycle()

        while not self.should_stop:
            self.set_human_detail("Фоновый контроль: прокси, сессии, отпечатки, прогрев")
            now = time.monotonic()

            if now - self._last_proxy_check >= _PROXY_CHECK_INTERVAL:
                self._proxy_cycle()

            if now - self._last_session_check >= _SESSION_CHECK_INTERVAL:
                self._session_cycle()
                self._fingerprint_check()

            # Проверка ссылок в профилях (раз в 24ч)
            if now - self._last_profile_check >= _PROFILE_CHECK_INTERVAL:
                self._profile_link_cycle()

            if now - self._last_warmup_reminder_check >= _WARMUP_REMINDER_INTERVAL:
                self._last_warmup_reminder_check = time.monotonic()
                try:
                    from pipeline.warmup_notify import scan_warmup_end_reminders

                    scan_warmup_end_reminders()
                except Exception as exc:
                    logger.debug("[GUARDIAN] warmup reminders: %s", exc)

            self.sleep(30.0)

    # ------------------------------------------------------------------
    # Проверка прокси
    # ------------------------------------------------------------------

    def _proxy_cycle(self) -> None:
        self._last_proxy_check = time.monotonic()
        self._set_status(AgentStatus.RUNNING, "проверка прокси")
        self.set_human_detail("Проверяю здоровье прокси у аккаунтов")
        bad_proxies = []

        try:
            from pipeline.utils import get_all_accounts, check_proxy_health

            accounts = get_all_accounts()
            if not accounts:
                self._set_status(AgentStatus.IDLE)
                return

            for acc in accounts:
                acc_cfg = acc.get("config", {})
                proxies = self._collect_proxies(acc_cfg)

                for proxy in proxies:
                    key = f"{proxy.get('host')}:{proxy.get('port')}"
                    healthy = check_proxy_health(proxy)
                    self._proxy_status[key] = healthy

                    if not healthy:
                        bad_proxies.append(f"{acc['name']}: {key}")
                        logger.warning("[GUARDIAN] Прокси недоступен: %s (%s)", key, acc["name"])

            # Записываем в память
            self.memory.set("proxy_status", {
                "checked_at": datetime.now().isoformat(timespec="seconds"),
                "total":      len(self._proxy_status),
                "healthy":    sum(1 for v in self._proxy_status.values() if v),
                "bad":        bad_proxies,
            })

            if bad_proxies:
                self._send(
                    f"⚠️ [GUARDIAN] Недоступных прокси: {len(bad_proxies)}\n"
                    + "\n".join(f"  • {p}" for p in bad_proxies[:5])
                )
            else:
                logger.debug("[GUARDIAN] Все прокси ОК (%d)", len(self._proxy_status))

        except Exception as e:
            logger.error("[GUARDIAN] Ошибка проверки прокси: %s", e)
        finally:
            self._set_status(AgentStatus.IDLE)

    def _collect_proxies(self, acc_cfg: Dict) -> List[Dict]:
        """Собирает все прокси аккаунта (основной + mobileproxy API + fallback)."""
        proxies = []
        primary = acc_cfg.get("proxy", {})
        if primary and primary.get("host"):
            proxies.append(primary)
        else:
            try:
                from pipeline.mobileproxy_connection import fetch_mobileproxy_http_proxy

                mp = fetch_mobileproxy_http_proxy(force_refresh=False, use_cache_on_api_fail=True)
                if mp:
                    proxies.append(mp)
            except Exception:
                pass
        for fb in acc_cfg.get("fallback_proxies", []):
            if fb and fb.get("host"):
                proxies.append(fb)
        return proxies

    # ------------------------------------------------------------------
    # Мониторинг сессий
    # ------------------------------------------------------------------

    def _fingerprint_check(self) -> None:
        """
        Проверяет GEO-согласованность fingerprint-профилей всех аккаунтов.

        Запускается раз в час вместе с _session_cycle.
        Предупреждает если timezone fingerprint не совпадает с GEO прокси —
        это один из главных сигналов антидетекта коротких видео-платформ.
        """
        self.set_human_detail("Сверяю GEO и отпечаток браузера с прокси")
        try:
            from pipeline.utils import get_all_accounts
            from pipeline.fingerprint.geo import get_geo_params

            accounts = get_all_accounts()
            issues = []

            for acc in accounts:
                acc_cfg = acc.get("config", {})
                fp_data = acc_cfg.get("fingerprint", {})
                country = (acc_cfg.get("country") or "").upper()

                for platform in acc.get("platforms", []):
                    if platform not in fp_data:
                        continue  # fingerprint не сгенерирован — сгенерится при запуске

                    fp = fp_data[platform]
                    if not country:
                        continue  # нет GEO — не можем проверить

                    expected = get_geo_params(country)
                    fp_tz = fp.get("timezone_id", "")

                    if fp_tz and fp_tz != expected["tz"]:
                        issues.append(
                            f"{acc['name']}/{platform}: "
                            f"tz={fp_tz!r} ≠ ожидаемый для {country}: {expected['tz']!r}"
                        )

            if issues:
                self.memory.set("fingerprint_issues", issues)
                lines = "\n".join(f"  • {i}" for i in issues[:5])
                self._send(
                    f"🔍 [GUARDIAN] Fingerprint GEO несоответствия: {len(issues)}\n"
                    f"{lines}\n"
                    f"(сбросить: удалить acc_config['fingerprint'][platform])"
                )
                logger.warning("[GUARDIAN] Fingerprint issues: %d", len(issues))
            else:
                self.memory.set("fingerprint_issues", [])
                logger.debug("[GUARDIAN] Fingerprint GEO check: всё OK (%d аккаунтов)", len(accounts))

        except Exception as exc:
            logger.warning("[GUARDIAN] Ошибка fingerprint_check: %s", exc)

    def _profile_link_cycle(self) -> None:
        """
        Проверяет что PreLend-ссылки на месте во всех профилях.

        Запускается раз в 24 часа. При обнаружении пропавшей ссылки —
        пробует восстановить автоматически, сообщает в Telegram.
        """
        self._last_profile_check = time.monotonic()
        self._set_status(AgentStatus.RUNNING, "проверка ссылок в профилях")
        self.set_human_detail("Проверяю PreLend-ссылки в bio профилей")
        missing_links: list = []

        try:
            from pipeline.utils import get_all_accounts
            from pipeline.profile_manager import verify_all_links, setup_all_links

            accounts = get_all_accounts()

            for acc in accounts:
                acc_cfg     = acc.get("config", {})
                prelend_url = acc_cfg.get("prelend_url", "")
                if not prelend_url:
                    continue  # ссылка не настроена — пропуск

                profile_dir = acc["dir"] / "browser_profile"
                results     = verify_all_links(acc_cfg, profile_dir)

                for platform, present in results.items():
                    if present:
                        continue

                    label = f"{acc['name']}/{platform}"
                    logger.info("[GUARDIAN] Ссылка пропала: %s — восстанавливаю", label)

                    restore = setup_all_links(acc_cfg, profile_dir)
                    if restore.get(platform):
                        logger.info("[GUARDIAN] ✅ Ссылка восстановлена: %s", label)
                    else:
                        missing_links.append(label)
                        logger.warning("[GUARDIAN] ❌ Восстановление не удалось: %s", label)

            from datetime import datetime
            self.memory.set("profile_links_status", {
                "checked_at": datetime.now().isoformat(timespec="seconds"),
                "missing":    missing_links,
            })

            if missing_links:
                lines = "\n".join(f"  • {m}" for m in missing_links)
                self._send(
                    f"🔗 [GUARDIAN] Пропавшие ссылки (авто-восстановление не удалось):\n"
                    f"{lines}\n\n"
                    f"Проверьте вручную."
                )
            else:
                logger.info(
                    "[GUARDIAN] Profile link check: всі ссылки на месте (%d аккаунтов)",
                    len(accounts),
                )

        except Exception as exc:
            logger.error("[GUARDIAN] Ошибка _profile_link_cycle: %s", exc)
        finally:
            self._set_status(AgentStatus.IDLE)

    def _session_cycle(self) -> None:
        self._last_session_check = time.monotonic()
        self._set_status(AgentStatus.RUNNING, "проверка сессий")
        self.set_human_detail("Проверяю актуальность сессий браузеров")
        stale = []

        try:
            from pipeline.utils import get_all_accounts
            from pipeline.session_manager import is_session_stale, get_session_age_hours

            accounts = get_all_accounts()
            for acc in accounts:
                for platform in acc.get("platforms", []):
                    if is_session_stale(acc["name"], platform):
                        age = get_session_age_hours(acc["name"], platform)
                        entry = {
                            "account":  acc["name"],
                            "platform": platform,
                            "age_h":    round(age, 1) if age else None,
                        }
                        stale.append(entry)
                        logger.info(
                            "[GUARDIAN] Устаревшая сессия: %s/%s (%.1f ч)",
                            acc["name"], platform, age or 0,
                        )

            self._stale_sessions = stale
            self.memory.set("stale_sessions", stale)

            if stale:
                lines = [f"  • {s['account']}/{s['platform']} ({s['age_h']}ч)" for s in stale[:5]]
                self._send(
                    f"⏰ [GUARDIAN] Устаревших сессий: {len(stale)}\n"
                    + "\n".join(lines)
                    + "\n(обновятся автоматически при следующей загрузке)"
                )

        except Exception as e:
            logger.error("[GUARDIAN] Ошибка проверки сессий: %s", e)
        finally:
            self._set_status(AgentStatus.IDLE)

    # ------------------------------------------------------------------
    # Карантин
    # ------------------------------------------------------------------

    def _refresh_quarantine(self) -> None:
        """Обновляет кэш карантина из quarantine.json."""
        try:
            from pipeline.quarantine import get_status
            status = get_status()
            quarantined: Dict[str, List] = {}
            for acc_name, platforms in status.items():
                if not isinstance(platforms, dict):
                    continue
                q_platforms = [
                    p for p, data in platforms.items()
                    if isinstance(data, dict) and data.get("quarantined", False)
                ]
                if q_platforms:
                    quarantined[acc_name] = q_platforms
            self._quarantined = quarantined
            self.memory.set("quarantine_summary", {
                "total":    sum(len(v) for v in quarantined.values()),
                "accounts": {k: v for k, v in quarantined.items()},
            })
        except Exception as e:
            logger.debug("[GUARDIAN] Ошибка обновления карантина: %s", e)

    # ------------------------------------------------------------------
    # Публичный API для PUBLISHER
    # ------------------------------------------------------------------

    def is_account_safe(self, acc_name: str, platform: str) -> Tuple[bool, str]:
        """
        Проверяет можно ли загружать для аккаунта.

        Returns:
            (True, "") — всё ОК
            (False, причина) — нельзя загружать
        """
        # 1. Карантин
        self._refresh_quarantine()
        if acc_name in self._quarantined:
            if platform in self._quarantined[acc_name]:
                return False, f"в карантине (платформа: {platform})"

        # 2. Карантин через официальный API
        try:
            from pipeline.quarantine import is_quarantined
            if is_quarantined(acc_name, platform):
                return False, "в карантине"
        except Exception:
            pass

        # 3. Прогрев после первой сессии (заливка отложена на несколько дней)
        try:
            from pipeline.upload_warmup import is_upload_blocked

            blocked, reason = is_upload_blocked(acc_name, platform)
            if blocked:
                return False, reason or "прогрев аккаунта"
        except Exception:
            pass

        return True, ""

    def report_upload_error(
        self,
        acc_name: str,
        platform: str,
        reason: str = "upload_failed",
    ) -> None:
        """Фиксирует ошибку загрузки — может поставить в карантин."""
        try:
            from pipeline.quarantine import mark_error, is_quarantined
            mark_error(acc_name, platform, reason)

            # Проверяем встал ли в карантин
            if is_quarantined(acc_name, platform):
                logger.warning("[GUARDIAN] %s/%s → карантин (%s)", acc_name, platform, reason)
                self._send(f"🚫 [GUARDIAN] {acc_name}/{platform} помещён в карантин: {reason}")
                self._quarantined.setdefault(acc_name, [])
                if platform not in self._quarantined[acc_name]:
                    self._quarantined[acc_name].append(platform)

            # Логируем бан-сигналы
            self._log_ban_signal(acc_name, platform, reason)

        except Exception as e:
            logger.warning("[GUARDIAN] report_upload_error: %s", e)

    def report_upload_success(self, acc_name: str, platform: str) -> None:
        """Фиксирует успешную загрузку — снимает счётчик ошибок."""
        try:
            from pipeline.quarantine import mark_success
            mark_success(acc_name, platform)
            # Убираем из кэша карантина если был
            if acc_name in self._quarantined:
                self._quarantined[acc_name] = [
                    p for p in self._quarantined[acc_name] if p != platform
                ]
        except Exception as e:
            logger.debug("[GUARDIAN] report_upload_success: %s", e)

    def get_safe_delay(self, acc_name: str) -> float:
        """
        Возвращает рекомендованную паузу (сек) между загрузками аккаунта.

        Логика принятия решения (иерархия):
          1. Кеш (TTL 30 мин) — LLM не вызывается на каждую загрузку
          2. LLM-решение на основе рекомендаций ACCOUNTANT + STRATEGIST
          3. Hardcoded антибан логика (fallback)

        Диапазон: 30–600 сек. Значения вне диапазона → fallback.
        """
        now = time.monotonic()
        cached = self._delay_cache.get(acc_name)
        if cached is not None:
            delay_val, cached_at = cached
            if now - cached_at < self._DELAY_CACHE_TTL:
                logger.debug(
                    "[GUARDIAN] Задержка для %s из кеша: %.0f сек (осталось %.0f сек)",
                    acc_name, delay_val, self._DELAY_CACHE_TTL - (now - cached_at),
                )
                return delay_val

        llm_delay = self._get_llm_delay(acc_name)
        if llm_delay is not None:
            self._delay_cache[acc_name] = (llm_delay, now)
            return llm_delay

        fallback = self._hardcoded_delay(acc_name)
        # Кешируем и fallback — чтобы не ходить в LLM при каждой загрузке даже когда Ollama недоступен
        self._delay_cache[acc_name] = (fallback, now)
        return fallback

    def _get_llm_delay(self, acc_name: str) -> Optional[float]:
        """Спрашивает Ollama какую паузу выставить для аккаунта.

        Возвращает float если ответ в диапазоне 30–600, иначе None.
        """
        _DELAY_MIN = 30
        _DELAY_MAX = 600

        accountant_rec = self.memory.read_recommendation("accountant", "guardian")
        strategist_rec = self.memory.read_recommendation("strategist", "guardian")

        if not accountant_rec and not strategist_rec:
            return None

        # Текущий статус карантина аккаунта
        quarantine_data = self.memory.get("quarantine_summary", {})
        accounts_data   = quarantine_data.get("accounts", {})
        in_quarantine   = acc_name in accounts_data

        accountant_hint = accountant_rec.get("content", "нет данных") if accountant_rec else "нет данных"
        strategist_hint = strategist_rec.get("content", "нет данных") if strategist_rec else "нет данных"

        prompt = (
            f"Ты антибан-менеджер. Определи паузу между загрузками для аккаунта.\n\n"
            f"Аккаунт: {acc_name}\n"
            f"В карантине: {'да' if in_quarantine else 'нет'}\n"
            f"ACCOUNTANT (лимиты): {_sanitize_llm_input(accountant_hint)}\n"
            f"STRATEGIST (стратегия): {_sanitize_llm_input(strategist_hint)}\n\n"
            f"Верни ТОЛЬКО одно целое число — количество секунд паузы "
            f"(от {_DELAY_MIN} до {_DELAY_MAX}). Без пояснений."
        )

        try:
            with self._gpu.acquire("GUARDIAN_DELAY", GPUPriority.LLM):
                raw = self._call_ollama_with_fallback(
                    prompt=prompt,
                    fallback_value=None,
                    context_description=f"задержка загрузки для {acc_name}",
                )
        except Exception as exc:
            logger.debug("[GUARDIAN] GPU недоступен для LLM-задержки: %s", exc)
            return None

        if raw is None:
            return None

        delay = self._parse_delay(raw, _DELAY_MIN, _DELAY_MAX)
        if delay is not None:
            logger.info(
                "[GUARDIAN] LLM-задержка для %s: %.0f сек (диапазон %d–%d)",
                acc_name, delay, _DELAY_MIN, _DELAY_MAX,
            )
        return delay

    @staticmethod
    def _parse_delay(raw: str, min_val: int, max_val: int) -> Optional[float]:
        """Извлекает число из ответа Ollama и валидирует диапазон.

        Защита: любое значение вне диапазона → None → fallback.
        """
        import re
        numbers = re.findall(r"\b(\d+(?:\.\d+)?)\b", raw.strip())
        if not numbers:
            logger.debug("[GUARDIAN] Не удалось извлечь число из ответа Ollama: %r", raw[:80])
            return None
        try:
            value = float(numbers[0])
        except ValueError:
            return None

        if value < min_val or value > max_val:
            logger.debug(
                "[GUARDIAN] LLM вернул задержку %.0f вне диапазона [%d, %d] — fallback",
                value, min_val, max_val,
            )
            return None

        return value

    def _hardcoded_delay(self, acc_name: str) -> float:
        """Hardcoded антибан логика — fallback когда LLM недоступен.

        Базовая: 30–90 сек.
        При карантинной истории: 120–240 сек.
        """
        import random
        try:
            quarantine_data = self.memory.get("quarantine_summary", {})
            accounts_data   = quarantine_data.get("accounts", {})
            if acc_name in accounts_data:
                return random.uniform(120, 240)
        except Exception:
            pass
        return random.uniform(30, 90)

    # ------------------------------------------------------------------
    # Утилиты
    # ------------------------------------------------------------------

    def _log_ban_signal(self, acc_name: str, platform: str, reason: str) -> None:
        """Записывает бан-сигнал для анализа STRATEGIST'ом."""
        ban_keywords = ["429", "403", "banned", "suspended", "captcha", "restricted"]
        is_ban = any(kw in reason.lower() for kw in ban_keywords)
        if is_ban:
            self.memory.log_event("GUARDIAN", "ban_signal", {
                "account":  acc_name,
                "platform": platform,
                "reason":   reason,
                "ts":       datetime.now().isoformat(timespec="seconds"),
            })
            self._send(f"🔴 [GUARDIAN] Бан-сигнал: {acc_name}/{platform} — {reason}")

    def get_stale_sessions(self) -> List[Dict]:
        """Возвращает список аккаунтов с устаревшими сессиями."""
        return list(self._stale_sessions)

    def get_proxy_summary(self) -> Dict:
        """Возвращает сводку по прокси."""
        return self.memory.get("proxy_status", {})
