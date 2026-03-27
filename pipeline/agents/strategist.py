"""
pipeline/agents/strategist.py — STRATEGIST: A/B анализ, расписание, репосты.

Оборачивает pipeline/analytics.py:
  - compare_ab_results()      → сравнение A/B вариантов
  - get_repost_candidates()   → видео кандидаты для репоста
  - queue_reposts()           → постановка в очередь репостов
  - collect_pending_analytics() → сбор аналитики

Запускается каждые 6 часов, сохраняет рекомендации в AgentMemory.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

_DEFAULT_INTERVAL = 6 * 3600  # 6 часов


class Strategist(BaseAgent):
    """
    Анализирует результаты публикаций и вырабатывает рекомендации:
      - какие метаданные работают лучше (A/B)
      - в какое время публиковать
      - что стоит репостить
      - как корректировать лимиты
    """

    def __init__(
        self,
        memory: Optional[AgentMemory] = None,
        notify: Any = None,
        interval_sec: int = _DEFAULT_INTERVAL,
    ) -> None:
        super().__init__("STRATEGIST", memory or get_memory(), notify)
        self._interval    = interval_sec
        self._cycle_count = 0
        from pipeline.agents.gpu_manager import get_gpu_manager
        self._gpu = get_gpu_manager()

    def run(self) -> None:
        logger.info("[STRATEGIST] Запущен, интервал=%ds", self._interval)
        # Первый анализ при запуске
        self._analysis_cycle()
        while not self.should_stop:
            if not self.sleep(self._interval):
                break
            self._analysis_cycle()

    # ------------------------------------------------------------------
    # Полный цикл анализа
    # ------------------------------------------------------------------

    def _analysis_cycle(self) -> None:
        self._set_status(AgentStatus.RUNNING, "анализ аналитики")
        self.set_human_detail("Анализирую метрики, A/B, расписание и рекомендации для агентов")
        self._cycle_count += 1
        cycle = self._cycle_count
        try:
            logger.info("[STRATEGIST] Запуск цикла анализа #%d", cycle)

            # 1. Собираем аналитику с платформ
            collected = self._collect_analytics()

            # 2. A/B анализ
            ab_results = self._analyse_ab()

            # 2б. A/B тест миниатюр
            thumb_results = self._analyse_thumbnails()

            # 2в. Детектор серийного контента
            self._detect_serial_candidates()

            # 3. Кандидаты на репост
            repost_count = self._process_reposts()

            # 4. Умное расписание + применение к аккаунтам
            schedule_recs = self._analyse_schedule()
            if schedule_recs:
                self._apply_schedule_recommendations(schedule_recs)

            # 5. LLM-рекомендации для других агентов (новый блок)
            self._generate_and_write_llm_recommendations(
                collected, ab_results, schedule_recs, cycle
            )

            # Сохраняем рекомендации в память
            recommendations = {
                "analytics_collected": collected,
                "ab_winner":           ab_results[0] if ab_results else None,
                "reposts_queued":      repost_count,
                "best_times":          schedule_recs,
            }
            self.memory.set("strategist_recommendations", recommendations)
            self.memory.log_event("STRATEGIST", "analysis_done", recommendations)
            self.report(recommendations)

            if ab_results or repost_count:
                parts = []
                if ab_results:
                    parts.append(f"A/B: {len(ab_results)} результат(ов)")
                if repost_count:
                    parts.append(f"репостов: {repost_count}")
                self._send(f"📊 [STRATEGIST] {', '.join(parts)}")

        except Exception as e:
            logger.error("[STRATEGIST] Ошибка анализа: %s", e)
        finally:
            self._set_status(AgentStatus.IDLE)

    # ------------------------------------------------------------------
    # LLM-рекомендации для агентов
    # ------------------------------------------------------------------

    def _generate_and_write_llm_recommendations(
        self,
        collected: int,
        ab_results: List[Dict],
        schedule_recs: Dict,
        cycle: int,
    ) -> None:
        """Формирует промпт на основе аналитики и данных SCOUT,
        вызывает Ollama, записывает rec.strategist.* для 4 агентов."""

        # Собираем данные для промпта
        analytics_summary = self._build_analytics_summary(collected, ab_results, schedule_recs)
        scout_data        = self._read_scout_data()

        prompt = (
            "Ты STRATEGIST — аналитик контентного пайплайна. "
            "Проанализируй данные и дай краткие практичные рекомендации для каждого агента.\n\n"
            f"АНАЛИТИКА (приоритет):\n{analytics_summary}\n\n"
            f"ДАННЫЕ ОТ SCOUT:\n{scout_data}\n\n"
            "Верни ТОЛЬКО валидный JSON без пояснений:\n"
            "{\n"
            '  "visionary": "рекомендация по стилю метаданных (заголовки, хэштеги, tone)",\n'
            '  "scout": "рекомендация по направлению поиска (ниши, ключевые слова)",\n'
            '  "editor": "рекомендация по стилю монтажа (фон, темп, формат)",\n'
            '  "guardian": "рекомендация по агрессивности загрузки (осторожно/активно/пауза)"\n'
            "}"
        )

        self._set_status(AgentStatus.WAITING, "ожидание GPU для LLM")
        try:
            from pipeline.agents.gpu_manager import GPUPriority
            with self._gpu.acquire("STRATEGIST", GPUPriority.LLM):
                self._set_status(AgentStatus.RUNNING, "LLM-рекомендации")
                raw = self._call_ollama_with_fallback(
                    prompt=prompt,
                    fallback_value=None,
                    context_description="генерация рекомендаций для агентов",
                )
        except Exception as exc:
            logger.warning("[STRATEGIST] GPU/Ollama ошибка: %s", exc)
            return

        if raw is None:
            # fallback уже залогирован внутри _call_ollama_with_fallback
            return

        # Парсим JSON — максимально защищённо
        recs = self._parse_llm_json(raw)
        if not recs:
            return

        # Записываем рекомендации в AgentMemory
        written = []
        for target_agent in ("visionary", "scout", "editor", "guardian"):
            content = recs.get(target_agent, "").strip()
            if not content:
                continue
            self.memory.write_recommendation(
                from_agent="strategist",
                to_agent=target_agent,
                content=content,
                cycle=cycle,
            )
            written.append(target_agent)

        if written:
            logger.info(
                "[STRATEGIST] Рекомендации записаны для: %s (цикл %d)",
                ", ".join(written), cycle,
            )
            self.memory.log_event(
                "STRATEGIST", "llm_recommendations_written",
                {"targets": written, "cycle": cycle},
            )

    def _build_analytics_summary(
        self,
        collected: int,
        ab_results: List[Dict],
        schedule_recs: Dict,
    ) -> str:
        """Собирает текстовое резюме аналитики для промпта."""
        lines = [f"Собрано записей аналитики: {collected}"]

        if ab_results:
            lines.append(f"A/B результатов: {len(ab_results)}")
            for r in ab_results[:3]:
                winner  = r.get("winner_variant", "?")
                ctr     = r.get("ctr_diff_pct", 0)
                vid     = r.get("video_stem", "?")
                lines.append(f"  - {vid}: победитель={winner}, CTR diff={ctr:.1f}%")
        else:
            lines.append("A/B результатов: нет данных")

        if schedule_recs:
            for platform, hours in schedule_recs.items():
                hours_str = ", ".join(f"{h:02d}:00" for h in hours)
                lines.append(f"Лучшее время для {platform}: {hours_str}")
        else:
            lines.append("Расписание: нет данных")

        return "\n".join(lines)

    def _detect_serial_candidates(self) -> None:
        """Запускает детектор серийного контента и уведомляет при находках."""
        try:
            from pipeline.serial_detector import detect_serial_candidates
            candidates = detect_serial_candidates()
            if candidates:
                top = candidates[:3]
                top_str = ", ".join(
                    f"«{c['title'][:25]}» (ER={c['engagement_rate']:.2f})"
                    for c in top
                )
                self._send(f"📺 [STRATEGIST] Серийные кандидаты: {top_str}")
                logger.info("[STRATEGIST] Serial candidates: %d видео", len(candidates))
        except Exception as exc:
            logger.warning("[STRATEGIST] Ошибка serial detector: %s", exc)

    def _analyse_thumbnails(self) -> list:
        """Сравнивает CTR thumbnail-вариантов и выбирает победителей."""
        try:
            from pipeline.agents.thumbnail_tester import compare_thumbnail_results
            results = compare_thumbnail_results()
            if results:
                for r in results:
                    self._send(
                        f"🖼 [STRATEGIST] Thumbnail winner {r['stem']}: "
                        f"вариант {r['winner']} ({r['reason']})"
                    )
            return results
        except Exception as exc:
            logger.warning("[STRATEGIST] Ошибка thumbnail A/B: %s", exc)
            return []

    def _read_scout_data(self) -> str:
        """Читает рекомендацию SCOUT → STRATEGIST из AgentMemory."""
        rec = self.memory.read_recommendation("scout", "strategist")
        if not rec:
            return "Нет данных от SCOUT"
        content = rec.get("content", "").strip()
        cycle   = rec.get("cycle", "?")
        return f"(цикл {cycle}) {content}" if content else "Нет данных от SCOUT"

    @staticmethod
    def _parse_llm_json(raw: str) -> Optional[Dict[str, str]]:
        """Извлекает JSON из ответа Ollama. Устойчив к markdown-обёрткам."""
        import json, re
        # Убираем markdown-блоки ```json ... ```
        clean = re.sub(r"```(?:json)?", "", raw).strip().rstrip("`").strip()
        # Ищем первый {...} блок
        match = re.search(r"\{[^{}]*\}", clean, re.DOTALL)
        if not match:
            logger.warning("[STRATEGIST] JSON не найден в ответе Ollama: %s", raw[:200])
            return None
        try:
            parsed = json.loads(match.group())
        except json.JSONDecodeError as exc:
            logger.warning("[STRATEGIST] Ошибка парсинга JSON: %s | raw=%s", exc, raw[:200])
            return None
        # Валидация: нужны хотя бы 2 из 4 ключей
        expected = {"visionary", "scout", "editor", "guardian"}
        found = expected & set(parsed.keys())
        if len(found) < 2:
            logger.warning(
                "[STRATEGIST] JSON не содержит нужных ключей: %s", list(parsed.keys())
            )
            return None
        return parsed

    # ------------------------------------------------------------------
    # Сбор аналитики
    # ------------------------------------------------------------------

    def _collect_analytics(self) -> int:
        try:
            from pipeline.analytics import collect_pending_analytics
            count = collect_pending_analytics(dry_run=False)
            logger.info("[STRATEGIST] Собрано аналитики: %d записей", count)
            return count
        except Exception as e:
            logger.warning("[STRATEGIST] Сбор аналитики не удался: %s", e)
            return 0

    # ------------------------------------------------------------------
    # A/B анализ
    # ------------------------------------------------------------------

    def _analyse_ab(self) -> List[Dict]:
        try:
            from pipeline.analytics import compare_ab_results
            results = compare_ab_results()
            if results:
                logger.info("[STRATEGIST] A/B результатов: %d", len(results))
                for r in results[:3]:  # логируем топ-3
                    logger.info(
                        "[STRATEGIST] A/B: %s — победитель вариант %s (CTR diff: %.1f%%)",
                        r.get("video_stem", "?"),
                        r.get("winner_variant", "?"),
                        r.get("ctr_diff_pct", 0),
                    )
            return results
        except Exception as e:
            logger.warning("[STRATEGIST] A/B анализ не удался: %s", e)
            return []

    # ------------------------------------------------------------------
    # Репосты
    # ------------------------------------------------------------------

    def _process_reposts(self) -> int:
        try:
            from pipeline.analytics import queue_reposts
            count = queue_reposts(dry_run=False)
            if count:
                logger.info("[STRATEGIST] В очередь репостов добавлено: %d", count)
            return count
        except Exception as e:
            logger.warning("[STRATEGIST] Репосты не удались: %s", e)
            return 0

    def _apply_schedule_recommendations(
        self, best_times: Dict[str, List[int]]
    ) -> None:
        """
        Применяет рекомендованные часы публикаций к config.json каждого аккаунта.

        Конвертирует часы → строки "HH:00", записывает в
        account_cfg["upload_schedule"][platform] и сохраняет файл.
        UploadScheduler подхватит изменения на следующем тике.

        Args:
            best_times: {platform: [час1, час2, час3]} из _analyse_schedule()
        """
        if not best_times:
            return

        try:
            from pipeline.utils import get_all_accounts, save_json

            accounts = get_all_accounts()
            updated  = 0

            for acc in accounts:
                acc_dir  = acc["dir"]
                cfg_path = acc_dir / "config.json"
                acc_cfg  = acc.get("config", {})

                if not isinstance(acc_cfg, dict):
                    continue

                schedule = acc_cfg.setdefault("upload_schedule", {})
                changed  = False

                for platform, hours in best_times.items():
                    # Проверяем, что платформа используется этим аккаунтом
                    if platform not in acc.get("platforms", []):
                        continue

                    new_times = [f"{h:02d}:00" for h in sorted(hours)]
                    old_times = schedule.get(platform, [])

                    if new_times != old_times:
                        schedule[platform] = new_times
                        changed = True
                        logger.info(
                            "[STRATEGIST] %s/%s расписание: %s → %s",
                            acc["name"], platform,
                            old_times or "(нет)", new_times,
                        )

                if changed:
                    save_json(cfg_path, acc_cfg)
                    updated += 1

            if updated:
                logger.info(
                    "[STRATEGIST] Расписание обновлено у %d аккаунт(ов): %s",
                    updated, best_times,
                )
                self.memory.log_event(
                    "STRATEGIST", "schedule_applied",
                    {"accounts_updated": updated, "best_times": best_times},
                )
                self._send(
                    f"📅 [STRATEGIST] Расписание обновлено для {updated} аккаунт(ов)"
                )

        except Exception as e:
            logger.warning("[STRATEGIST] Ошибка применения расписания: %s", e)

    def get_repost_candidates(self) -> List[Dict]:
        """Возвращает кандидатов на репост (для PUBLISHER)."""
        try:
            from pipeline.analytics import get_repost_candidates
            return get_repost_candidates()
        except Exception as e:
            logger.warning("[STRATEGIST] get_repost_candidates: %s", e)
            return []

    # ------------------------------------------------------------------
    # Умное расписание
    # ------------------------------------------------------------------

    def _analyse_schedule(self) -> Dict[str, List[int]]:
        """
        Анализирует часы публикаций и просмотров.
        Возвращает рекомендованные часы для каждой платформы.
        """
        try:
            from pipeline.analytics import load_analytics as _load_analytics
            data = _load_analytics()
            uploads = data.get("uploads", {})

            if not uploads:
                return {}

            # Собираем статистику по часам
            hour_views: Dict[str, Dict[int, List[int]]] = {}
            for _vid_key, vid_data in uploads.items():
                if not isinstance(vid_data, dict):
                    continue
                for platform_key, pdata in vid_data.items():
                    if not isinstance(pdata, dict):
                        continue
                    views = pdata.get("views", 0)
                    upload_ts = pdata.get("upload_ts", "")
                    if not upload_ts or not views:
                        continue
                    try:
                        from datetime import datetime
                        dt = datetime.fromisoformat(upload_ts)
                        hour = dt.hour
                        platform = platform_key.split(":")[0] if ":" in platform_key else platform_key
                        hour_views.setdefault(platform, {}).setdefault(hour, []).append(views)
                    except Exception:
                        continue

            # Находим лучшие часы (по среднему числу просмотров)
            best_times = {}
            for platform, hours in hour_views.items():
                sorted_hours = sorted(
                    hours.items(),
                    key=lambda x: sum(x[1]) / len(x[1]) if x[1] else 0,
                    reverse=True,
                )
                best_times[platform] = [h for h, _ in sorted_hours[:3]]

            if best_times:
                logger.info("[STRATEGIST] Лучшее время: %s", best_times)

            return best_times

        except Exception as e:
            logger.warning("[STRATEGIST] Анализ расписания не удался: %s", e)
            return {}
