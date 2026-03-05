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
        self._interval = interval_sec

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
        try:
            logger.info("[STRATEGIST] Запуск цикла анализа")

            # 1. Собираем аналитику с платформ
            collected = self._collect_analytics()

            # 2. A/B анализ
            ab_results = self._analyse_ab()

            # 3. Кандидаты на репост
            repost_count = self._process_reposts()

            # 4. Умное расписание
            schedule_recs = self._analyse_schedule()

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
            from pipeline.analytics import _load_analytics
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
