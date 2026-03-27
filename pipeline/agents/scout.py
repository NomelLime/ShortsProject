"""
pipeline/agents/scout.py — SCOUT: мониторинг трендов и сбор URL.

Оборачивает pipeline/downloader.py:
  - search_and_save()          → полный цикл поиска
  - _expand_keywords_with_ai() → расширение ключевых слов (с GPU lock)
  - merge_and_save_urls()      → сохранение без дубликатов

Цикл: каждые interval_sec секунд запускает поиск.
COMMANDER может установить scout_keywords_override в AgentMemory.
"""
from __future__ import annotations

import logging
from typing import Any, List, Optional

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agents.gpu_manager import get_gpu_manager, GPUPriority
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)
_DEFAULT_INTERVAL = 3600  # 1 час


class Scout(BaseAgent):
    def __init__(
        self,
        memory: Optional[AgentMemory] = None,
        notify: Any = None,
        interval_sec: int = _DEFAULT_INTERVAL,
    ) -> None:
        super().__init__("SCOUT", memory or get_memory(), notify)
        self._interval    = interval_sec
        self._gpu         = get_gpu_manager()
        self._total_found = 0
        self._cycle_count = 0
        # История циклов: [(cycle, keywords_used, urls_found)] для детекции трендов
        self._cycle_history: list = []

    def run(self) -> None:
        logger.info("[SCOUT] Запущен, интервал=%ds", self._interval)
        self._crawl_cycle()
        while not self.should_stop:
            if not self.sleep(self._interval):
                break
            self._crawl_cycle()

    # ------------------------------------------------------------------

    def _crawl_cycle(self) -> None:
        self._set_status(AgentStatus.RUNNING, "поиск URL")
        self.set_human_detail("Ищу новые ссылки по ключевым словам и трендам")
        self._cycle_count += 1
        cycle = self._cycle_count
        try:
            from pipeline import config
            from pipeline.utils import load_keywords, merge_and_save_urls

            override = self.memory.get("scout_keywords_override")
            if override:
                keywords = override if isinstance(override, list) else [override]
                logger.info("[SCOUT] Override-ключевые слова: %s", keywords[:5])
            else:
                keywords = load_keywords()
                # Приоритизация через TrendScout: перемещаем трендовые слова вперёд
                trend_scores: dict = self.memory.get("trend_scores") or {}
                if trend_scores:
                    threshold = getattr(config, "TREND_SCOUT_THRESHOLD", 2)
                    trending = [k for k, v in trend_scores.items() if v >= threshold]
                    # Добавляем трендовые ключевые слова которых нет в базовом списке
                    kw_set = {k.lower() for k in keywords}
                    extra = [t for t in trending if t.lower() not in kw_set]
                    # Объединяем: trending first, остальные в конце
                    keywords = extra[:10] + keywords
                    if extra:
                        logger.info("[SCOUT] TrendScout добавил %d трендовых ключевых слов", len(extra[:10]))

            if not keywords:
                logger.warning("[SCOUT] Нет ключевых слов — пропускаю цикл")
                return

            logger.info("[SCOUT] Ключевых слов: %d", len(keywords))
            expanded = self._expand_keywords(keywords)

            new_urls = self._search_ytdlp(expanded)
            if self.memory.get("scout_browser_enabled", True):
                new_urls = list(dict.fromkeys(new_urls + self._search_browser(expanded)))

            if not new_urls:
                logger.info("[SCOUT] Новых URL не найдено")
                return

            # VL thumbnail pre-filter: отклоняем мусор до скачивания
            new_urls = self._vl_filter_urls(new_urls)

            saved = merge_and_save_urls(new_urls, config.URLS_FILE)
            self._total_found += saved
            logger.info("[SCOUT] Сохранено %d новых URL (сессия: %d)", saved, self._total_found)

            self.memory.log_event("SCOUT", "crawl_done", {
                "found": len(new_urls), "saved_new": saved,
                "total_session": self._total_found,
            })
            self.report({"last_saved": saved, "total_found": self._total_found})
            if saved > 0:
                self._send(f"🔍 [SCOUT] Найдено {saved} новых URL (сессия: {self._total_found})")

            # Записываем тренд для STRATEGIST если есть значимый результат
            self._write_trend_recommendation(expanded, len(new_urls), saved, cycle)

        except Exception as e:
            logger.error("[SCOUT] Ошибка: %s", e)
            raise
        finally:
            if self.status != AgentStatus.ERROR:
                self._set_status(AgentStatus.IDLE)

    def _expand_keywords(self, keywords: List[str]) -> List[str]:
        try:
            from pipeline import config
            if not getattr(config, "AI_KEYWORD_EXPANSION", False):
                return keywords
            self._set_status(AgentStatus.WAITING, "ожидание GPU")
            with self._gpu.acquire("SCOUT", GPUPriority.LLM):
                self._set_status(AgentStatus.RUNNING, "AI расширение KW")
                from pipeline.downloader import _expand_keywords_with_ai
                return _expand_keywords_with_ai(keywords)
        except TimeoutError:
            logger.info("[SCOUT] GPU занят — используем исходные keywords без расширения")
            return keywords
        except Exception as e:
            logger.warning("[SCOUT] AI расширение не удалось: %s", e)
            return keywords

    def _search_ytdlp(self, keywords: List[str]) -> List[str]:
        try:
            from pipeline.utils import load_proxy
            from pipeline.downloader import _search_ytdlp
            urls = _search_ytdlp(keywords, load_proxy())
            logger.info("[SCOUT] yt-dlp: %d URL", len(urls))
            return urls
        except Exception as e:
            logger.warning("[SCOUT] yt-dlp не удался: %s", e)
            return []

    def _search_browser(self, keywords: List[str]) -> List[str]:
        try:
            from pipeline.utils import load_proxy
            from pipeline.downloader import _search_browser
            urls = _search_browser(keywords, load_proxy())
            logger.info("[SCOUT] Браузер: %d URL", len(urls))
            return urls
        except Exception as e:
            logger.warning("[SCOUT] Браузерный поиск не удался: %s", e)
            return []

    # ------------------------------------------------------------------
    # VL thumbnail pre-filter
    # ------------------------------------------------------------------

    def _vl_filter_urls(self, urls: List[str]) -> List[str]:
        """
        VL-оценка thumbnail для YouTube-видео перед добавлением в очередь.

        - Только YouTube URL (остальные проходят без проверки)
        - Лимит SCOUT_VL_MAX_PER_CYCLE проверок за цикл (остаток добавляется)
        - Результаты кешируются в vl_cache.json — повторный поиск бесплатен
        - При отключённом флаге или ошибке — все URL проходят
        """
        try:
            from pipeline import config as _cfg
            if not getattr(_cfg, "SCOUT_VL_THUMBNAIL_FILTER", False):
                return urls

            from pipeline.ai import vl_score_thumbnail
            min_score  = getattr(_cfg, "SCOUT_VL_MIN_SCORE", 7)
            max_checks = getattr(_cfg, "SCOUT_VL_MAX_PER_CYCLE", 20)

            filtered: List[str] = []
            checked = 0

            self._set_status(AgentStatus.RUNNING, f"VL thumbnail фильтр ({len(urls)} URL)")
            self._set_status(AgentStatus.WAITING, "ожидание GPU для VL")

            with self._gpu.acquire("SCOUT_VL", GPUPriority.LLM):
                self._set_status(AgentStatus.RUNNING, "VL thumbnail фильтрация")
                for i, url in enumerate(urls):
                    if checked >= max_checks:
                        # Лимит достигнут — остаток добавляем без проверки
                        filtered.extend(urls[i:])
                        break
                    score = vl_score_thumbnail(url)
                    if score is None:
                        # Не YouTube или ошибка — пропускаем
                        filtered.append(url)
                        continue
                    checked += 1
                    if score >= min_score:
                        filtered.append(url)
                    else:
                        logger.info("[SCOUT] VL отклонил (score=%d): %s", score, url[:70])

            rejected = len(urls) - len(filtered)
            logger.info(
                "[SCOUT] VL filter: %d/%d прошли, %d отклонено (проверено %d)",
                len(filtered), len(urls), rejected, checked,
            )
            self.memory.log_event("SCOUT", "vl_filter_done", {
                "total": len(urls), "passed": len(filtered),
                "rejected": rejected, "checked": checked,
            })
            return filtered

        except Exception as e:
            logger.warning("[SCOUT] VL filter error — все URL пройдут: %s", e)
            return urls

    # ------------------------------------------------------------------
    # Детекция трендов и запись рекомендации для STRATEGIST
    # ------------------------------------------------------------------

    def _write_trend_recommendation(
        self,
        keywords: List[str],
        urls_found: int,
        urls_saved: int,
        cycle: int,
    ) -> None:
        """Анализирует результаты цикла, при значимом тренде пишет
        ``rec.scout.strategist`` в AgentMemory.

        Записывает рекомендацию если:
        - найдено ≥ 5 новых URL (минимальный сигнал), ИЛИ
        - текущий цикл значительно превышает среднее предыдущих (рост ≥ 50%).
        """
        # Обновляем историю (храним последние 10 циклов)
        self._cycle_history.append(urls_found)
        if len(self._cycle_history) > 10:
            self._cycle_history.pop(0)

        # Проверяем порог и рост
        is_significant = urls_found >= 5
        growth_pct     = 0.0

        if len(self._cycle_history) >= 2:
            prev_avg = sum(self._cycle_history[:-1]) / len(self._cycle_history[:-1])
            if prev_avg > 0:
                growth_pct = (urls_found - prev_avg) / prev_avg * 100
                if growth_pct >= 50:
                    is_significant = True

        if not is_significant:
            logger.debug(
                "[SCOUT] Тренд не значимый (found=%d, growth=%.1f%%) — пропускаю запись",
                urls_found, growth_pct,
            )
            return

        # Определяем топ-нишу (самое часто встречаемое ключевое слово)
        top_niche = self._detect_top_niche(keywords)

        # Формируем содержательное описание
        growth_str = f" (+{growth_pct:.0f}% к среднему)" if growth_pct >= 50 else ""
        content = (
            f"Цикл {cycle}: найдено {urls_found} URL{growth_str}. "
            f"Сохранено новых: {urls_saved}. "
            f"Топ-ниша: '{top_niche}'. "
            f"Всего ключевых слов в поиске: {len(keywords)}."
        )

        self.memory.write_recommendation(
            from_agent="scout",
            to_agent="strategist",
            content=content,
            cycle=cycle,
        )
        logger.info("[SCOUT] Тренд записан для STRATEGIST: %s", content)

    @staticmethod
    def _detect_top_niche(keywords: List[str]) -> str:
        """Определяет топ-нишу по частоте встречаемости ключевых слов.

        Ранее брало кратчайшее слово — "AI" побеждало "cooking", "travel"
        и другие реальные категории. Теперь выбираем нишу по частоте:
        слово, которое встречается в наибольшем количестве ключевых фраз,
        является доминирующей темой.
        При равенстве предпочитаем более длинное слово (более специфичное).
        """
        if not keywords:
            return "unknown"
        cleaned = [kw.strip().lower() for kw in keywords if kw.strip()]
        if not cleaned:
            return "unknown"

        # Считаем частоту каждого отдельного слова во всех ключевых фразах
        from collections import Counter
        word_freq: Counter = Counter()
        for phrase in cleaned:
            # Слова длиннее 3 символов — исключаем артикли и предлоги
            words = [w for w in phrase.split() if len(w) > 3]
            for w in words:
                word_freq[w] += 1

        if not word_freq:
            # Если все слова короткие — берём самую частую фразу целиком
            phrase_freq: Counter = Counter(cleaned)
            return phrase_freq.most_common(1)[0][0]

        # Топ по частоте; при равенстве — длиннее (специфичнее)
        top_word = max(word_freq.keys(), key=lambda w: (word_freq[w], len(w)))
        logger.debug("[SCOUT] top_niche='%s' (freq=%d из %d фраз)", top_word, word_freq[top_word], len(cleaned))
        return top_word
