"""
pipeline/agents/visionary.py — VISIONARY: метаданные, хуки, A/B тексты.

Оборачивает pipeline/ai.py:
  - generate_video_metadata()   → заголовки, описания, хэштеги, хуки
  - load_trending_hashtags()    → актуальные хэштеги
  - check_ollama()              → проверка доступности LLM

Все вызовы Ollama защищены GPU-блокировкой (GPUPriority.LLM).
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agents.gpu_manager import get_gpu_manager, GPUPriority
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)


class Visionary(BaseAgent):
    """
    Генерирует AI-метаданные для видео.

    Основные методы (вызываются EDITOR'ом):
      generate_metadata(video_path, num_variants) → List[Dict]
      get_trending_hashtags()                     → List[str]
    """

    def __init__(
        self,
        memory: Optional[AgentMemory] = None,
        notify: Any = None,
    ) -> None:
        super().__init__("VISIONARY", memory or get_memory(), notify)
        self._gpu   = get_gpu_manager()
        self._ollama_ok: Optional[bool] = None  # None = ещё не проверяли

    def run(self) -> None:
        logger.info("[VISIONARY] Запущен")
        self._check_ollama()
        while not self.should_stop:
            self._set_status(AgentStatus.IDLE)
            self.sleep(60.0)

    # ------------------------------------------------------------------
    # Публичный API
    # ------------------------------------------------------------------

    def generate_metadata(
        self,
        video_path: Path,
        num_variants: int = 2,
        ab_variant: Optional[str] = None,
    ) -> List[Dict]:
        """
        Генерирует метаданные для видео через Ollama + YOLO.

        Перед генерацией читает рекомендации STRATEGIST и SCOUT из AgentMemory.
        При конфликте между ними активирует A/B режим: часть вариантов
        генерируется по рекомендации STRATEGIST, часть — по SCOUT.

        Args:
            video_path:   путь к видео файлу
            num_variants: количество A/B вариантов
            ab_variant:   принудительный вариант ("strategist"/"scout"), None = авто

        Returns:
            Список словарей с ключами: title, description, hashtags, hook_text, ab_variant
        """
        video_path = Path(video_path)
        if not video_path.exists():
            logger.error("[VISIONARY] Файл не найден: %s", video_path)
            return []

        # Читаем рекомендации из памяти
        strategist_rec = self.memory.read_recommendation("strategist", "visionary")
        scout_rec      = self.memory.read_recommendation("scout",      "visionary")

        # Детектируем конфликт и выбираем режим генерации
        conflict = self._detect_conflict(strategist_rec, scout_rec)

        if conflict and ab_variant is None:
            logger.info(
                "[VISIONARY] Конфликт STRATEGIST vs SCOUT — активирован A/B режим"
            )
            return self._generate_ab(video_path, num_variants, strategist_rec, scout_rec)

        # Обычный режим — единый контекст (STRATEGIST приоритетен)
        context_hashtags = self._extract_context_hashtags(
            strategist_rec if ab_variant != "scout" else scout_rec,
            scout_rec      if ab_variant != "scout" else None,
        )
        tag = ab_variant or ("strategist" if strategist_rec else None)
        return self._run_generation(video_path, num_variants, context_hashtags, tag)

    # ------------------------------------------------------------------
    # A/B генерация при конфликте рекомендаций
    # ------------------------------------------------------------------

    def _generate_ab(
        self,
        video_path: Path,
        num_variants: int,
        strategist_rec: Optional[Dict],
        scout_rec: Optional[Dict],
    ) -> List[Dict]:
        """Генерирует две группы вариантов — по STRATEGIST и по SCOUT."""
        # Делим варианты: минимум 1 на каждую сторону
        n_strategist = max(1, num_variants // 2)
        n_scout      = max(1, num_variants - n_strategist)

        strategist_tags = self._extract_context_hashtags(strategist_rec, None)
        scout_tags      = self._extract_context_hashtags(scout_rec, None)

        variants_a = self._run_generation(video_path, n_strategist, strategist_tags, "strategist")
        variants_b = self._run_generation(video_path, n_scout,      scout_tags,      "scout")

        all_variants = variants_a + variants_b
        logger.info(
            "[VISIONARY] A/B: %d вариантов (strategist=%d, scout=%d) для %s",
            len(all_variants), len(variants_a), len(variants_b), video_path.name,
        )
        self.memory.log_event("VISIONARY", "ab_generated", {
            "file":        video_path.name,
            "strategist":  len(variants_a),
            "scout":       len(variants_b),
        })
        return all_variants

    # ------------------------------------------------------------------
    # Детектор конфликта рекомендаций
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_conflict(
        strategist_rec: Optional[Dict],
        scout_rec: Optional[Dict],
    ) -> bool:
        """Возвращает True если обе рекомендации существуют и расходятся.

        Алгоритм: токенизируем оба текста, считаем overlap значимых слов.
        Если пересечение < 30% от объединения — считаем конфликтом.
        """
        if not strategist_rec or not scout_rec:
            return False

        def _tokens(text: str) -> set:
            import re
            words = re.findall(r"\b[a-zа-яё]{3,}\b", text.lower())
            # Стоп-слова (слишком общие для сравнения)
            stop = {
                "для", "это", "при", "или", "что", "как", "все", "есть",
                "use", "the", "for", "and", "with", "this", "that", "more",
            }
            return {w for w in words if w not in stop}

        tokens_s = _tokens(strategist_rec.get("content", ""))
        tokens_c = _tokens(scout_rec.get("content", ""))

        if not tokens_s or not tokens_c:
            return False

        intersection = tokens_s & tokens_c
        union        = tokens_s | tokens_c
        overlap      = len(intersection) / len(union)

        is_conflict = overlap < 0.30
        logger.debug(
            "[VISIONARY] Overlap STRATEGIST vs SCOUT: %.0f%% → конфликт=%s",
            overlap * 100, is_conflict,
        )
        return is_conflict

    # ------------------------------------------------------------------
    # Вспомогательные
    # ------------------------------------------------------------------

    def _extract_context_hashtags(
        self,
        primary_rec: Optional[Dict],
        secondary_rec: Optional[Dict],
    ) -> List[str]:
        """Извлекает ключевые слова из рекомендаций для передачи как trending_hashtags.

        Это позволяет инжектировать контекст в промпт generate_video_metadata
        без модификации pipeline/ai.py.
        """
        import re
        hints: List[str] = []

        for rec in (primary_rec, secondary_rec):
            if not rec:
                continue
            content = rec.get("content", "")
            # Берём слова длиннее 4 символов как потенциальные контекстные хинты
            words = re.findall(r"\b[a-zа-яёA-ZА-ЯЁ]{4,}\b", content)
            hints.extend(words[:5])  # Не более 5 слов с каждой рекомендации

        # Дедупликация с сохранением порядка
        seen: set = set()
        result: List[str] = []
        for h in hints:
            lh = h.lower()
            if lh not in seen:
                seen.add(lh)
                result.append(h)

        return result[:10]  # Передаём не более 10 хинтов

    def _run_generation(
        self,
        video_path: Path,
        num_variants: int,
        context_hashtags: List[str],
        ab_variant: Optional[str],
    ) -> List[Dict]:
        """Запускает generate_video_metadata с GPU lock и тегирует результат."""
        self._set_status(AgentStatus.WAITING, "ожидание GPU")
        try:
            with self._gpu.acquire("VISIONARY", GPUPriority.LLM):
                self._set_status(AgentStatus.RUNNING, f"генерация meta для {video_path.name}")
                from pipeline.ai import generate_video_metadata
                variants = generate_video_metadata(
                    video_path,
                    trending_hashtags=context_hashtags or None,
                    num_variants=num_variants,
                )

            # Тегируем каждый вариант
            if ab_variant:
                for v in variants:
                    if isinstance(v, dict):
                        v["ab_variant"] = ab_variant

            logger.info(
                "[VISIONARY] Meta готова: %d вариант(ов) для %s (ab_variant=%s)",
                len(variants), video_path.name, ab_variant,
            )
            self.memory.log_event("VISIONARY", "meta_generated", {
                "file":       video_path.name,
                "variants":   len(variants),
                "ab_variant": ab_variant,
                "context":    bool(context_hashtags),
            })
            self._set_status(AgentStatus.IDLE)
            return variants

        except Exception as e:
            logger.error("[VISIONARY] Ошибка генерации meta: %s", e)
            self._set_status(AgentStatus.ERROR, str(e))
            return self._fallback_meta(video_path, num_variants)

    def get_trending_hashtags(self) -> List[str]:
        """Загружает список актуальных хэштегов."""
        try:
            from pipeline.ai import load_trending_hashtags
            tags = load_trending_hashtags()
            logger.debug("[VISIONARY] Хэштегов загружено: %d", len(tags))
            return tags
        except Exception as e:
            logger.warning("[VISIONARY] Ошибка загрузки хэштегов: %s", e)
            return []

    def is_ollama_available(self) -> bool:
        """Проверяет доступность Ollama."""
        if self._ollama_ok is None:
            self._check_ollama()
        return bool(self._ollama_ok)

    # ------------------------------------------------------------------
    # Вспомогательные
    # ------------------------------------------------------------------

    def _check_ollama(self) -> None:
        self._set_status(AgentStatus.RUNNING, "проверка Ollama")
        try:
            from pipeline.ai import check_ollama
            self._ollama_ok = check_ollama()
            status = "доступен ✓" if self._ollama_ok else "недоступен ✗"
            logger.info("[VISIONARY] Ollama %s", status)
            self.memory.set("ollama_available", self._ollama_ok)
            if not self._ollama_ok:
                self._send("⚠️ [VISIONARY] Ollama недоступен — метаданные будут заглушками")
        except Exception as e:
            self._ollama_ok = False
            logger.warning("[VISIONARY] Проверка Ollama не удалась: %s", e)
        finally:
            self._set_status(AgentStatus.IDLE)

    def _fallback_meta(self, video_path: Path, num_variants: int) -> List[Dict]:
        """Заглушка-метаданные если Ollama недоступен."""
        from pipeline.ai import _fallback_meta
        return _fallback_meta(video_path, num_variants)
