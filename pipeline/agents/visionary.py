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


def _sanitize_llm_input(text: str, max_len: int = 300) -> str:
    """Санитизирует строку из внешних источников перед включением в LLM-промпт.

    Защита от prompt injection: заголовки из внешних источников → SCOUT → STRATEGIST
    → VISIONARY → generate_video_metadata. Убирает управляющие конструкции.
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
            self.set_human_detail("Готов сгенерировать заголовки и описания по запросу EDITOR")
            self.sleep(60.0)

    # ------------------------------------------------------------------
    # Публичный API
    # ------------------------------------------------------------------

    def generate_metadata(
        self,
        video_path: Path,
        num_variants: int = 2,
        ab_variant: Optional[str] = None,
        account_cfg: Optional[Dict] = None,
        target_platform: str = "vk",
    ) -> List[Dict]:
        """
        Генерирует метаданные для видео через Ollama VL (модель видит реальные кадры).

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

        self.set_human_detail(f"Генерирую метаданные и хуки для «{video_path.name}»")
        return self._generate_metadata_routed(
            video_path, num_variants, ab_variant, account_cfg, target_platform,
            acquire_gpu=True,
        )

    def generate_metadata_no_acquire(
        self,
        video_path: Path,
        num_variants: int = 2,
        ab_variant: Optional[str] = None,
        account_cfg: Optional[Dict] = None,
        target_platform: str = "vk",
    ) -> List[Dict]:
        """Как generate_metadata, но без захвата GPU (внутри уже удержанного EDITOR lock)."""
        video_path = Path(video_path)
        if not video_path.exists():
            logger.error("[VISIONARY] Файл не найден: %s", video_path)
            return []

        self.set_human_detail(f"Генерирую метаданные для «{video_path.name}» (внутри EDITOR)")
        return self._generate_metadata_routed(
            video_path, num_variants, ab_variant, account_cfg, target_platform,
            acquire_gpu=False,
        )

    def _generate_metadata_routed(
        self,
        video_path: Path,
        num_variants: int,
        ab_variant: Optional[str],
        account_cfg: Optional[Dict],
        target_platform: str,
        acquire_gpu: bool,
    ) -> List[Dict]:
        strategist_rec = self.memory.read_recommendation("strategist", "visionary")
        scout_rec      = self.memory.read_recommendation("scout",      "visionary")

        conflict = self._detect_conflict(strategist_rec, scout_rec)

        if conflict and ab_variant is None:
            logger.info(
                "[VISIONARY] Конфликт STRATEGIST vs SCOUT — активирован A/B режим"
            )
            return self._generate_ab(
                video_path, num_variants, strategist_rec, scout_rec,
                account_cfg=account_cfg, target_platform=target_platform,
                acquire_gpu=acquire_gpu,
            )

        context_hashtags = self._extract_context_hashtags(
            strategist_rec if ab_variant != "scout" else scout_rec,
            scout_rec      if ab_variant != "scout" else None,
        )
        tag = ab_variant or ("strategist" if strategist_rec else None)
        return self._run_generation(
            video_path, num_variants, context_hashtags, tag,
            account_cfg=account_cfg, target_platform=target_platform,
            acquire_gpu=acquire_gpu,
        )

    # ------------------------------------------------------------------
    # A/B генерация при конфликте рекомендаций
    # ------------------------------------------------------------------

    def _generate_ab(
        self,
        video_path: Path,
        num_variants: int,
        strategist_rec: Optional[Dict],
        scout_rec: Optional[Dict],
        account_cfg: Optional[Dict] = None,
        target_platform: str = "vk",
        acquire_gpu: bool = True,
    ) -> List[Dict]:
        """Генерирует две группы вариантов — по STRATEGIST и по SCOUT."""
        # Делим варианты: минимум 1 на каждую сторону
        n_strategist = max(1, num_variants // 2)
        n_scout      = max(1, num_variants - n_strategist)

        strategist_tags = self._extract_context_hashtags(strategist_rec, None)
        scout_tags      = self._extract_context_hashtags(scout_rec, None)

        variants_a = self._run_generation(
            video_path, n_strategist, strategist_tags, "strategist",
            account_cfg=account_cfg, target_platform=target_platform,
            acquire_gpu=acquire_gpu,
        )
        variants_b = self._run_generation(
            video_path, n_scout, scout_tags, "scout",
            account_cfg=account_cfg, target_platform=target_platform,
            acquire_gpu=acquire_gpu,
        )

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
        """Возвращает True если обе рекомендации существуют и сильно расходятся.

        Алгоритм: токенизируем оба текста, считаем overlap значимых слов.
        Порог: overlap < 10% считается конфликтом.

        Ранее порог был 30% — STRATEGIST и SCOUT пишут о разных вещах
        (тренды vs стратегия), поэтому overlap почти всегда < 30%,
        что включало A/B режим практически каждый раз (удвоение GPU).
        Теперь A/B режим включается только при действительно нулевом пересечении.
        """
        if not strategist_rec or not scout_rec:
            return False

        def _tokens(text: str) -> set:
            import re
            words = re.findall(r"\b[a-zа-яё]{3,}\b", text.lower())
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

        # Порог снижен с 30% до 10%: агенты разного контекста, overlap естественно низкий
        is_conflict = overlap < 0.10
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

        Фильтрует служебные слова типа "Используй", "заголовки", "рекомендую" —
        они не являются тематическими хинтами.
        """
        import re
        hints: List[str] = []

        # Расширенный стоп-лист: общие/служебные слова из рекомендаций агентов
        STOP_WORDS = {
            # Русские служебные
            "используй", "используйте", "заголовки", "заголовок", "рекомендую",
            "рекомендуем", "нужно", "следует", "добавь", "добавьте", "включи",
            "включай", "публикуй", "публикуйте", "загружай", "контент", "видео",
            "формат", "стиль", "тренд", "тренды", "хэштег", "хэштеги", "тема",
            "темы", "аудитория", "охват", "время", "часть", "часов", "каждый",
            # Английские служебные
            "include", "suggest", "recommend", "should", "would", "could",
            "content", "video", "style", "format", "trending", "hashtag",
            "hashtags", "topic", "audience", "reach", "upload", "post",
            # Стандартные стоп-слова (дублируем из _detect_conflict)
            "для", "это", "при", "или", "что", "как", "все", "есть",
            "use", "the", "for", "and", "with", "this", "that", "more",
        }

        for rec in (primary_rec, secondary_rec):
            if not rec:
                continue
            content = rec.get("content", "")
            # Берём слова длиннее 4 символов как потенциальные контекстные хинты
            words = re.findall(r"\b[a-zа-яёA-ZА-ЯЁ]{4,}\b", content)
            filtered = [w for w in words if w.lower() not in STOP_WORDS]
            hints.extend(filtered[:5])  # Не более 5 слов с каждой рекомендации

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
        account_cfg: Optional[Dict] = None,
        target_platform: str = "vk",
        acquire_gpu: bool = True,
    ) -> List[Dict]:
        """Запускает generate_video_metadata; опционально с GPU lock."""
        self._set_status(AgentStatus.WAITING, "ожидание GPU")
        try:
            from pipeline.ai import generate_video_metadata

            def _call() -> List[Dict]:
                self._set_status(AgentStatus.RUNNING, f"генерация meta для {video_path.name}")
                return generate_video_metadata(
                    video_path,
                    trending_hashtags=context_hashtags or None,
                    num_variants=num_variants,
                    account_cfg=account_cfg,
                    target_platform=target_platform,
                )

            if acquire_gpu:
                with self._gpu.acquire("VISIONARY", GPUPriority.LLM):
                    variants = _call()
            else:
                variants = _call()

            # Тегируем каждый вариант
            if ab_variant:
                for v in variants:
                    if isinstance(v, dict):
                        v["ab_variant"] = ab_variant

            # Agent-first enrich: hook scoring + voice persona hints.
            try:
                from pipeline.agents.hook_lab import HookLabAgent
                from pipeline.agents.voice_persona import VoicePersonaAgent

                enriched = HookLabAgent.annotate_variants(variants)
                variants = [VoicePersonaAgent.apply_persona(v) for v in enriched]
            except Exception as enrich_exc:
                logger.debug("[VISIONARY] hook/persona enrich skipped: %s", enrich_exc)

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
            if variants:
                first = variants[0] if isinstance(variants[0], dict) else {}
                self.memory.emit_agent_event(
                    "VISIONARY",
                    "meta_enriched",
                    {"file": video_path.name, "variants": len(variants)},
                    creative_id=str(first.get("creative_id") or ""),
                    hook_type=str(first.get("hook_type") or ""),
                    experiment_id=str(first.get("ab_variant") or "meta_default"),
                    agent_run_id=f"visionary:{video_path.stem}",
                )
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
