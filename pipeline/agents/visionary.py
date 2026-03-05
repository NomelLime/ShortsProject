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
    ) -> List[Dict]:
        """
        Генерирует метаданные для видео через Ollama + YOLO.

        Args:
            video_path:   путь к видео файлу
            num_variants: количество A/B вариантов

        Returns:
            Список словарей с ключами: title, description, hashtags, hook_text
        """
        video_path = Path(video_path)
        if not video_path.exists():
            logger.error("[VISIONARY] Файл не найден: %s", video_path)
            return []

        self._set_status(AgentStatus.WAITING, "ожидание GPU")
        try:
            with self._gpu.acquire("VISIONARY", GPUPriority.LLM):
                self._set_status(AgentStatus.RUNNING, f"генерация meta для {video_path.name}")
                from pipeline.ai import generate_video_metadata
                variants = generate_video_metadata(video_path, num_variants=num_variants)

            logger.info(
                "[VISIONARY] Meta готова: %d вариант(ов) для %s",
                len(variants), video_path.name,
            )
            self.memory.log_event("VISIONARY", "meta_generated", {
                "file": video_path.name,
                "variants": len(variants),
            })
            self._set_status(AgentStatus.IDLE)
            return variants

        except Exception as e:
            logger.error("[VISIONARY] Ошибка генерации meta: %s", e)
            self._set_status(AgentStatus.ERROR, str(e))
            # Возвращаем fallback-метаданные чтобы pipeline не стопился
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
