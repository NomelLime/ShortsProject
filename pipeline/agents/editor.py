"""pipeline/agents/editor.py — EDITOR: нарезка, клонирование, постобработка, фоны."""
from __future__ import annotations
import logging
from pathlib import Path
from typing import Any, Optional
from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agents.gpu_manager import get_gpu_manager, GPUPriority
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

class Editor(BaseAgent):
    def __init__(self, memory: Optional[AgentMemory] = None, notify: Any = None) -> None:
        super().__init__("EDITOR", memory or get_memory(), notify)
        self._gpu = get_gpu_manager()

    def run(self) -> None:
        from pipeline.main_processing import run_processing_phase
        logger.info("[EDITOR] Запущен")
        while not self.should_stop:
            self._set_status(AgentStatus.IDLE)
            self.sleep(30.0)

    def process(self, task: dict) -> bool:
        """Запускает полный цикл обработки одного видео."""
        from pipeline.main_processing import run_processing_phase
        bg_path = self._select_background(task.get("topic", ""))
        self._set_status(AgentStatus.RUNNING, "обработка")
        try:
            with self._gpu.acquire("EDITOR", GPUPriority.ENCODE):
                run_processing_phase()
            self._set_status(AgentStatus.IDLE)
            return True
        except Exception as e:
            logger.error("[EDITOR] Ошибка: %s", e)
            self._set_status(AgentStatus.ERROR, str(e))
            raise

    def _select_background(self, topic: str) -> Optional[Path]:
        """
        Выбирает фон: сравнивает файлы в assets/backgrounds/ с AI-генерацией.
        Логика: если есть подходящий файл по теме — использовать его,
        иначе сгенерировать через AnimateDiff (если включено).
        """
        from pipeline import config
        bg_dir = config.BG_VIDEO_DIR if hasattr(config, 'BG_VIDEO_DIR') else Path("assets/backgrounds")
        bg_files = list(Path(bg_dir).glob("*.mp4")) if Path(bg_dir).exists() else []

        if bg_files:
            # Ищем файл по теме (по имени файла)
            topic_lower = topic.lower()
            for f in bg_files:
                if any(w in f.stem.lower() for w in topic_lower.split()):
                    logger.info("[EDITOR] Фон по теме: %s", f.name)
                    return f
            # Нет совпадения — используем случайный файл
            import random
            chosen = random.choice(bg_files)
            logger.info("[EDITOR] Фон случайный: %s", chosen.name)
            return chosen

        # Нет файлов — AnimateDiff (инфраструктура)
        ai_enabled = self.memory.get("animatediff_enabled", False)
        if ai_enabled:
            logger.info("[EDITOR] Фон: AnimateDiff генерация (тема=%s)", topic)
            return self._generate_bg_animatediff(topic)

        logger.warning("[EDITOR] Нет фонов в %s и AnimateDiff выключен", bg_dir)
        return None

    def _generate_bg_animatediff(self, topic: str) -> Optional[Path]:
        """Заглушка для генерации фона через AnimateDiff (Этап 5)."""
        logger.info("[EDITOR] AnimateDiff: TODO — реализовать в Этапе 5")
        return None
