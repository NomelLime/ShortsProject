"""
pipeline/agents/editor.py — EDITOR: нарезка, постобработка, клонирование.

Оборачивает pipeline/main_processing.py:
  - run_processing() → полный цикл обработки

Умный выбор фона:
  1. Ищет файл по теме в assets/backgrounds/
  2. Нет совпадения → лучший по score (размер + дата)
  3. Нет файлов → AnimateDiff (если включён, иначе без фона)
"""
from __future__ import annotations

import logging
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agents.gpu_manager import get_gpu_manager, GPUPriority
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

_SCAN_INTERVAL = 120  # секунды между проверкой очереди обработки


class Editor(BaseAgent):
    """
    Запускает pipeline обработки: нарезка → AI-meta → постобработка → клоны.

    Вызывает run_processing() из main_processing.py с GPU-блокировкой
    на ffmpeg encode. AI-метаданные (Ollama) захватываются VISIONARY'ем
    внутри run_processing через тот же GPUManager.
    """

    def __init__(
        self,
        memory: Optional[AgentMemory] = None,
        notify: Any = None,
        visionary: Any = None,   # Visionary агент — опционально
    ) -> None:
        super().__init__("EDITOR", memory or get_memory(), notify)
        self._gpu       = get_gpu_manager()
        self._visionary = visionary
        self._processed = 0

    def run(self) -> None:
        logger.info("[EDITOR] Запущен, интервал=%ds", _SCAN_INTERVAL)
        while not self.should_stop:
            self._process_cycle()
            if not self.sleep(_SCAN_INTERVAL):
                break

    # ------------------------------------------------------------------
    # Основной цикл обработки
    # ------------------------------------------------------------------

    def _process_cycle(self) -> None:
        self._set_status(AgentStatus.RUNNING, "проверка очереди")
        try:
            from pipeline import config
            from pipeline.utils import probe_video

            preparing_dir = Path(config.PREPARING_DIR)
            if not preparing_dir.exists() or not any(preparing_dir.iterdir()):
                self._set_status(AgentStatus.IDLE)
                return

            logger.info("[EDITOR] Запуск цикла обработки видео")
            self._set_status(AgentStatus.WAITING, "ожидание GPU (encode)")

            with self._gpu.acquire("EDITOR", GPUPriority.ENCODE):
                self._set_status(AgentStatus.RUNNING, "обработка видео")
                from pipeline.main_processing import run_processing
                processed_files = run_processing(dry_run=False)

            count = len(processed_files) if processed_files else 0
            self._processed += count

            if count:
                logger.info("[EDITOR] Обработано %d файлов (итого: %d)", count, self._processed)
                self.memory.log_event("EDITOR", "processing_done", {
                    "processed": count, "total": self._processed,
                })
                self.report({"last_batch": count, "total_processed": self._processed})
                self._send(f"✂️ [EDITOR] Обработано {count} видео (итого: {self._processed})")

        except Exception as e:
            logger.error("[EDITOR] Ошибка обработки: %s", e)
            self._set_status(AgentStatus.ERROR, str(e))
            raise
        finally:
            if self._status != AgentStatus.ERROR:
                self._set_status(AgentStatus.IDLE)

    # ------------------------------------------------------------------
    # Умный выбор фона
    # ------------------------------------------------------------------

    def select_background(self, topic: str = "") -> Optional[Path]:
        """
        Выбирает лучший фон для видео.

        Алгоритм:
          1. Ищем файл по теме (совпадение слов в имени файла)
          2. Нет совпадения → get_unique_bg() (ротация без повторов)
          3. Нет файлов в папке → AnimateDiff (если enabled) или None
        """
        try:
            from pipeline import config
            from pipeline.utils import get_unique_bg

            bg_dir = getattr(config, "BG_VIDEO_DIR", None)
            if bg_dir is None:
                # Ищем в стандартных местах
                for candidate in ["assets/backgrounds", "assets/bg_videos", "backgrounds"]:
                    p = Path(config.BASE_DIR) / candidate
                    if p.exists():
                        bg_dir = p
                        break

            if bg_dir and Path(bg_dir).exists():
                bg_files = list(Path(bg_dir).glob("*.mp4"))
                if bg_files:
                    # Поиск по теме
                    if topic:
                        topic_words = topic.lower().split()
                        matches = [
                            f for f in bg_files
                            if any(w in f.stem.lower() for w in topic_words)
                        ]
                        if matches:
                            chosen = random.choice(matches)
                            logger.info("[EDITOR] Фон по теме '%s': %s", topic, chosen.name)
                            return chosen

                    # Ротация без повторов
                    try:
                        chosen = get_unique_bg(bg_dir)
                        if chosen:
                            logger.info("[EDITOR] Фон (ротация): %s", chosen.name)
                            return chosen
                    except Exception:
                        chosen = random.choice(bg_files)
                        logger.info("[EDITOR] Фон (случайный): %s", chosen.name)
                        return chosen

            # Нет файлов — пробуем AI-генерацию
            if self.memory.get("animatediff_enabled", False):
                return self._generate_bg_ai(topic)

            logger.debug("[EDITOR] Фон не найден (bg_dir=%s)", bg_dir)
            return None

        except Exception as e:
            logger.warning("[EDITOR] Ошибка выбора фона: %s", e)
            return None

    def _generate_bg_ai(self, topic: str) -> Optional[Path]:
        """
        Генерация фона через AnimateDiff/ComfyUI.
        Инфраструктура — реализуется в Этапе 5.
        """
        logger.info("[EDITOR] AnimateDiff генерация фона (тема: %s) — TODO Этап 5", topic)
        # Placeholder для Этапа 5:
        # with self._gpu.acquire("EDITOR_ANIMATEDIFF", GPUPriority.VIDEO_GEN):
        #     from pipeline.animatediff import generate_background
        #     return generate_background(topic)
        return None
