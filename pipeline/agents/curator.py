"""
pipeline/agents/curator.py — CURATOR: качество, дедупликация, вирусный потенциал.

Оборачивает pipeline/utils.py:
  - is_duplicate()             → perceptual hash дедупликация
  - compute_perceptual_hash()  → вычисление хэша
  - probe_video()              → проверка целостности

Сканирует PREPARING_DIR каждые N секунд, отбирает годные видео.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

_SCAN_INTERVAL   = 60    # секунды между сканированиями
_MIN_DURATION    = 5.0   # минимальная длина видео (сек)
_MAX_DURATION    = 600.0 # максимальная длина (сек)
_MIN_WIDTH       = 320   # минимальная ширина (px)


class Curator(BaseAgent):
    """
    Сканирует папку с загруженными видео, фильтрует по:
      - целостности файла (probe_video)
      - длительности (5с–10мин)
      - разрешению (≥ 320px)
      - дедупликации через perceptual hash

    Отмечает в AgentMemory количество принятых/отклонённых.
    """

    def __init__(
        self,
        memory: Optional[AgentMemory] = None,
        notify: Any = None,
        scan_interval: int = _SCAN_INTERVAL,
    ) -> None:
        super().__init__("CURATOR", memory or get_memory(), notify)
        self._interval  = scan_interval
        self._accepted  = 0
        self._rejected  = 0

    def run(self) -> None:
        logger.info("[CURATOR] Запущен, интервал=%ds", self._interval)
        while not self.should_stop:
            self._scan_cycle()
            if not self.sleep(self._interval):
                break

    # ------------------------------------------------------------------

    def _scan_cycle(self) -> None:
        self._set_status(AgentStatus.RUNNING, "сканирование")
        try:
            from pipeline import config
            from pipeline.utils import probe_video, is_duplicate

            preparing_dir = Path(config.PREPARING_DIR)
            if not preparing_dir.exists():
                self._set_status(AgentStatus.IDLE)
                return

            video_files = [
                f for f in preparing_dir.rglob("*")
                if f.suffix.lower() in {".mp4", ".mov", ".avi", ".mkv", ".webm"}
                   and not f.name.startswith(".")
            ]

            if not video_files:
                self._set_status(AgentStatus.IDLE)
                return

            accepted_this_cycle = 0
            rejected_this_cycle = 0

            for video in video_files:
                ok, reason = self._evaluate(video, probe_video, is_duplicate)
                if ok:
                    accepted_this_cycle += 1
                    self._accepted += 1
                else:
                    rejected_this_cycle += 1
                    self._rejected += 1
                    logger.debug("[CURATOR] Отклонён %s: %s", video.name, reason)

            if accepted_this_cycle or rejected_this_cycle:
                logger.info(
                    "[CURATOR] Цикл: принято=%d, отклонено=%d (итого: +%d/-%d)",
                    accepted_this_cycle, rejected_this_cycle,
                    self._accepted, self._rejected,
                )
                self.memory.log_event("CURATOR", "scan_done", {
                    "accepted": accepted_this_cycle,
                    "rejected": rejected_this_cycle,
                })
                self.report({
                    "total_accepted": self._accepted,
                    "total_rejected": self._rejected,
                })

        except Exception as e:
            logger.error("[CURATOR] Ошибка сканирования: %s", e)
        finally:
            self._set_status(AgentStatus.IDLE)

    def _evaluate(self, video: Path, probe_fn, is_dup_fn) -> Tuple[bool, str]:
        """Возвращает (True, '') если видео подходит, или (False, причина)."""
        # 1. Целостность файла
        try:
            info = probe_fn(video)
        except Exception as e:
            return False, f"probe failed: {e}"

        # 2. Длительность
        duration = info.get("duration", 0)
        if duration < _MIN_DURATION:
            return False, f"слишком короткое ({duration:.1f}с)"
        if duration > _MAX_DURATION:
            return False, f"слишком длинное ({duration:.1f}с)"

        # 3. Разрешение
        width = info.get("width", 0)
        if width < _MIN_WIDTH:
            return False, f"низкое разрешение ({width}px)"

        # 4. Дедупликация
        try:
            if is_dup_fn(video):
                return False, "дубликат (perceptual hash)"
        except Exception as e:
            logger.debug("[CURATOR] hash check failed for %s: %s", video.name, e)

        return True, ""

    # ------------------------------------------------------------------
    # Публичный API для других агентов
    # ------------------------------------------------------------------

    def check_video(self, video_path: Path) -> Tuple[bool, str]:
        """Проверить конкретное видео (вызывается другими агентами)."""
        from pipeline.utils import probe_video, is_duplicate
        return self._evaluate(video_path, probe_video, is_duplicate)
