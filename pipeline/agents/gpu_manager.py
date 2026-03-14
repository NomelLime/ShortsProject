"""
pipeline/agents/gpu_manager.py — Менеджер GPU-ресурсов для ShortsProject.

Проблема: RTX 5070 Ti имеет 12GB VRAM. Одновременный запуск
Ollama (8-9GB) + TTS Kokoro (0.5GB) + AnimateDiff (~8GB) невозможен.

Решение: GPUResourceManager — очередь токенов (semaphore-based).
Каждый потребитель GPU запрашивает токен перед запуском и
возвращает после завершения.

Приоритеты (чем ниже число — тем выше приоритет):
  0 — CRITICAL (антибан, CAPTCHA)
  1 — LLM / Ollama (метаданные, анализ)
  2 — TTS / Kokoro (озвучка)
  3 — VideoGen / AnimateDiff (генерация фонов)
  4 — ffmpeg encode (постобработка, клонирование)

Использование:
    gpu = GPUResourceManager()

    with gpu.acquire(consumer="VISIONARY", priority=1):
        ollama.generate(...)  # безопасно — только один в VRAM

    # или как декоратор:
    @gpu.gpu_task(priority=2)
    def run_tts():
        ...
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


# Приоритеты GPU задач
class GPUPriority:
    CRITICAL  = 0   # CAPTCHA / антибан
    LLM       = 1   # Ollama inference
    TTS       = 2   # Kokoro TTS
    VIDEO_GEN = 3   # AnimateDiff / фоны
    ENCODE    = 4   # ffmpeg (CPU+GPU)


_GPU_TASK_MAX_RETRIES = 3   # максимум повторных попыток получить слот GPU


@dataclass(order=True)
class _GPUTask:
    """Задача в очереди GPU. Сортируется по приоритету."""
    priority:  int
    seq:       int = field(compare=False)   # порядок добавления (FIFO внутри приоритета)
    consumer:  str = field(compare=False)
    event:     threading.Event = field(compare=False, default_factory=threading.Event)
    retries:   int = field(compare=False, default=0)


class GPUResourceManager:
    """
    Менеджер доступа к GPU. Гарантирует что в каждый момент времени
    GPU использует только один "тяжёлый" процесс.

    max_concurrent: сколько задач могут использовать GPU одновременно.
    На 12GB VRAM рекомендуется 1 (для LLM/TTS/VideoGen).
    ffmpeg encode можно разрешить параллельно с LLM если нужно.
    """

    def __init__(self, max_concurrent: int = 1) -> None:
        self._max       = max_concurrent
        self._semaphore = threading.Semaphore(max_concurrent)
        self._lock      = threading.Lock()
        self._task_queue: queue.PriorityQueue = queue.PriorityQueue(maxsize=100)
        self._active: Dict[str, float] = {}   # consumer → start_time
        self._seq       = 0
        self._stats: Dict[str, Dict] = {}
        self._dispatcher_thread = threading.Thread(
            target=self._dispatcher, daemon=True, name="gpu-dispatcher"
        )
        self._running = False

    def start(self) -> None:
        """Запускает фоновый диспетчер очереди."""
        if not self._running:
            self._running = True
            # Пересоздаём поток — threading.Thread нельзя запустить повторно после stop()
            self._dispatcher_thread = threading.Thread(
                target=self._dispatcher, daemon=True, name="gpu-dispatcher"
            )
            self._dispatcher_thread.start()
            logger.info("[GPUManager] Запущен (max_concurrent=%d)", self._max)

    def stop(self) -> None:
        self._running = False
        logger.info("[GPUManager] Остановлен.")

    @contextmanager
    def acquire(self, consumer: str, priority: int = GPUPriority.ENCODE):
        """
        Контекстный менеджер для захвата GPU токена.

        Использование:
            with gpu_manager.acquire("VISIONARY", GPUPriority.LLM):
                result = ollama.generate(...)
        """
        task = self._enqueue(consumer, priority)
        logger.debug("[GPUManager] [%s] Ожидаем GPU (приоритет %d)...", consumer, priority)

        # Ждём пока диспетчер разрешит нашей задаче выполниться
        task.event.wait(timeout=360)
        if not task.event.is_set():
            raise TimeoutError(f"[GPUManager] GPU не получен за 360с для '{consumer}'")

        start_time = time.monotonic()
        with self._lock:
            self._active[consumer] = start_time

        logger.info("[GPUManager] [%s] GPU захвачен (приоритет %d)", consumer, priority)

        try:
            yield
        finally:
            elapsed = time.monotonic() - start_time
            with self._lock:
                self._active.pop(consumer, None)
                self._update_stats(consumer, elapsed)
            self._semaphore.release()
            logger.info(
                "[GPUManager] [%s] GPU освобождён (занято %.1f сек.)",
                consumer, elapsed,
            )

    def gpu_task(self, priority: int = GPUPriority.ENCODE):
        """Декоратор для функций использующих GPU."""
        def decorator(fn: Callable):
            def wrapper(*args, **kwargs):
                consumer = fn.__qualname__
                with self.acquire(consumer, priority):
                    return fn(*args, **kwargs)
            wrapper.__name__ = fn.__name__
            return wrapper
        return decorator

    def status(self) -> Dict:
        """Возвращает текущее состояние менеджера."""
        with self._lock:
            return {
                "active":        dict(self._active),
                "queue_size":    self._task_queue.qsize(),
                "max_concurrent": self._max,
                "stats":         dict(self._stats),
            }

    def _enqueue(self, consumer: str, priority: int) -> _GPUTask:
        with self._lock:
            seq = self._seq
            self._seq += 1
        task = _GPUTask(priority=priority, seq=seq, consumer=consumer)
        self._task_queue.put(task)
        return task

    def _dispatcher(self) -> None:
        """Фоновый поток: берёт задачи из очереди и разрешает им запускаться."""
        while self._running:
            try:
                task = self._task_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            # Ждём свободного слота
            acquired = self._semaphore.acquire(timeout=300)
            if not acquired:
                task.retries += 1
                if task.retries >= _GPU_TASK_MAX_RETRIES:
                    logger.error(
                        "[GPUManager] Задача [%s] отменена после %d попыток — GPU не освобождается",
                        task.consumer, task.retries,
                    )
                    # event остаётся неустановленным → consumer получит TimeoutError
                    continue
                logger.warning(
                    "[GPUManager] Timeout ожидания слота для [%s] (попытка %d/%d)",
                    task.consumer, task.retries, _GPU_TASK_MAX_RETRIES,
                )
                # Возвращаем задачу в очередь
                self._task_queue.put(task)
                continue

            # Разрешаем задаче выполняться
            task.event.set()

    def _update_stats(self, consumer: str, elapsed: float) -> None:
        if consumer not in self._stats:
            self._stats[consumer] = {"calls": 0, "total_sec": 0.0, "avg_sec": 0.0}
        s = self._stats[consumer]
        s["calls"]     += 1
        s["total_sec"] += elapsed
        s["avg_sec"]    = s["total_sec"] / s["calls"]


# Глобальный синглтон
_gpu_manager: Optional[GPUResourceManager] = None


def get_gpu_manager() -> GPUResourceManager:
    """Возвращает глобальный GPUResourceManager (создаёт при первом вызове)."""
    global _gpu_manager
    if _gpu_manager is None:
        _gpu_manager = GPUResourceManager(max_concurrent=1)
        _gpu_manager.start()
    return _gpu_manager
