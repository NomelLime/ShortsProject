"""Предзагрузка VL-модели (ollama keep_alive) перед batch."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def warm_vl_model() -> None:
    """Пинг VL-модели с keep_alive=30m; ошибки игнорируются."""
    try:
        import ollama

        from pipeline import config

        ollama.chat(
            model=config.OLLAMA_MODEL,
            messages=[{"role": "user", "content": "ping"}],
            keep_alive="30m",
            options={"num_predict": 1},
        )
    except Exception:
        pass


def register_gpu_warm_callback() -> None:
    """Регистрирует warm callback на глобальном GPU manager."""
    from shared_gpu_lock.gpu_manager import get_gpu_manager

    def _warm_for_consumer(consumer: str) -> None:
        cl = consumer.lower()
        if not any(x in cl for x in ("slicer", "activity_vl", "curator", "scout", "visionary")):
            return
        warm_vl_model()

    gpu = get_gpu_manager()
    if getattr(gpu, "_warm_callback", None) is None:
        gpu._warm_callback = _warm_for_consumer
