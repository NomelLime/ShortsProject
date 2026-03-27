"""
Обратная совместимость: реэкспорт из пакета shared_gpu_lock.

См. shared_gpu_lock/gpu_manager.py — PriorityQueue, семафор, кросс-процессный lock.
"""

from shared_gpu_lock.gpu_manager import GPUResourceManager, GPUPriority, get_gpu_manager

__all__ = ["GPUResourceManager", "GPUPriority", "get_gpu_manager"]
