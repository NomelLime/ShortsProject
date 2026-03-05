"""
pipeline/agents/sentinel.py — SENTINEL: мониторинг системы и авто-восстановление.

Следит за:
  - CPU / RAM / Disk (через psutil)
  - GPU температурой и VRAM (через nvidia-smi)
  - Ошибками других агентов (через AgentMemory)
  - Зависшими процессами ffmpeg

Алерты → Telegram.
Авто-восстановление → через memory флаги для DIRECTOR.
"""
from __future__ import annotations

import logging
import subprocess
from typing import Any, Dict, Optional

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

_CHECK_INTERVAL    = 120   # 2 минуты
_CPU_WARN_PCT      = 90
_RAM_WARN_PCT      = 85
_DISK_WARN_PCT     = 90
_GPU_TEMP_WARN_C   = 85
_VRAM_WARN_PCT     = 90


class Sentinel(BaseAgent):
    """
    Системный сторож. Работает независимо от всех остальных агентов.
    Запускается первым, останавливается последним.
    """

    def __init__(
        self,
        memory: Optional[AgentMemory] = None,
        notify: Any = None,
    ) -> None:
        super().__init__("SENTINEL", memory or get_memory(), notify)
        self._alert_cooldown: Dict[str, float] = {}  # алёрт → последнее время
        self._alert_interval = 1800  # 30 мин между одинаковыми алертами

    def run(self) -> None:
        logger.info("[SENTINEL] Мониторинг запущен")
        import time
        while not self.should_stop:
            self._monitor_cycle()
            if not self.sleep(_CHECK_INTERVAL):
                break

    # ------------------------------------------------------------------
    # Полный цикл мониторинга
    # ------------------------------------------------------------------

    def _monitor_cycle(self) -> None:
        self._set_status(AgentStatus.RUNNING, "мониторинг")
        metrics = {}
        alerts  = []

        try:
            # CPU / RAM / Disk
            sys_metrics, sys_alerts = self._check_system()
            metrics.update(sys_metrics)
            alerts.extend(sys_alerts)

            # GPU
            gpu_metrics, gpu_alerts = self._check_gpu()
            metrics.update(gpu_metrics)
            alerts.extend(gpu_alerts)

            # Агенты
            agent_alerts = self._check_agents()
            alerts.extend(agent_alerts)

            # Зависший ffmpeg
            ffmpeg_alert = self._check_stuck_processes()
            if ffmpeg_alert:
                alerts.append(ffmpeg_alert)

        except Exception as e:
            logger.debug("[SENTINEL] Ошибка цикла: %s", e)
        finally:
            # Сохраняем метрики
            self.memory.set("system_metrics", metrics, persist=False)
            self.report({"metrics": metrics, "active_alerts": len(alerts)})

            # Отправляем новые алерты
            import time
            now = time.time()
            for alert in alerts:
                key = alert[:40]
                if now - self._alert_cooldown.get(key, 0) > self._alert_interval:
                    self._send(f"⚠️ [SENTINEL] {alert}")
                    self._alert_cooldown[key] = now

            self._set_status(AgentStatus.IDLE)

    # ------------------------------------------------------------------
    # Системные ресурсы
    # ------------------------------------------------------------------

    def _check_system(self):
        metrics = {}
        alerts  = []
        try:
            import psutil

            cpu = psutil.cpu_percent(interval=1)
            ram = psutil.virtual_memory()
            disk = psutil.disk_usage("/")

            metrics["cpu_pct"]  = cpu
            metrics["ram_pct"]  = ram.percent
            metrics["ram_used_gb"] = round(ram.used / 1e9, 1)
            metrics["ram_total_gb"] = round(ram.total / 1e9, 1)
            metrics["disk_pct"] = disk.percent

            if cpu > _CPU_WARN_PCT:
                alerts.append(f"CPU {cpu:.0f}% > {_CPU_WARN_PCT}%")
            if ram.percent > _RAM_WARN_PCT:
                alerts.append(f"RAM {ram.percent:.0f}% ({metrics['ram_used_gb']:.1f}/{metrics['ram_total_gb']:.1f}GB)")
            if disk.percent > _DISK_WARN_PCT:
                alerts.append(f"Диск {disk.percent:.0f}%")

        except ImportError:
            logger.debug("[SENTINEL] psutil не установлен")
        except Exception as e:
            logger.debug("[SENTINEL] sys check: %s", e)
        return metrics, alerts

    # ------------------------------------------------------------------
    # GPU мониторинг
    # ------------------------------------------------------------------

    def _check_gpu(self):
        metrics = {}
        alerts  = []
        try:
            result = subprocess.run(
                ["nvidia-smi",
                 "--query-gpu=temperature.gpu,memory.used,memory.total,utilization.gpu",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                parts = result.stdout.strip().split(", ")
                if len(parts) >= 3:
                    temp     = int(parts[0])
                    vram_used  = int(parts[1])   # MB
                    vram_total = int(parts[2])   # MB
                    gpu_util   = int(parts[3]) if len(parts) > 3 else 0
                    vram_pct   = round(vram_used / vram_total * 100, 1) if vram_total else 0

                    metrics["gpu_temp_c"]    = temp
                    metrics["vram_used_mb"]  = vram_used
                    metrics["vram_total_mb"] = vram_total
                    metrics["vram_pct"]      = vram_pct
                    metrics["gpu_util_pct"]  = gpu_util

                    if temp > _GPU_TEMP_WARN_C:
                        alerts.append(f"GPU температура {temp}°C > {_GPU_TEMP_WARN_C}°C")
                    if vram_pct > _VRAM_WARN_PCT:
                        alerts.append(f"VRAM {vram_pct:.0f}% ({vram_used}/{vram_total}MB)")

        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass  # nvidia-smi не установлен
        except Exception as e:
            logger.debug("[SENTINEL] GPU check: %s", e)
        return metrics, alerts

    # ------------------------------------------------------------------
    # Проверка агентов
    # ------------------------------------------------------------------

    def _check_agents(self):
        alerts = []
        try:
            statuses = self.memory.get_all_agent_statuses()
            for agent, status in statuses.items():
                if "ERROR" in status.upper():
                    alerts.append(f"агент {agent} в ERROR: {status}")
        except Exception as e:
            logger.debug("[SENTINEL] agent check: %s", e)
        return alerts

    # ------------------------------------------------------------------
    # Зависшие процессы
    # ------------------------------------------------------------------

    def _check_stuck_processes(self) -> Optional[str]:
        try:
            import psutil
            stuck_ffmpeg = []
            for proc in psutil.process_iter(["name", "cpu_percent", "create_time"]):
                try:
                    if proc.info["name"] in ("ffmpeg", "ffmpeg.exe"):
                        import time
                        age_min = (time.time() - proc.info["create_time"]) / 60
                        if age_min > 30:  # висит > 30 минут
                            stuck_ffmpeg.append(round(age_min))
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            if stuck_ffmpeg:
                return f"зависших ffmpeg процессов: {len(stuck_ffmpeg)} (до {max(stuck_ffmpeg)} мин)"
        except ImportError:
            pass
        except Exception as e:
            logger.debug("[SENTINEL] ffmpeg check: %s", e)
        return None
