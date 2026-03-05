"""
pipeline/agents/sentinel.py — SENTINEL: мониторинг системы и авто-восстановление.

Следит за:
  - CPU / RAM / Disk (через psutil)
  - GPU температурой и VRAM (через nvidia-smi)
  - Ошибками других агентов (через AgentMemory)
  - Зависшими процессами ffmpeg

Авто-рестарт агентов:
  - При ERROR > 2 мин → пишет имя в AgentMemory["sentinel_restart_requests"]
  - DIRECTOR читает этот флаг в своём watchdog и вызывает restart_agent()
  - Не трогает WAITING (ждёт GPU) и IDLE (норма)

Алерты → Telegram.
"""
from __future__ import annotations

import logging
import subprocess
import time
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
_ERROR_RESTART_SEC = 120   # рестарт только если в ERROR дольше 2 мин


class Sentinel(BaseAgent):
    """
    Системный сторож. Запускается первым, останавливается последним.

    Авто-рестарт: при обнаружении агента в ERROR > _ERROR_RESTART_SEC —
    добавляет имя в AgentMemory["sentinel_restart_requests"].
    DIRECTOR обрабатывает этот список в своём watchdog-цикле.
    """

    def __init__(
        self,
        memory: Optional[AgentMemory] = None,
        notify: Any = None,
    ) -> None:
        super().__init__("SENTINEL", memory or get_memory(), notify)
        self._alert_cooldown: Dict[str, float] = {}
        self._alert_interval = 1800  # 30 мин между одинаковыми алертами

        # Когда агент впервые замечен в ERROR: {agent_name: timestamp}
        self._error_since: Dict[str, float] = {}

    def run(self) -> None:
        logger.info("[SENTINEL] Мониторинг запущен")
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
            sys_metrics, sys_alerts = self._check_system()
            metrics.update(sys_metrics)
            alerts.extend(sys_alerts)

            gpu_metrics, gpu_alerts = self._check_gpu()
            metrics.update(gpu_metrics)
            alerts.extend(gpu_alerts)

            agent_alerts = self._check_agents()
            alerts.extend(agent_alerts)

            ffmpeg_alert = self._check_stuck_processes()
            if ffmpeg_alert:
                alerts.append(ffmpeg_alert)

        except Exception as e:
            logger.debug("[SENTINEL] Ошибка цикла: %s", e)
        finally:
            self.memory.set("system_metrics", metrics, persist=False)
            self.report({"metrics": metrics, "active_alerts": len(alerts)})

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

            cpu  = psutil.cpu_percent(interval=1)
            ram  = psutil.virtual_memory()
            disk = psutil.disk_usage("/")

            metrics["cpu_pct"]      = cpu
            metrics["ram_pct"]      = ram.percent
            metrics["ram_used_gb"]  = round(ram.used  / 1e9, 1)
            metrics["ram_total_gb"] = round(ram.total / 1e9, 1)
            metrics["disk_pct"]     = disk.percent

            if cpu > _CPU_WARN_PCT:
                alerts.append(f"CPU {cpu:.0f}% > {_CPU_WARN_PCT}%")
            if ram.percent > _RAM_WARN_PCT:
                alerts.append(
                    f"RAM {ram.percent:.0f}% "
                    f"({metrics['ram_used_gb']:.1f}/{metrics['ram_total_gb']:.1f}GB)"
                )
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
                    temp       = int(parts[0])
                    vram_used  = int(parts[1])
                    vram_total = int(parts[2])
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
            pass
        except Exception as e:
            logger.debug("[SENTINEL] GPU check: %s", e)
        return metrics, alerts

    # ------------------------------------------------------------------
    # Проверка агентов + авто-рестарт через DIRECTOR
    # ------------------------------------------------------------------

    def _check_agents(self) -> list:
        """
        Проверяет статусы агентов.

        Логика:
          - WAITING  → агент ждёт GPU, не трогаем
          - IDLE     → всё нормально
          - ERROR > _ERROR_RESTART_SEC → запрашиваем рестарт у DIRECTOR
          - ERROR    → только предупреждение, ещё ждём

        Запрос рестарта: добавляем имя в memory["sentinel_restart_requests"].
        DIRECTOR обрабатывает список в _watchdog().
        """
        alerts: list = []
        now = time.time()

        try:
            statuses = self.memory.get_all_agent_statuses()

            for agent, status in statuses.items():
                status_upper = status.upper() if isinstance(status, str) else ""

                # Пропускаем себя
                if agent == self.name:
                    continue

                # Агент ждёт GPU — не трогаем
                if "WAITING" in status_upper:
                    # Сбрасываем счётчик ошибок если был
                    self._error_since.pop(agent, None)
                    continue

                # Агент в ERROR
                if "ERROR" in status_upper:
                    if agent not in self._error_since:
                        # Первый раз видим — запоминаем момент
                        self._error_since[agent] = now
                        alerts.append(f"агент {agent} в ERROR: {status}")
                        logger.warning(
                            "[SENTINEL] %s перешёл в ERROR, ждём %ds перед рестартом",
                            agent, _ERROR_RESTART_SEC,
                        )
                    else:
                        error_duration = now - self._error_since[agent]
                        if error_duration >= _ERROR_RESTART_SEC:
                            # Порог превышен — запрашиваем рестарт у DIRECTOR
                            self._request_restart(agent)
                            # Сбрасываем таймер (чтобы не спамить повторно)
                            self._error_since.pop(agent, None)
                        else:
                            remaining = int(_ERROR_RESTART_SEC - error_duration)
                            logger.debug(
                                "[SENTINEL] %s в ERROR %ds, рестарт через %ds",
                                agent, int(error_duration), remaining,
                            )
                else:
                    # Агент вышел из ERROR сам — сбрасываем таймер
                    if agent in self._error_since:
                        logger.info("[SENTINEL] %s вышел из ERROR самостоятельно", agent)
                        self._error_since.pop(agent)

        except Exception as e:
            logger.debug("[SENTINEL] agent check: %s", e)

        return alerts

    def _request_restart(self, agent_name: str) -> None:
        """
        Записывает запрос на рестарт в AgentMemory.
        DIRECTOR читает sentinel_restart_requests в _watchdog().
        """
        requests: list = self.memory.get("sentinel_restart_requests", [])
        if agent_name not in requests:
            requests.append(agent_name)
            self.memory.set("sentinel_restart_requests", requests)
            logger.info(
                "[SENTINEL] Запрошен рестарт агента %s (был в ERROR > %ds)",
                agent_name, _ERROR_RESTART_SEC,
            )
            self.memory.log_event(
                "SENTINEL", "restart_requested",
                {"agent": agent_name, "error_threshold_sec": _ERROR_RESTART_SEC},
            )
            self._send(
                f"🔁 [SENTINEL] Запрашиваю рестарт {agent_name} "
                f"(ERROR > {_ERROR_RESTART_SEC // 60} мин)"
            )

    # ------------------------------------------------------------------
    # Зависшие процессы
    # ------------------------------------------------------------------

    def _check_stuck_processes(self) -> Optional[str]:
        try:
            import psutil
            stuck = []
            for proc in psutil.process_iter(["name", "create_time"]):
                try:
                    if proc.info["name"] in ("ffmpeg", "ffmpeg.exe"):
                        age_min = (time.time() - proc.info["create_time"]) / 60
                        if age_min > 30:
                            stuck.append(round(age_min))
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            if stuck:
                return f"зависших ffmpeg: {len(stuck)} (до {max(stuck)} мин)"
        except ImportError:
            pass
        except Exception as e:
            logger.debug("[SENTINEL] ffmpeg check: %s", e)
        return None
