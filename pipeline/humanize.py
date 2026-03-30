"""
Единая «человечность» для автоматических заходов на платформы.

- Уровень (осторожный / нормальный / агрессивный): env SHORTS_HUMANIZE_LEVEL и/или
  KV humanize_level / humanize_level_<AGENT> в AgentMemory (решают агенты / COMMANDER).
- Паузы масштабируются по локальному времени GEO аккаунта (как ACTIVITY_HOURS_*):
  в активном окне — короче, ночью — длиннее (аккаунт «спит»).
- Риск: LOW / MEDIUM / HIGH / CRITICAL — у финальных кликов (публикация) минимальный джиттер.

Логирование: при agent + memory — log_event для заметных пауз (см. пороги).
"""

from __future__ import annotations

import logging
import os
import random
import time
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional

from pipeline import config
from pipeline.fingerprint.geo import get_geo_params

if TYPE_CHECKING:
    from pipeline.agent_memory import AgentMemory

logger = logging.getLogger(__name__)


class HumanizeLevel(str, Enum):
    CAUTIOUS = "cautious"
    NORMAL = "normal"
    AGGRESSIVE = "aggressive"


class HumanizeRisk(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


# Множитель длительности паузы по уровню агента
_LEVEL_PAUSE_MULT: Dict[HumanizeLevel, float] = {
    HumanizeLevel.CAUTIOUS: 1.5,
    HumanizeLevel.NORMAL: 1.0,
    HumanizeLevel.AGGRESSIVE: 0.62,
}

_MAX_SINGLE_PAUSE_SEC = float(os.getenv("HUMANIZE_MAX_SINGLE_PAUSE_SEC", "95"))
_LOG_PAUSE_THRESHOLD_SEC = float(os.getenv("HUMANIZE_LOG_PAUSE_THRESHOLD_SEC", "2.8"))
_LOG_PAUSE_SAMPLE = float(os.getenv("HUMANIZE_LOG_PAUSE_SAMPLE", "0.09"))


def _memory_optional(memory: Any) -> Optional["AgentMemory"]:
    if memory is not None:
        return memory
    try:
        from pipeline.agent_memory import get_memory

        return get_memory()
    except Exception:
        return None


def resolve_humanize_level(agent_name: Optional[str], memory: Any = None) -> HumanizeLevel:
    mem = _memory_optional(memory)
    if mem and agent_name:
        key = f"humanize_level_{str(agent_name).upper()}"
        raw = mem.get(key)
        if isinstance(raw, str):
            try:
                return HumanizeLevel(raw.strip().lower())
            except ValueError:
                pass
        raw_g = mem.get("humanize_level")
        if isinstance(raw_g, str):
            try:
                return HumanizeLevel(raw_g.strip().lower())
            except ValueError:
                pass
    env_v = (os.getenv("SHORTS_HUMANIZE_LEVEL", "") or "normal").strip().lower()
    try:
        return HumanizeLevel(env_v)
    except ValueError:
        return HumanizeLevel.NORMAL


def geo_pause_multiplier(account_cfg: Optional[Dict[str, Any]]) -> float:
    """
    < 1 — «день/пик» в локальном GEO (паузы чуть короче).
    > 1 — ночь / вне окна (паузы длиннее).
    """
    if not account_cfg:
        return 1.0
    country = (account_cfg.get("country") or "US").upper().strip()
    if len(country) != 2:
        country = "US"
    tz_name = (get_geo_params(country).get("tz") or "UTC").strip()
    try:
        from zoneinfo import ZoneInfo

        now = datetime.now(ZoneInfo(tz_name))
        hour = now.hour
    except Exception:
        return 1.0

    start = int(getattr(config, "ACTIVITY_HOURS_START", 8))
    end = int(getattr(config, "ACTIVITY_HOURS_END", 23))
    in_window = start <= hour < end if start < end else hour >= start or hour < end

    peak_lo = int(os.getenv("HUMANIZE_PEAK_HOUR_START", "12"))
    peak_hi = int(os.getenv("HUMANIZE_PEAK_HOUR_END", "21"))
    peak_mult = float(os.getenv("HUMANIZE_PEAK_PAUSE_MULT", "0.88"))
    night_mult = float(os.getenv("HUMANIZE_NIGHT_PAUSE_MULT", "1.72"))
    day_mult = float(os.getenv("HUMANIZE_DAY_PAUSE_MULT", "0.96"))

    if not in_window:
        return night_mult
    if peak_lo <= hour < peak_hi:
        return peak_mult
    return day_mult


def human_pause(
    lo: float,
    hi: float,
    *,
    account_cfg: Optional[Dict[str, Any]] = None,
    agent: Optional[str] = None,
    memory: Any = None,
    risk: HumanizeRisk = HumanizeRisk.MEDIUM,
    context: str = "",
) -> float:
    """
    Пауза с гауссовым разбросом. Возвращает фактическую длительность (сек).
    """
    lo = float(lo)
    hi = float(max(lo, hi))

    if risk == HumanizeRisk.CRITICAL:
        d = max(0.04, random.uniform(0.1, 0.42))
        time.sleep(d)
        return d

    level = resolve_humanize_level(agent, memory)
    lvl_m = _LEVEL_PAUSE_MULT.get(level, 1.0)
    geo_m = geo_pause_multiplier(account_cfg)

    if risk == HumanizeRisk.HIGH:
        lvl_m = min(lvl_m, 1.05)
        geo_m = min(geo_m, 1.15)

    lo2 = lo * lvl_m * geo_m
    hi2 = hi * lvl_m * geo_m
    mid = random.uniform(lo2, hi2)
    sigma = max(0.12, (hi2 - lo2) * 0.18)
    delay = max(0.12, random.gauss(mid, sigma))
    delay = min(delay, _MAX_SINGLE_PAUSE_SEC)

    time.sleep(delay)

    mem = _memory_optional(memory) if agent else None
    if mem and agent:
        if delay >= _LOG_PAUSE_THRESHOLD_SEC or random.random() < _LOG_PAUSE_SAMPLE:
            try:
                mem.log_event(
                    str(agent).upper(),
                    "humanize_pause",
                    {
                        "sec": round(delay, 2),
                        "ctx": (context or "")[:120],
                        "risk": risk.value,
                        "level": resolve_humanize_level(agent, mem).value,
                        "geo_mult": round(geo_m, 3),
                    },
                )
            except Exception as exc:
                logger.debug("humanize log_event: %s", exc)

    return delay


def human_scroll_step(
    page: Any,
    *,
    account_cfg: Optional[Dict[str, Any]] = None,
    agent: Optional[str] = None,
    memory: Any = None,
    risk: HumanizeRisk = HumanizeRisk.LOW,
) -> None:
    """Один шаг колеса мыши + пауза между шагами."""
    direction = random.choice([1, 1, 1, -1])
    delta = random.randint(280, 920) * direction
    try:
        page.mouse.wheel(0, delta)
    except Exception:
        return
    human_pause(0.35, 1.45, account_cfg=account_cfg, agent=agent, memory=memory, risk=risk, context="scroll_step")


def human_scroll_burst(
    page: Any,
    scrolls: Optional[int] = None,
    *,
    account_cfg: Optional[Dict[str, Any]] = None,
    agent: Optional[str] = None,
    memory: Any = None,
) -> None:
    """Несколько шагов скролла как при просмотре ленты."""
    count = scrolls if scrolls is not None else random.randint(3, 8)
    for _ in range(max(1, count)):
        human_scroll_step(page, account_cfg=account_cfg, agent=agent, memory=memory, risk=HumanizeRisk.LOW)


def log_throttle_wait(
    memory: Any,
    agent: str,
    seconds: float,
    context: str = "",
) -> None:
    """Для длинных ожиданий (антиспам между заливками и т.п.)."""
    mem = _memory_optional(memory)
    if not mem or seconds < 8:
        return
    try:
        mem.log_event(
            agent.upper(),
            "humanize_throttle",
            {"sec": round(seconds, 1), "ctx": (context or "")[:120]},
        )
    except Exception:
        pass
