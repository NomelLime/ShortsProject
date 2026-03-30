"""
Ротация аккаунта пайплайна (поиск / yt-dlp cookies / браузер) по LRU для SCOUT и TREND_SCOUT.

Если заданы SHORTS_PIPELINE_ACCOUNT или YTDLP_COOKIES_ACCOUNT — поведение как раньше (фиксированный аккаунт).

Иначе при PIPELINE_ACCOUNT_ROTATION=1 каждый цикл SCOUT или TREND_SCOUT выбирает аккаунт
с наименьшим временем последнего использования; метки хранятся в AgentMemory kv
``pipeline_account_scout_lru`` (общая очередь для обоих агентов).

Пул кандидатов: PIPELINE_ACCOUNT_POOL (через запятую) или все подкаталоги accounts/* с config.json.
"""
from __future__ import annotations

import contextvars
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, List, Optional

logger = logging.getLogger(__name__)

_SCOUT_PIPELINE_CTX: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "scout_pipeline_account",
    default=None,
)

KV_LAST_USED = "pipeline_account_scout_lru"
_EPOCH = "1970-01-01T00:00:00+00:00"


def _accounts_root() -> Path:
    from pipeline import config

    r = Path(config.ACCOUNTS_ROOT)
    return r if r.is_absolute() else (Path(config.BASE_DIR) / r)


def _validate_account_name(name: str) -> bool:
    n = (name or "").strip()
    if not n or "/" in n or "\\" in n or ".." in n:
        return False
    return (_accounts_root() / n / "config.json").is_file()


def rotation_enabled() -> bool:
    v = os.getenv("PIPELINE_ACCOUNT_ROTATION", "").strip().lower()
    return v in ("1", "true", "yes", "on", "scout")


def _env_pinned_account_name() -> Optional[str]:
    raw = (
        os.getenv("SHORTS_PIPELINE_ACCOUNT", "").strip()
        or os.getenv("YTDLP_COOKIES_ACCOUNT", "").strip()
    )
    if not raw:
        return None
    if _validate_account_name(raw):
        return raw
    logger.warning(
        "[pipeline_rotation] SHORTS_PIPELINE_ACCOUNT / YTDLP_COOKIES_ACCOUNT=%s: нет валидного config.json",
        raw,
    )
    return None


def _pool_from_env() -> Optional[List[str]]:
    raw = os.getenv("PIPELINE_ACCOUNT_POOL", "").strip()
    if not raw:
        return None
    return [p.strip() for p in raw.split(",") if p.strip()]


def eligible_pipeline_accounts() -> List[str]:
    """Имена папок в accounts/ с config.json, пересечённые с POOL если задан."""
    root = _accounts_root()
    if not root.is_dir():
        return []

    explicit = _pool_from_env()
    if explicit:
        names = [n for n in explicit if _validate_account_name(n)]
        if len(names) < len(explicit):
            missing = set(explicit) - set(names)
            logger.warning("[pipeline_rotation] PIPELINE_ACCOUNT_POOL: пропуск невалидных: %s", missing)
        return sorted(names)

    out: List[str] = []
    try:
        for p in sorted(root.iterdir()):
            if p.is_dir() and (p / "config.json").is_file():
                out.append(p.name)
    except OSError as exc:
        logger.warning("[pipeline_rotation] Не читается %s: %s", root, exc)
    return out


def pick_lru_pipeline_account(memory: Any) -> Optional[str]:
    """Аккаунт с минимальным last_used (ISO); при равенстве лексикографически меньший."""
    candidates = eligible_pipeline_accounts()
    if not candidates:
        logger.warning("[pipeline_rotation] Нет кандидатов для ротации (accounts / POOL)")
        return None

    lu: Any = {}
    try:
        lu = memory.get(KV_LAST_USED) or {}
    except Exception:
        lu = {}
    if not isinstance(lu, dict):
        lu = {}

    def sort_key(name: str) -> tuple:
        ts = lu.get(name)
        if not isinstance(ts, str):
            ts = _EPOCH
        return (ts, name)

    chosen = sorted(candidates, key=sort_key)[0]
    logger.info("[pipeline_rotation] LRU выбран аккаунт пайплайна: %s", chosen)
    return chosen


def touch_pipeline_account(memory: Any, name: str) -> None:
    if not memory or not name:
        return
    try:
        lu = memory.get(KV_LAST_USED) or {}
        if not isinstance(lu, dict):
            lu = {}
        lu = dict(lu)
        lu[name] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        memory.set(KV_LAST_USED, lu)
    except Exception as exc:
        logger.debug("[pipeline_rotation] touch не записан: %s", exc)


def get_active_pipeline_account_name() -> Optional[str]:
    """Имя аккаунта из контекста цикла SCOUT (если установлено и валидно)."""
    n = _SCOUT_PIPELINE_CTX.get()
    if not n:
        return None
    if _validate_account_name(n):
        return n
    return None


@contextmanager
def scout_pipeline_cycle_account(memory: Optional[Any]) -> Iterator[None]:
    """
    В начале цикла SCOUT / TREND_SCOUT: при включённой ротации и без pinned env —
    выбрать LRU и привязать к потоку. В конце — обновить метку last_used.

    Используется также агентом TrendScout (тот же контекст и KV, что и у SCOUT).
    """
    if _env_pinned_account_name():
        yield
        return
    if not rotation_enabled():
        yield
        return
    if memory is None:
        yield
        return

    name = pick_lru_pipeline_account(memory)
    if not name:
        yield
        return

    token = _SCOUT_PIPELINE_CTX.set(name)
    try:
        yield
    finally:
        _SCOUT_PIPELINE_CTX.reset(token)
        touch_pipeline_account(memory, name)
