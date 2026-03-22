"""
Сводка по активным прогревам заливки (лог / stdout / опционально Telegram).

Запуск: python -m pipeline.warmup_report
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from pipeline import config
from pipeline.upload_warmup import WARMUP_FILENAME, load_account_config

logger = logging.getLogger(__name__)


def collect_warmup_rows() -> List[Tuple[str, str, str, str]]:
    """
    Список (account, platform, until_iso, scope) для активных прогревов.
    """
    rows: List[Tuple[str, str, str, str]] = []
    root = Path(config.ACCOUNTS_ROOT)
    if not root.exists():
        return rows
    now = datetime.now(timezone.utc)
    for acc_dir in sorted(root.iterdir()):
        if not acc_dir.is_dir():
            continue
        wpath = acc_dir / WARMUP_FILENAME
        if not wpath.exists():
            continue
        acc_cfg = load_account_config(acc_dir)
        if acc_cfg.get("skip_upload_warmup") is True:
            continue
        try:
            data = json.loads(wpath.read_text(encoding="utf-8"))
        except Exception:
            continue
        for plat, info in (data.get("platforms") or {}).items():
            until_s = info.get("upload_allowed_after")
            if not until_s:
                continue
            try:
                until = datetime.fromisoformat(until_s)
                if until.tzinfo is None:
                    until = until.replace(tzinfo=timezone.utc)
            except Exception:
                continue
            if now >= until:
                continue
            sc = info.get("scope") or "?"
            rows.append((acc_dir.name, plat, until_s, str(sc)))
    return rows


def format_warmup_report_text() -> str:
    rows = collect_warmup_rows()
    if not rows:
        return "Прогрев заливки: активных окон нет."
    lines = ["Прогрев заливки (аккаунт / платформа / до UTC / scope):"]
    for acc, plat, until_s, sc in rows:
        lines.append(f"  • {acc}  {plat}  до {until_s}  ({sc})")
    lines.append(f"Всего: {len(rows)} записей.")
    return "\n".join(lines)


def log_warmup_dashboard(log: logging.Logger | None = None) -> None:
    """Пишет сводку в лог построчно (удобно при старте пайплайна)."""
    lg = log or logger
    for line in format_warmup_report_text().split("\n"):
        lg.info("[warmup] %s", line)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(format_warmup_report_text())


if __name__ == "__main__":
    main()
