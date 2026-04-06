"""
pipeline/publish_bridge.py - Operator publish bridge for platform migration.

Bridge mode routes selected platforms to a manual/operator workflow while
preserving a unified status contract for downstream systems.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple

from pipeline import config, utils

STATUS_QUEUED = "queued"
STATUS_IN_PROGRESS = "in_progress"
STATUS_PUBLISHED = "published"
STATUS_FAILED_RETRYABLE = "failed_retryable"
STATUS_FAILED_TERMINAL = "failed_terminal"
STATUS_MANUAL_REQUIRED = "manual_required"
MANUAL_REQUIRED_SENTINEL = "__MANUAL_REQUIRED__"

_BRIDGE_DATA_FILE = Path(config.DATA_DIR) / "operator_publish_queue.json"
_BRIDGE_STATS_FILE = Path(config.DATA_DIR) / "operator_bridge_stats.json"


def bridge_enabled_for_platform(platform: str) -> bool:
    if not getattr(config, "PUBLISH_BRIDGE_ENABLED", False):
        return False
    enabled_set = {str(p).strip().lower() for p in getattr(config, "PUBLISH_BRIDGE_PLATFORMS", set())}
    return platform in enabled_set


def queue_manual_publish(
    *,
    platform: str,
    account_id: str,
    video_path: Path,
    meta: Dict,
    reason: str = "operator_bridge_active",
) -> Dict:
    now_iso = datetime.now(timezone.utc).isoformat()
    ticket_id = f"{account_id}:{platform}:{video_path.stem}:{int(datetime.now(timezone.utc).timestamp())}"
    payload = {
        "ticket_id": ticket_id,
        "status": STATUS_MANUAL_REQUIRED,
        "platform": platform,
        "account_id": account_id,
        "source_path": str(video_path),
        "meta": dict(meta or {}),
        "reason": reason,
        "created_at": now_iso,
    }

    data = utils.load_json(_BRIDGE_DATA_FILE) or {}
    queue = data.get("queue")
    if not isinstance(queue, list):
        queue = []
    queue.append(payload)
    data["queue"] = queue
    data["updated_at"] = now_iso
    utils.save_json(_BRIDGE_DATA_FILE, data)

    _update_operator_bridge_stats(last_status=STATUS_MANUAL_REQUIRED, platform=platform)
    return payload


def get_publish_handler_mode(platform: str) -> Tuple[str, bool]:
    """
    Returns (mode, fail_open).
    mode: shadow | active | fallback
    """
    mode = str(getattr(config, "PUBLISH_BRIDGE_MODE", "active")).strip().lower()
    if mode not in {"shadow", "active", "fallback"}:
        mode = "active"
    fail_open = bool(getattr(config, "PUBLISH_BRIDGE_FAIL_OPEN", True))
    if not bridge_enabled_for_platform(platform):
        return "legacy", fail_open
    return mode, fail_open


def _update_operator_bridge_stats(*, last_status: str, platform: str) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    stats = utils.load_json(_BRIDGE_STATS_FILE) or {}
    by_platform = stats.get("by_platform")
    if not isinstance(by_platform, dict):
        by_platform = {}
    bucket = by_platform.get(platform) or {
        "manual_required": 0,
        "published": 0,
        "failed_retryable": 0,
        "failed_terminal": 0,
        "retry_count": 0,
        "publish_durations_sec": [],
    }
    if last_status in bucket and isinstance(bucket.get(last_status), int):
        bucket[last_status] += 1
    if last_status == STATUS_FAILED_RETRYABLE:
        bucket["retry_count"] = int(bucket.get("retry_count", 0)) + 1
    by_platform[platform] = bucket
    stats["by_platform"] = by_platform
    stats["updated_at"] = now_iso
    utils.save_json(_BRIDGE_STATS_FILE, stats)

