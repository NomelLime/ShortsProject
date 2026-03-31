"""
METRICS_SCOUT_PLATFORM — нативный сбор метрик из кабинетов платформ.

Собирает метрики по загруженным видео (accounts/*) и пишет отдельный блок
`platform_native_metrics` в data/analytics.json.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from pipeline import config
from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agent_memory import AgentMemory, get_memory
from pipeline.analytics import _PLATFORM_COLLECTORS
from pipeline.browser import close_browser, launch_browser
from pipeline.session_manager import ensure_session_fresh, mark_session_verified
from pipeline.utils import get_all_accounts


class MetricsScoutPlatform(BaseAgent):
    def __init__(self, memory: Optional[AgentMemory] = None, notify: Any = None) -> None:
        super().__init__("METRICS_SCOUT_PLATFORM", memory or get_memory(), notify)
        self._interval_sec = max(1, int(config.METRICS_SCOUT_PLATFORM_INTERVAL_H)) * 3600
        self._cooldown_hours = max(1, int(config.METRICS_SCOUT_PLATFORM_COOLDOWN_H))

    def run(self) -> None:
        if not config.METRICS_SCOUT_PLATFORM_ENABLED:
            self._set_status(AgentStatus.IDLE, "отключён")
            return
        self._collect_cycle()
        while not self.should_stop:
            if self.memory.get("metrics_scout_platform_force"):
                self.memory.set("metrics_scout_platform_force", False)
                self._collect_cycle()
                continue
            if not self.sleep(self._interval_sec):
                break
            self._collect_cycle()

    def trigger_now(self) -> None:
        self.memory.set("metrics_scout_platform_force", True)

    def _collect_cycle(self) -> None:
        self._set_status(AgentStatus.RUNNING, "сбор platform_native_metrics")
        self.set_human_detail("Собираю нативные метрики видео из залогиненных кабинетов")
        analytics = self._load_analytics()
        platforms = ("youtube", "tiktok", "instagram")
        recs: List[Dict[str, Any]] = []
        cooldowns = self._get_cooldowns(analytics)

        for acc in get_all_accounts():
            acc_name = acc["name"]
            acc_cfg = acc.get("config", {})
            acc_platforms = [p.lower() for p in (acc.get("platforms") or [])]
            acc_dir = Path(acc["dir"])
            for platform in platforms:
                if platform not in acc_platforms:
                    continue
                if self._is_on_cooldown(cooldowns, acc_name, platform):
                    continue
                collector = _PLATFORM_COLLECTORS.get(platform)
                if not collector:
                    continue
                candidates = self._candidate_uploads(analytics, acc_name, platform)
                if not candidates:
                    continue

                profile_dir = acc_dir / "browser_profile"
                pw = ctx = None
                try:
                    pw, ctx = launch_browser(acc_cfg, profile_dir, platform=platform)
                    if not ensure_session_fresh(ctx, acc_name, platform):
                        self._set_cooldown(cooldowns, acc_name, platform, "login_invalid")
                        continue
                    mark_session_verified(acc_name, platform, valid=True)

                    for item in candidates:
                        page = ctx.new_page()
                        try:
                            stats = collector(page, item["url"])
                            if self._looks_like_challenge(page):
                                self._set_cooldown(cooldowns, acc_name, platform, "captcha_or_challenge")
                                break
                        finally:
                            try:
                                page.close()
                            except Exception:
                                pass

                        if not stats:
                            continue
                        enriched = self._enrich_stats(stats)
                        recs.append(
                            {
                                "video_stem": item["video_stem"],
                                "account_name": acc_name,
                                "platform": platform,
                                "url": item["url"],
                                "uploaded_at": item.get("uploaded_at"),
                                "collected_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                "metrics": enriched,
                            }
                        )
                        self.human_pause(1.5, 4.0, account_cfg=acc_cfg, context="metrics_scout_video_read")
                except RuntimeError:
                    self._set_cooldown(cooldowns, acc_name, platform, "proxy_unavailable")
                except Exception as exc:
                    self.logger.warning("[%s] %s/%s: %s", self.name, acc_name, platform, exc)
                finally:
                    if pw and ctx:
                        close_browser(pw, ctx)

        block = self._build_native_block(recs, cooldowns)
        analytics["platform_native_metrics"] = block
        self._save_analytics(analytics)
        self.report(
            {
                "updated_at": block.get("updated_at"),
                "videos_collected": block.get("videos_collected", 0),
                "platforms": list((block.get("by_platform") or {}).keys()),
            }
        )
        self._send_top3_summary(block)
        self._set_status(AgentStatus.IDLE)

    def _candidate_uploads(self, analytics: Dict[str, Any], acc_name: str, platform: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for video_stem, entry in analytics.items():
            if video_stem == "platform_native_metrics":
                continue
            if not isinstance(entry, dict):
                continue
            upload = (entry.get("uploads") or {}).get(platform) or {}
            if not isinstance(upload, dict):
                continue
            if upload.get("account_name") and upload.get("account_name") != acc_name:
                continue
            if not upload.get("url"):
                continue
            out.append(
                {
                    "video_stem": video_stem,
                    "url": upload.get("url"),
                    "uploaded_at": upload.get("uploaded_at"),
                }
            )
        out.sort(key=lambda x: x.get("uploaded_at") or "", reverse=True)
        return out[:20]

    def _build_native_block(self, records: List[Dict[str, Any]], cooldowns: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        by_platform: Dict[str, Dict[str, Any]] = {}
        for platform in ("youtube", "tiktok", "instagram"):
            p_rows = [r for r in records if r.get("platform") == platform]
            p_rows.sort(key=lambda x: x.get("uploaded_at") or "", reverse=True)
            recent_20 = p_rows[:20]
            top_3 = sorted(
                p_rows,
                key=lambda x: (
                    x.get("metrics", {}).get("views")
                    if x.get("metrics", {}).get("views") is not None
                    else (x.get("metrics", {}).get("likes", 0) + x.get("metrics", {}).get("comments", 0))
                ),
                reverse=True,
            )[:3]
            by_platform[platform] = {
                "recent_20": recent_20,
                "top_popular_3": top_3,
            }
        return {
            "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "collector": self.name,
            "videos_collected": len(records),
            "cooldowns": cooldowns,
            "by_platform": by_platform,
        }

    def _send_top3_summary(self, block: Dict[str, Any]) -> None:
        lines = ["📊 <b>METRICS_SCOUT_PLATFORM</b>"]
        for platform, pdata in (block.get("by_platform") or {}).items():
            top = (pdata or {}).get("top_popular_3") or []
            if not top:
                continue
            row = []
            for item in top:
                m = item.get("metrics") or {}
                views = m.get("views")
                views_label = f"{views:,}" if isinstance(views, int) else "n/a"
                row.append(f"{item.get('video_stem')} ({views_label})")
            lines.append(f"{platform.upper()}: " + " | ".join(row))
        if len(lines) > 1:
            self._send("\n".join(lines))

    def _enrich_stats(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        views = stats.get("views")
        likes = stats.get("likes")
        comments = stats.get("comments")
        engagement_rate = None
        if isinstance(views, int) and views > 0:
            engagement_rate = round(((likes or 0) + (comments or 0)) / views, 6)
        return {
            "views": views,
            "likes": likes,
            "comments": comments,
            "shares": stats.get("shares"),
            "saves": stats.get("saves"),
            "engagement_rate": engagement_rate,
        }

    def _looks_like_challenge(self, page) -> bool:
        try:
            txt = (page.content() or "").lower()
            markers = ("captcha", "verify you are human", "challenge", "security check")
            return any(m in txt for m in markers)
        except Exception:
            return False

    def _cooldown_until(self) -> str:
        return (datetime.now(timezone.utc) + timedelta(hours=self._cooldown_hours)).isoformat(timespec="seconds")

    def _get_cooldowns(self, analytics: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        block = analytics.get("platform_native_metrics") or {}
        cds = block.get("cooldowns")
        if isinstance(cds, dict):
            return cds
        return {}

    def _set_cooldown(self, cooldowns: Dict[str, Dict[str, Any]], account_name: str, platform: str, reason: str) -> None:
        cooldowns.setdefault(account_name, {})
        cooldowns[account_name][platform] = {
            "until": self._cooldown_until(),
            "reason": reason,
        }

    def _is_on_cooldown(self, cooldowns: Dict[str, Dict[str, Any]], account_name: str, platform: str) -> bool:
        entry = ((cooldowns.get(account_name) or {}).get(platform) or {})
        until = entry.get("until")
        if not until:
            return False
        try:
            dt = datetime.fromisoformat(str(until).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return datetime.now(timezone.utc) < dt
        except ValueError:
            return False

    def _load_analytics(self) -> Dict[str, Any]:
        path = config.ANALYTICS_FILE
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_analytics(self, data: Dict[str, Any]) -> None:
        path = config.ANALYTICS_FILE
        path.parent.mkdir(parents=True, exist_ok=True)
        text = json.dumps(data, ensure_ascii=False, indent=2)
        fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
        try:
            os.write(fd, text.encode("utf-8"))
            os.close(fd)
            os.replace(tmp, str(path))
        except Exception:
            try:
                os.close(fd)
            except Exception:
                pass
            try:
                os.unlink(tmp)
            except Exception:
                pass
            raise

