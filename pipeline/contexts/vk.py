"""
pipeline/contexts/vk.py - Desktop context for VK Video.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

from pipeline.contexts.base import BasePlatformContext

_COMMON_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-default-apps",
    "--no-first-run",
]


class VkContext(BasePlatformContext):
    platform_name = "vk"

    def build_launch_kwargs(
        self,
        profile_dir: Path,
        fingerprint: Dict,
        proxy_config: Optional[Dict],
    ) -> Dict:
        kwargs: Dict = {
            "user_data_dir": str(profile_dir),
            "headless": False,
            "user_agent": fingerprint["user_agent"],
            "viewport": fingerprint["viewport"],
            "locale": fingerprint["locale"],
            "timezone_id": fingerprint["timezone_id"],
            "has_touch": False,
            "is_mobile": False,
            "device_scale_factor": 1,
            "args": _COMMON_ARGS,
        }
        if proxy_config:
            kwargs["proxy"] = proxy_config
        return kwargs

    def post_launch(self, context, fingerprint: Dict) -> None:
        self._apply_stealth_and_fp(context, fingerprint)

    def get_login_url(self) -> str:
        return "https://id.vk.com/auth"

    def get_session_check_url(self) -> str:
        return "https://vk.com/video"

    def get_redirect_markers(self) -> List[str]:
        return ["login.vk.com", "vk.com/login"]

