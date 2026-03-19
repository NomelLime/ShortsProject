"""
pipeline/contexts/instagram.py — Instagram: мобильный приоритет для Reels.

Instagram менее строг чем TikTok, но:
- Reels с мобильного UA получают приоритет в Explore
- Мобильный режим открывает трендовую музыку
- Десктопная загрузка работает, но с пониженным органическим reach

Стратегия: мобильная эмуляция без sensor API (не так строго как TikTok).
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

from pipeline.contexts.base import BasePlatformContext

_MOBILE_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--no-first-run",
    "--disable-default-apps",
]


class InstagramContext(BasePlatformContext):
    """Мобильный контекст для Instagram Reels."""

    platform_name = "instagram"

    def build_launch_kwargs(
        self,
        profile_dir: Path,
        fingerprint: Dict,
        proxy_config: Optional[Dict],
    ) -> Dict:
        kwargs: Dict = {
            "user_data_dir":       str(profile_dir),
            "headless":            False,
            "user_agent":          fingerprint["user_agent"],
            "viewport":            fingerprint["viewport"],
            "locale":              fingerprint["locale"],
            "timezone_id":         fingerprint["timezone_id"],
            "has_touch":           True,
            "is_mobile":           True,
            "device_scale_factor": fingerprint["pixel_ratio"],
            "args":                _MOBILE_ARGS,
        }
        if proxy_config:
            kwargs["proxy"] = proxy_config
        return kwargs

    def post_launch(self, context, fingerprint: Dict) -> None:
        self._apply_stealth_and_fp(context, fingerprint)

    def get_login_url(self) -> str:
        return "https://www.instagram.com/accounts/login/"

    def get_session_check_url(self) -> str:
        return "https://www.instagram.com/accounts/edit/"

    def get_redirect_markers(self) -> List[str]:
        return [
            "instagram.com/accounts/login",
            "/login/?next=",
        ]
