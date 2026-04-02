"""
pipeline/contexts/youtube.py — YouTube: десктоп, Studio-оптимизированный.

YouTube — наиболее лояльная к десктопу платформа:
- Studio работает отлично с десктопного Chrome
- Не проверяет мобильность агрессивно
- Основная задача — скрыть автоматизацию (webdriver), не мобилизация

Стратегия: десктопный Chrome + stealth + fingerprint инъекции.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

from pipeline.contexts.base import BasePlatformContext

_COMMON_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-extensions-except=",
    "--disable-default-apps",
    "--no-first-run",
]


class YouTubeContext(BasePlatformContext):
    """Desktop-контекст для YouTube/Studio (стабильнее для Google Login)."""

    platform_name = "youtube"

    def build_launch_kwargs(
        self,
        profile_dir: Path,
        fingerprint: Dict,
        proxy_config: Optional[Dict],
    ) -> Dict:
        kwargs: Dict = {
            "user_data_dir":     str(profile_dir),
            "headless":          False,
            "user_agent":        fingerprint["user_agent"],
            "viewport":          fingerprint["viewport"],
            "locale":            fingerprint["locale"],
            "timezone_id":       fingerprint["timezone_id"],
            # Для Google Login мобильная эмуляция чаще даёт "browser not secure".
            "has_touch":         False,
            "is_mobile":         False,
            "device_scale_factor": 1,
            "args":              _COMMON_ARGS,
        }
        if proxy_config:
            kwargs["proxy"] = proxy_config
        return kwargs

    def post_launch(self, context, fingerprint: Dict) -> None:
        self._apply_stealth_and_fp(context, fingerprint)

    def get_login_url(self) -> str:
        return "https://accounts.google.com/ServiceLogin"

    def get_session_check_url(self) -> str:
        return "https://studio.youtube.com"

    def get_redirect_markers(self) -> List[str]:
        return [
            "accounts.google.com/signin",
            "accounts.google.com/ServiceLogin",
            "accounts.google.com/v3/signin",
        ]
