"""
pipeline/contexts/tiktok.py — TikTok: максимальная мобильная эмуляция.

TikTok — самая строгая платформа по антидетекту:
- Проверяет Canvas/WebGL fingerprint
- Детектит эмуляторы по touch events и sensor API
- Зависит от IP-геолокации / timezone соответствия
- Десктопные аккаунты понижаются в For You Page

Стратегия: полная мобильная эмуляция + touch events + sensor API stubs.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

from pipeline.contexts.base import BasePlatformContext

_MOBILE_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--enable-features=NetworkService",
    "--disable-features=TranslateUI",
    "--disable-sync",
    "--no-first-run",
    "--disable-default-apps",
]


class TikTokContext(BasePlatformContext):
    """Мобильный контекст для TikTok с максимальной эмуляцией."""

    platform_name = "tiktok"

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
            "has_touch":           True,    # ← критично для TikTok
            "is_mobile":           True,
            "device_scale_factor": fingerprint["pixel_ratio"],
            "args":                _MOBILE_ARGS,
        }
        if proxy_config:
            kwargs["proxy"] = proxy_config
        return kwargs

    def post_launch(self, context, fingerprint: Dict) -> None:
        self._apply_stealth_and_fp(context, fingerprint)

        # TikTok-специфичные инъекции: Sensor API stubs
        context.add_init_script("""
        (() => {
            // TikTok проверяет наличие Sensor APIs на мобильных устройствах
            if (!window.DeviceMotionEvent) {
                window.DeviceMotionEvent = class extends Event {
                    constructor(t, i) { super(t, i); }
                    get acceleration() { return null; }
                    get rotationRate() { return null; }
                    get interval() { return 0; }
                };
            }
            if (!window.DeviceOrientationEvent) {
                window.DeviceOrientationEvent = class extends Event {
                    constructor(t, i) { super(t, i); }
                    get alpha() { return 0; }
                    get beta()  { return 0; }
                    get gamma() { return 0; }
                };
            }
            // Touch support markers
            try {
                Object.defineProperty(navigator, 'msMaxTouchPoints', { get: () => 5 });
            } catch(_) {}
            window.ontouchstart = null;
        })();
        """)

    def get_login_url(self) -> str:
        return "https://www.tiktok.com/login"

    def get_session_check_url(self) -> str:
        return "https://www.tiktok.com/upload"

    def get_redirect_markers(self) -> List[str]:
        return ["tiktok.com/login", "/login?redirect"]
