"""
pipeline/contexts/base.py — Базовый класс платформенного браузерного контекста.

Каждая платформа (YouTube, TikTok, Instagram) наследует и переопределяет
параметры запуска под специфику своей антидетект-системы.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional


class BasePlatformContext(ABC):
    """
    Интерфейс платформенного браузерного контекста.

    Отвечает за:
    - Генерацию kwargs для launch_persistent_context()
    - Post-launch инициализацию (stealth, fingerprint инъекции)
    - URL для логина и проверки сессии
    """

    platform_name: str = ""

    @abstractmethod
    def build_launch_kwargs(
        self,
        profile_dir: Path,
        fingerprint: Dict,
        proxy_config: Optional[Dict],
    ) -> Dict:
        """
        Возвращает kwargs для playwright launch_persistent_context().

        Args:
            profile_dir:  путь к директории профиля браузера
            fingerprint:  dict от generate_fingerprint()
            proxy_config: dict для playwright proxy или None

        Returns:
            dict с параметрами для launch_persistent_context()
        """
        ...

    @abstractmethod
    def post_launch(self, context, fingerprint: Dict) -> None:
        """
        Действия после создания контекста.

        Применяет stealth, fingerprint инъекции, платформенные патчи.
        Вызывается ДО открытия первой страницы.

        Args:
            context:     playwright BrowserContext
            fingerprint: dict от generate_fingerprint()
        """
        ...

    def get_login_url(self) -> str:
        """URL для ручного логина пользователя."""
        return ""

    def get_session_check_url(self) -> str:
        """URL для проверки что сессия валидна."""
        return ""

    def get_redirect_markers(self) -> List[str]:
        """URL-маркеры редиректа на форму логина."""
        return []

    def _apply_stealth_and_fp(self, context, fingerprint: Dict) -> None:
        """
        Общий helper: применяет playwright-stealth + fingerprint инъекции.

        Используется во всех платформенных реализациях.
        """
        try:
            from playwright_stealth import Stealth
            stealth = Stealth()
            for page in context.pages:
                stealth.apply_stealth_sync(page)
            context.on("page", lambda p: stealth.apply_stealth_sync(p))
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(
                "[%s] playwright-stealth не применён: %s", self.platform_name, e
            )

        from pipeline.fingerprint.injector import apply_fingerprint
        apply_fingerprint(context, fingerprint)
