#!/usr/bin/env python3
"""
setup_account.py — Интерактивная настройка аккаунта.

Запуск:
    python setup_account.py

Программа запросит все необходимые данные и создаст структуру:
    accounts/<name>/
        config.json
        browser_profile/
        upload_queue/
            youtube/
            tiktok/
            instagram/
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


# ─────────────────────────────────────────────────────────────────────────────
# Утилиты ввода
# ─────────────────────────────────────────────────────────────────────────────

def _ask(prompt: str, default: str = "") -> str:
    """Запрашивает строку у пользователя. Если пусто — возвращает default."""
    suffix = f" [{default}]" if default else ""
    try:
        value = input(f"  {prompt}{suffix}: ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\nОтмена.")
        sys.exit(0)
    return value if value else default


def _ask_bool(prompt: str, default: bool = True) -> bool:
    """Запрашивает да/нет."""
    default_str = "Y/n" if default else "y/N"
    try:
        raw = input(f"  {prompt} [{default_str}]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print("\nОтмена.")
        sys.exit(0)
    if not raw:
        return default
    return raw in ("y", "yes", "да", "1")


def _ask_choice(prompt: str, choices: list[str], multi: bool = False) -> list[str]:
    """
    Запрашивает выбор из списка.
    multi=True — можно выбрать несколько через запятую.
    """
    print(f"  {prompt}")
    for i, c in enumerate(choices, 1):
        print(f"    {i}. {c}")
    try:
        raw = input("  Введите номер(а) через запятую: ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\nОтмена.")
        sys.exit(0)

    selected = []
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            idx = int(part) - 1
            if 0 <= idx < len(choices):
                selected.append(choices[idx])
    if not selected:
        # По умолчанию — всё
        return choices if multi else [choices[0]]
    return selected if multi else selected[:1]


# ─────────────────────────────────────────────────────────────────────────────
# Сборка конфига
# ─────────────────────────────────────────────────────────────────────────────

def _collect_proxy() -> dict:
    """Собирает настройки прокси."""
    use_proxy = _ask_bool("Использовать прокси для этого аккаунта?", default=True)
    if not use_proxy:
        return {}

    print()
    host     = _ask("Хост прокси (например: 1.2.3.4 или proxy.example.com)")
    port_str = _ask("Порт прокси", default="8080")
    username = _ask("Логин прокси (Enter — без авторизации)", default="")
    password = _ask("Пароль прокси", default="") if username else ""

    try:
        port = int(port_str)
    except ValueError:
        print(f"  ⚠ Некорректный порт «{port_str}», используется 8080.")
        port = 8080

    proxy: dict = {"host": host, "port": port}
    if username:
        proxy["username"] = username
        proxy["password"] = password
    return proxy


def build_config() -> tuple[str, dict]:
    """
    Интерактивно собирает конфиг аккаунта.
    Возвращает (имя_аккаунта, словарь_конфига).
    """
    PLATFORMS_ALL = ["youtube", "tiktok", "instagram"]

    print()
    print("=" * 55)
    print("  Настройка нового аккаунта ShortsProject")
    print("=" * 55)
    print()

    # 1. Имя аккаунта
    name = ""
    while not name:
        name = _ask("Имя аккаунта (латиница, без пробелов, например: acc_yt_01)")
        name = name.replace(" ", "_")
        if not name:
            print("  ⚠ Имя не может быть пустым.")

    # 2. Платформы
    print()
    platforms = _ask_choice(
        "Выберите платформы для этого аккаунта:",
        PLATFORMS_ALL,
        multi=True,
    )
    print(f"  ✓ Выбрано: {', '.join(platforms)}")

    # 3. Прокси
    print()
    proxy = _collect_proxy()
    if proxy:
        print(f"  ✓ Прокси: {proxy.get('host')}:{proxy.get('port')}")
    else:
        print("  ✓ Прокси не используется")

    # 4. User-Agent (опционально)
    print()
    custom_ua = _ask_bool("Задать кастомный User-Agent?", default=False)
    if custom_ua:
        ua = _ask(
            "User-Agent",
            default=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
    else:
        ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )

    cfg: dict = {
        "platforms": platforms,
        "user_agent": ua,
    }
    if proxy:
        cfg["proxy"] = proxy

    return name, cfg


# ─────────────────────────────────────────────────────────────────────────────
# Создание структуры аккаунта
# ─────────────────────────────────────────────────────────────────────────────

def create_account(accounts_root: str = "accounts") -> None:
    """Основная точка входа: собирает данные и записывает файлы на диск."""
    name, cfg = build_config()

    acc_dir = Path(accounts_root) / name
    config_path = acc_dir / "config.json"

    # Проверяем, не существует ли уже
    if config_path.exists():
        print()
        overwrite = _ask_bool(
            f"Аккаунт «{name}» уже существует. Перезаписать config.json?",
            default=False,
        )
        if not overwrite:
            print("Отмена — файл не изменён.")
            return

    # Создаём папки
    (acc_dir / "browser_profile").mkdir(parents=True, exist_ok=True)
    for platform in cfg["platforms"]:
        (acc_dir / "upload_queue" / platform).mkdir(parents=True, exist_ok=True)

    # Записываем config.json
    config_path.write_text(
        json.dumps(cfg, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print()
    print("=" * 55)
    print(f"  ✅ Аккаунт «{name}» создан!")
    print(f"  📁 Путь: {acc_dir.resolve()}")
    print(f"  📄 config.json:")
    print()
    print(json.dumps(cfg, ensure_ascii=False, indent=4))
    print()
    print("  Следующий шаг: запустите пайплайн — при первом запуске")
    print("  откроется браузер для ручного входа в аккаунт.")
    print("=" * 55)
    print()


if __name__ == "__main__":
    create_account()
