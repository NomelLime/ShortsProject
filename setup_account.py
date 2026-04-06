#!/usr/bin/env python3
"""
setup_account.py — Интерактивная настройка аккаунта.

Запуск:
    python setup_account.py

Программа запросит данные и создаст структуру:
    accounts/<name>/
        config.json
        browser_profile/

Прокси: только mobileproxy.space (MOBILEPROXY_API_KEY + MOBILEPROXY_PROXY_ID в .env).
Параметры host/port/login не дублируются в config — берутся из API при запуске пайплайна.
        upload_queue/
            vk/
            rutube/
            ok/
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

from pipeline import config as project_config


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


def _mobileproxy_supported_iso_set() -> frozenset[str] | None:
    """
    ISO2 из маппинга MOBILEPROXY (get_id_country + MOBILEPROXY_ISO_TO_ID_JSON).
    None — нет ключей в .env, продолжать нельзя.
    Пустой frozenset — список из API пуст; страну всё равно спрашиваем, проверка ниже.
    """
    from pipeline.mobileproxy_connection import mobileproxy_env_configured
    from pipeline.mobileproxy_api import list_supported_iso2_codes

    if not mobileproxy_env_configured():
        print(
            "  ❌ Задайте в .env MOBILEPROXY_API_KEY и MOBILEPROXY_PROXY_ID.\n"
            f"     Документация API: {project_config.MOBILEPROXY_API_DOCS_URL}"
        )
        return None
    codes = list_supported_iso2_codes()
    if not codes:
        print(
            "  ⚠ Список стран из API пуст (get_id_country не распознан или нет ключей). "
            "Задайте MOBILEPROXY_ISO_TO_ID_JSON или проверьте ключ API.\n"
            "     Введите целевой ISO ниже — проверка будет при шаге прокси.\n"
            f"     Справка: {project_config.MOBILEPROXY_API_DOCS_URL}"
        )
        return frozenset()
    print(
        "  Разрешённые ISO для смены линии (mobileproxy.space, command=get_id_country):"
    )
    for i in range(0, len(codes), 16):
        print("    " + ", ".join(codes[i : i + 16]))
    return frozenset(codes)


# ─────────────────────────────────────────────────────────────────────────────
# Сборка конфига
# ─────────────────────────────────────────────────────────────────────────────

def build_config() -> tuple[str, dict] | tuple[None, None]:
    """
    Интерактивно собирает конфиг аккаунта.
    Возвращает (имя_аккаунта, словарь_конфига) или (None, None) при отмене / ошибке проверки прокси.
    """
    PLATFORMS_ALL = ["vk", "rutube", "ok"]

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

    # 3. Страна аккаунта (обязательно; GEO + mobileproxy API)
    print()
    print("  Страна аккаунта — обязательный ISO-код (прокси и отпечаток под это ГЕО).")
    supported_iso = _mobileproxy_supported_iso_set()
    if supported_iso is None:
        return None, None

    country = ""
    while not country:
        country_raw = _ask("Страна (ISO, напр: US, DE, GB)", default="").upper().strip()
        if len(country_raw) != 2 or not country_raw.isalpha():
            print("  ⚠ Нужен двухбуквенный латинский код ISO, например US.")
            continue
        if supported_iso and country_raw not in supported_iso:
            print(
                f"  ⚠ ISO «{country_raw}» нет в списке стран MOBILEPROXY для вашей линии. "
                "Выберите код из списка выше или добавьте пару в MOBILEPROXY_ISO_TO_ID_JSON.\n"
                f"     Документация: {project_config.MOBILEPROXY_API_DOCS_URL}"
            )
            continue
        country = country_raw
    print(f"  ✓ Страна: {country}")

    # 4. Прокси: только mobileproxy API (get_my_proxy), без ручного host/port
    print()
    print("  Проверка прокси mobileproxy.space (get_my_proxy по .env)…")
    from pipeline.mobileproxy_connection import verify_mobileproxy_for_new_account

    if not verify_mobileproxy_for_new_account(country):
        return None, None

    # 5. User-Agent (опционально)
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
        "country": country,
        "proxy_source": "mobileproxy",
    }

    # ── 6. PreLend URL (параметры HTTP-прокси не храним в config — только API/кэш) ──
    print()
    prelend_url = _ask(
        "PreLend URL (ссылка для bio/профиля, Enter — пропустить)",
        default="",
    )
    if prelend_url:
        if not prelend_url.startswith("http"):
            prelend_url = "https://" + prelend_url
        cfg["prelend_url"] = prelend_url
        print(f"  ✓ PreLend URL: {prelend_url}")

        # Автогенерация per-platform bio URL с UTM через Nginx rewrites
        from urllib.parse import urlparse
        parsed      = urlparse(prelend_url)
        base_domain = f"{parsed.scheme}://{parsed.netloc}"

        _platform_paths = {
            "tiktok":    "/t/",
            "instagram": "/i/",
            "youtube":   "/y/",
        }

        prelend_urls: dict = {}
        for _p in platforms:
            _path = _platform_paths.get(_p, "/go/")
            prelend_urls[_p] = f"{base_domain}{_path}{name}"

        cfg["prelend_urls"] = prelend_urls
        print("  ✓ Bio-ссылки (с UTM):")
        for _p, _u in prelend_urls.items():
            print(f"      {_p}: {_u}")

        # ── 7. Bio текст ─────────────────────────────────────────────────
        print()
        print("  Bio/описание профиля (можно задать общий и per-platform):")
        bio = _ask("  Общий текст bio (Enter — пропустить)", default="")
        if bio:
            cfg["bio_text"] = bio

        for platform in platforms:
            platform_bio = _ask(
                f"  Bio для {platform} (Enter — использовать общий)",
                default="",
            )
            if platform_bio:
                cfg[f"bio_text_{platform}"] = platform_bio
    else:
        print("  ✓ PreLend URL не задан (можно добавить позже в config.json)")

    return name, cfg


# ─────────────────────────────────────────────────────────────────────────────
# Создание структуры аккаунта
# ─────────────────────────────────────────────────────────────────────────────

def _project_root() -> Path:
    return Path(__file__).resolve().parent


def _maybe_seed_keywords_txt() -> None:
    """Предлагает создать data/keywords.txt из examples/keywords.example.txt (первичный запуск)."""
    root = _project_root()
    example = root / "examples" / "keywords.example.txt"
    target = root / "data" / "keywords.txt"
    if not example.is_file():
        return
    has_kw = False
    if target.is_file():
        for line in target.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if s and not s.startswith("#"):
                has_kw = True
                break
    if has_kw:
        return
    print()
    if not _ask_bool("Создать data/keywords.txt из шаблона (ключи для SCOUT / поиска видео)?", default=True):
        print("  ✓ Пропущено — добавьте ключевые слова вручную или: python scripts/init_keywords.py")
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(example, target)
    print(f"  ✓ Создан {target}")
    print("  Отредактируйте список под свою нишу (см. комментарии в файле).")


def create_account(accounts_root: str = "accounts") -> None:
    """Основная точка входа: собирает данные и записывает файлы на диск."""
    name, cfg = build_config()
    if not name or cfg is None:
        print()
        print("  Аккаунт не создан.")
        return

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

    _maybe_seed_keywords_txt()

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
    print("  После первого успешного входа заливка видео блокируется на 3–5 суток")
    print("  (прогрев); остальной пайплайн работает. Отключить: \"skip_upload_warmup\": true")
    print("  в config.json. Один прогрев на весь аккаунт: \"upload_warmup_scope\": \"account\".")
    print("  См. UPLOAD_WARMUP_* и ACTIVITY_WARMUP_* в pipeline/config.py.")
    print("  Сводка прогрева: python -m pipeline.warmup_report")
    print("  Из архива в OUTPUT: python -m pipeline.redistribute_from_archive --days 7")
    print("=" * 55)
    print()


if __name__ == "__main__":
    create_account()
