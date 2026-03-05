"""
status.py — Диагностический дашборд ShortsProject.

Вызывается из launch.bat. Выводит в терминал текущее состояние:
  - очереди загрузки по аккаунтам
  - карантин аккаунтов
  - репост-кандидаты
  - сессии (возраст cookies)
  - зависимости (ffmpeg, Ollama, venv)
  - analytics.json — топ видео
"""

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# ── корень проекта в sys.path ──────────────────────────────────────────
ROOT = Path(__file__).parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Цвета (Windows 10+ поддерживает ANSI через chcp 65001)
R  = "\033[91m"   # red
G  = "\033[92m"   # green
Y  = "\033[93m"   # yellow
B  = "\033[94m"   # blue
C  = "\033[96m"   # cyan
W  = "\033[97m"   # white bold
DIM = "\033[2m"
RST = "\033[0m"

SEP  = f"{DIM}{'─' * 58}{RST}"
SEP2 = f"{DIM}{'═' * 58}{RST}"


def h(title: str) -> None:
    print(f"\n{SEP}\n {W}{title}{RST}\n{SEP}")


def ok(msg: str)   -> None: print(f"  {G}✔{RST}  {msg}")
def warn(msg: str) -> None: print(f"  {Y}⚠{RST}  {msg}")
def err(msg: str)  -> None: print(f"  {R}✘{RST}  {msg}")
def info(msg: str) -> None: print(f"  {C}·{RST}  {msg}")


# ─────────────────────────────────────────────────────────────────────
# 1. Зависимости
# ─────────────────────────────────────────────────────────────────────

def show_deps() -> None:
    h("ЗАВИСИМОСТИ")

    # Python
    ok(f"Python {sys.version.split()[0]}")

    # ffmpeg
    try:
        r = subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=5)
        ver = r.stdout.decode(errors="replace").splitlines()[0].split()[2]
        ok(f"ffmpeg {ver}")
    except Exception:
        err("ffmpeg не найден — обработка видео невозможна")

    # yt-dlp
    try:
        import yt_dlp
        ok(f"yt-dlp {yt_dlp.version.__version__}")
    except ImportError:
        err("yt-dlp не установлен")

    # rebrowser-playwright
    try:
        import rebrowser_playwright
        ok("rebrowser-playwright установлен")
    except ImportError:
        err("rebrowser-playwright не установлен")

    # imagehash + numpy (для дедупликации)
    try:
        import imagehash, numpy
        ok(f"imagehash {imagehash.__version__}  numpy {numpy.__version__}")
    except ImportError as e:
        warn(f"imagehash/numpy: {e}")

    # Ollama
    try:
        from pipeline import config
        r = subprocess.run(
            ["ollama", "list"], capture_output=True, timeout=5
        )
        if r.returncode == 0:
            ok(f"Ollama запущен (модель: {config.OLLAMA_MODEL})")
        else:
            warn("Ollama не отвечает — AI-функции недоступны")
    except Exception:
        warn("Ollama не найден — AI-функции недоступны")

    # Telegram
    try:
        from pipeline.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            ok("Telegram настроен")
        else:
            warn("Telegram не настроен (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID пустые)")
    except Exception:
        warn("Не удалось прочитать конфиг Telegram")


# ─────────────────────────────────────────────────────────────────────
# 2. Очереди загрузки
# ─────────────────────────────────────────────────────────────────────

def show_queues() -> None:
    h("ОЧЕРЕДИ ЗАГРУЗКИ")
    try:
        from pipeline import utils, config
        accounts = utils.get_all_accounts()
        if not accounts:
            warn("Аккаунты не найдены")
            return

        total_queued = 0
        for acc in accounts:
            acc_name  = acc["name"]
            platforms = acc["platforms"]
            today     = utils.get_uploads_today(acc["dir"])
            parts     = []
            for platform in platforms:
                q = utils.get_upload_queue(acc["dir"], platform)
                lim = config.PLATFORM_DAILY_LIMITS.get(platform, config.DAILY_UPLOAD_LIMIT)
                parts.append(f"{platform}: {len(q)} в очереди")
                total_queued += len(q)
            parts_str = "  |  ".join(parts)
            info(f"{W}{acc_name}{RST}  —  {parts_str}  {DIM}(сегодня: {today} загрузок){RST}")

        print()
        info(f"Итого в очередях: {W}{total_queued}{RST} видео")
    except Exception as e:
        err(f"Ошибка чтения очередей: {e}")


# ─────────────────────────────────────────────────────────────────────
# 3. Карантин
# ─────────────────────────────────────────────────────────────────────

def show_quarantine() -> None:
    h("КАРАНТИН АККАУНТОВ")
    try:
        from pipeline.quarantine import get_status
        data = get_status()
        if not data:
            ok("Все аккаунты активны")
            return

        any_quarantined = False
        for acc_name, platforms in data.items():
            for platform, entry in platforms.items():
                until = entry.get("until")
                errors = entry.get("errors", 0)
                total  = entry.get("total_quarantines", 0)

                if until:
                    try:
                        until_dt = datetime.fromisoformat(until)
                        remaining = max(0, (until_dt - datetime.now()).total_seconds() / 60)
                        err(f"{acc_name} / {platform}  —  карантин ещё {remaining:.0f} мин "
                            f"(до {until_dt.strftime('%H:%M')})  {DIM}всего карантинов: {total}{RST}")
                        any_quarantined = True
                    except Exception:
                        pass
                elif errors > 0:
                    warn(f"{acc_name} / {platform}  —  {errors} ошибок подряд "
                         f"(порог: {__import__('pipeline.config', fromlist=['config']).config.QUARANTINE_ERROR_THRESHOLD})")

        if not any_quarantined:
            ok("Ни один аккаунт не в карантине")
    except Exception as e:
        err(f"Ошибка: {e}")


# ─────────────────────────────────────────────────────────────────────
# 4. Сессии (возраст cookies)
# ─────────────────────────────────────────────────────────────────────

def show_sessions() -> None:
    h("СЕССИИ (ВОЗРАСТ COOKIES)")
    try:
        from pipeline.session_manager import get_session_age_hours
        from pipeline import utils, config

        accounts = utils.get_all_accounts()
        if not accounts:
            warn("Аккаунты не найдены")
            return

        for acc in accounts:
            for platform in acc["platforms"]:
                age = get_session_age_hours(acc["name"], platform)
                if age is None:
                    warn(f"{acc['name']} / {platform}  —  никогда не проверялась")
                elif age >= config.SESSION_MAX_AGE_HOURS:
                    err(f"{acc['name']} / {platform}  —  {age:.1f} ч  {R}(истекла){RST}")
                elif age >= config.SESSION_REFRESH_WARN_HOURS:
                    warn(f"{acc['name']} / {platform}  —  {age:.1f} ч  {Y}(скоро истечёт){RST}")
                else:
                    ok(f"{acc['name']} / {platform}  —  {age:.1f} ч")
    except Exception as e:
        err(f"Ошибка: {e}")


# ─────────────────────────────────────────────────────────────────────
# 5. Репост-кандидаты
# ─────────────────────────────────────────────────────────────────────

def show_reposts() -> None:
    h("РЕПОСТ-КАНДИДАТЫ")
    try:
        from pipeline.analytics import get_repost_candidates
        from pipeline import config
        candidates = get_repost_candidates()
        if not candidates:
            ok(f"Нет видео с < {config.REPOST_MIN_VIEWS} просмотров за "
               f"{config.REPOST_AFTER_HOURS} ч")
            return

        for c in candidates:
            info(f"{W}{c['stem'][:40]}{RST}  /  {c['platform']}  —  "
                 f"{R}{c['original_views']}{RST} просмотров  "
                 f"{DIM}(попытка {c['repost_attempt']}){RST}")
    except Exception as e:
        err(f"Ошибка: {e}")


# ─────────────────────────────────────────────────────────────────────
# 6. Аналитика — топ видео
# ─────────────────────────────────────────────────────────────────────

def show_analytics() -> None:
    h("АНАЛИТИКА — ТОП ВИДЕО")
    try:
        from pipeline import config
        if not config.ANALYTICS_FILE.exists():
            warn("analytics.json не найден — данных ещё нет")
            return

        data = json.loads(config.ANALYTICS_FILE.read_text(encoding="utf-8"))
        if not data:
            warn("analytics.json пуст")
            return

        # Сортируем по суммарным просмотрам
        scored = []
        for stem, entry in data.items():
            views = sum(
                u.get("views") or 0
                for u in entry.get("uploads", {}).values()
            )
            scored.append((views, stem, entry))
        scored.sort(reverse=True)

        info(f"Всего видео в аналитике: {W}{len(data)}{RST}")
        print()
        for views, stem, entry in scored[:8]:
            title = entry.get("title") or stem
            tags  = ", ".join(f"#{t}" for t in (entry.get("tags") or [])[:3])
            platforms_str = "  ".join(
                f"{pl}: {u.get('views') or '?'} 👁"
                for pl, u in entry.get("uploads", {}).items()
                if u.get("views") is not None
            )
            print(f"  {G}{views:>7,}{RST}  {W}{title[:40]}{RST}")
            if tags:
                print(f"  {DIM}        {tags}{RST}")
            if platforms_str:
                print(f"  {DIM}        {platforms_str}{RST}")
    except Exception as e:
        err(f"Ошибка: {e}")


# ─────────────────────────────────────────────────────────────────────
# 7. Чекпоинт скачивания
# ─────────────────────────────────────────────────────────────────────

def show_checkpoint() -> None:
    h("ЧЕКПОИНТ СКАЧИВАНИЯ")
    try:
        from pipeline import config
        cp_path = config.BASE_DIR / "data" / "download_checkpoint.json"
        if not cp_path.exists():
            ok("Чекпоинт отсутствует (первый запуск или был сброшен)")
            return

        cp = json.loads(cp_path.read_text(encoding="utf-8"))
        done   = len(cp.get("done",   []))
        failed = len(cp.get("failed", []))
        ok(f"Обработано: {G}{done}{RST}  |  Провалено: {R}{failed}{RST}")
        if done + failed > 0:
            info(f"Запуск с RESET_CHECKPOINT=1 сбросит чекпоинт")
    except Exception as e:
        err(f"Ошибка: {e}")


# ─────────────────────────────────────────────────────────────────────
# ТОЧКИ ВХОДА
# ─────────────────────────────────────────────────────────────────────

SECTIONS = {
    "all":        [show_deps, show_queues, show_quarantine, show_sessions,
                   show_reposts, show_analytics, show_checkpoint],
    "deps":       [show_deps],
    "queues":     [show_queues],
    "quarantine": [show_quarantine],
    "sessions":   [show_sessions],
    "reposts":    [show_reposts],
    "analytics":  [show_analytics],
    "checkpoint": [show_checkpoint],
}


def main() -> None:
    section = sys.argv[1] if len(sys.argv) > 1 else "all"
    fns = SECTIONS.get(section, SECTIONS["all"])

    print(f"\n{SEP2}")
    print(f"  {W}ShortsProject — Статус{RST}   {DIM}{datetime.now().strftime('%d.%m.%Y %H:%M')}{RST}")
    print(SEP2)

    for fn in fns:
        try:
            fn()
        except Exception as e:
            err(f"Секция '{fn.__name__}': {e}")

    print(f"\n{SEP2}\n")


if __name__ == "__main__":
    main()
