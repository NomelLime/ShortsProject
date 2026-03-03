"""
download.py — Этап «Скачивание видео из urls.txt»
Параллельно скачивает все URL из urls.txt в папку preparing_shorts/,
проверяет целостность через ffprobe и ведёт лог ошибок.
"""

from __future__ import annotations

import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from threading import Lock
from typing import Final

from yt_dlp import YoutubeDL

from pipeline import config as cfg
from pipeline import utils

log = utils.get_logger("download")


# ── Типы ─────────────────────────────────────────────────────────────────────

class DownloadStatus(Enum):
    OK              = auto()
    FAILED          = auto()
    INTEGRITY_ERROR = auto()


@dataclass
class DownloadResult:
    url:    str
    status: DownloadStatus
    file:   Path | None = None
    reason: str         = ""

    @property
    def ok(self) -> bool:
        return self.status is DownloadStatus.OK


@dataclass
class DownloadStats:
    total:           int        = 0
    ok:              int        = 0
    failed:          int        = 0
    integrity_error: int        = 0
    files:           list[Path] = field(default_factory=list)

    def __post_init__(self) -> None:
        # Инициализируем блокировку в __post_init__, чтобы не засорять dataclass
        self._lock: Lock = Lock()

    def record(self, result: DownloadResult) -> None:
        with self._lock:
            if result.status is DownloadStatus.OK:
                self.ok += 1
                if result.file:
                    self.files.append(result.file)
            elif result.status is DownloadStatus.INTEGRITY_ERROR:
                self.integrity_error += 1
            else:
                self.failed += 1


# ── Файл ошибок (потокобезопасная запись) ────────────────────────────────────

_failed_lock: Final[Lock] = Lock()


def _log_failed(url: str, reason: str) -> None:
    with _failed_lock:
        with cfg.FAILED_URLS_FILE.open("a", encoding="utf-8") as f:
            f.write(f"{url}  # {reason}\n")


# ── Определение платформы и куки ─────────────────────────────────────────────

_PLATFORM_DOMAINS: Final[dict[str, str]] = {
    "tiktok.com":    "tiktok",
    "instagram.com": "instagram",
    "youtube.com":   "youtube",
    "youtu.be":      "youtube",
}


def _platform_of(url: str) -> str:
    url_lower = url.lower()
    for domain, name in _PLATFORM_DOMAINS.items():
        if domain in url_lower:
            return name
    return "unknown"


def _cookies_for(url: str) -> str | None:
    path = cfg.COOKIES.get(_platform_of(url))
    if path and path.exists():
        log.debug("Куки: %s", path.name)
        return str(path)
    return None


# ── YDL-опции для скачивания ─────────────────────────────────────────────────

def _download_ydl_opts(proxy: str | None, cookies_file: str | None) -> dict:
    opts: dict = {
        # Формат: нативный mp4, иначе лучшее доступное
        "format":   "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",

        # Скачиваем в PREPARING_DIR — папку для «сырых» исходников,
        # а не в OUTPUT_DIR (финальные шортсы).
        "outtmpl":  str(cfg.PREPARING_DIR / "%(title).100s.%(ext)s"),

        "writeinfojson":    True,
        "writethumbnail":   False,
        "writedescription": False,

        "retries":                    cfg.RETRIES,
        "fragment_retries":           cfg.RETRIES,
        "skip_unavailable_fragments": True,
        "ignoreerrors":               False,
        "socket_timeout":             cfg.SOCKET_TIMEOUT,
        "sleep_interval":             random.uniform(cfg.SLEEP_MIN, cfg.SLEEP_MAX),
        "max_sleep_interval":         cfg.SLEEP_MAX,

        "concurrent_fragment_downloads": cfg.FRAGMENT_THREADS,

        "quiet":       True,
        "no_warnings": False,

        "postprocessors": [{"key": "FFmpegVideoConvertor", "preferedformat": "mp4"}],

        "noplaylist": True,
        "geo_bypass": True,
        "http_headers": cfg.DEFAULT_HEADERS,
    }

    if proxy:
        opts["proxy"] = proxy
    if cookies_file:
        opts["cookiefile"] = cookies_file

    return opts


# ── Определение пути к скачанному файлу ──────────────────────────────────────

def _resolve_filepath(info: dict) -> Path | None:
    """Извлекает путь к скачанному файлу из info-словаря yt-dlp."""
    for dl in info.get("requested_downloads") or []:
        if fp := dl.get("filepath"):
            p = Path(fp)
            if p.exists():
                return p

    title = "".join(
        c for c in info.get("title", "")
        if c.isalnum() or c in " _-"
    )[:100]
    ext       = info.get("ext", "mp4")
    candidate = cfg.PREPARING_DIR / f"{title}.{ext}"
    return candidate if candidate.exists() else None


# ── Скачивание одного URL ─────────────────────────────────────────────────────

def download_single(url: str, proxy: str | None) -> DownloadResult:
    """Скачивает одно видео, проверяет целостность, возвращает DownloadResult."""
    log.info("[%s] %s", _platform_of(url).upper(), url)

    try:
        opts = _download_ydl_opts(proxy, _cookies_for(url))
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)

        if not info:
            return _fail(url, "extract_info вернул None")

        filepath = _resolve_filepath(info)
        if not filepath:
            return _fail(url, "Файл не найден после скачивания")

        if not utils.check_video_integrity(filepath):
            _cleanup(filepath)
            return DownloadResult(
                url=url,
                status=DownloadStatus.INTEGRITY_ERROR,
                file=filepath,
                reason="Видео повреждено (ffprobe)",
            )

        size_mb = filepath.stat().st_size / 1_048_576
        log.info("✓ %s  (%.1f МБ)", filepath.name, size_mb)
        return DownloadResult(url=url, status=DownloadStatus.OK, file=filepath)

    except Exception as exc:
        return _fail(url, str(exc)[:300])


def _fail(url: str, reason: str) -> DownloadResult:
    log.error("✗ %s — %s", url, reason)
    _log_failed(url, reason)
    return DownloadResult(url=url, status=DownloadStatus.FAILED, reason=reason)


def _cleanup(filepath: Path) -> None:
    """Удаляет повреждённый видеофайл и сопутствующий .info.json."""
    log.warning("Удаляем повреждённый файл: %s", filepath.name)
    for path in (filepath, filepath.with_suffix(".info.json")):
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


# ── Параллельное скачивание всех URL ─────────────────────────────────────────

def download_all(
    urls:        list[str] | None = None,
    max_workers: int               = cfg.MAX_WORKERS,
    proxy:       str | None        = None,
) -> DownloadStats:
    """
    Скачивает все URL параллельно.

    Параметры:
        urls        — список URL; если None — читает из urls.txt
        max_workers — параллельных скачиваний
        proxy       — прокси; если None — берётся из config.json
    """
    log.info("═══ Запуск параллельного скачивания ═══")
    cfg.PREPARING_DIR.mkdir(parents=True, exist_ok=True)

    if urls is None:
        urls = utils.read_lines(cfg.URLS_FILE)
        if not urls:
            log.error("urls.txt пуст или не найден: %s", cfg.URLS_FILE)
            return DownloadStats()
        log.info("Загружено %d URL из %s", len(urls), cfg.URLS_FILE)

    if not urls:
        log.error("Список URL пуст.")
        return DownloadStats()

    if proxy is None:
        proxy = utils.load_proxy()

    cfg.FAILED_URLS_FILE.write_text(
        f"# Проблемные URL — {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
        encoding="utf-8",
    )

    stats = DownloadStats(total=len(urls))
    log.info("Всего: %d | Потоков: %d | Прокси: %s",
             len(urls), max_workers, proxy or "нет")

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="dl") as pool:
        futures = {pool.submit(download_single, url, proxy): url for url in urls}

        for done, future in enumerate(as_completed(futures), start=1):
            stats.record(future.result())

            if done % 10 == 0 or done == len(urls):
                log.info(
                    "Прогресс %d/%d | ✓%d ✗%d ⚠%d",
                    done, len(urls), stats.ok, stats.failed, stats.integrity_error,
                )

    _print_summary(stats)
    return stats


def _print_summary(stats: DownloadStats) -> None:
    problems = stats.failed + stats.integrity_error
    log.info("═══ ИТОГИ ═══")
    log.info("  Всего:         %d", stats.total)
    log.info("  Успешно:       %d", stats.ok)
    log.info("  Ошибки:        %d", stats.failed)
    log.info("  Повреждённые:  %d", stats.integrity_error)
    log.info("  Папка:         %s", cfg.PREPARING_DIR.resolve())
    if problems:
        log.info("  Проблемные URL → %s", cfg.FAILED_URLS_FILE)
    log.info("═════════════")


def main() -> None:
    workers = int(os.environ.get("DOWNLOAD_WORKERS", cfg.MAX_WORKERS))
    proxy   = os.environ.get("PROXY") or None
    stats   = download_all(max_workers=workers, proxy=proxy)
    print(f"\n[download.py] {stats.ok}/{stats.total} видео → {cfg.PREPARING_DIR}")


if __name__ == "__main__":
    main()
