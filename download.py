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
            f.write(f"{url} # {reason}\n")


# ── Скачивание одного URL ────────────────────────────────────────────────────

def download_single(url: str, proxy: Optional[str] = None) -> DownloadResult:
    """Скачивает одно видео через yt-dlp."""
    ydl_opts = {
        "outtmpl": str(cfg.PREPARING_DIR / "%(id)s.%(ext)s"),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "format": "bv*+ba/b",
        "retries": 3,
        "continuedl": True,
        "proxy": proxy,
    }

    try:
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            out_path = Path(ydl.prepare_filename(info))

        if not out_path.exists():
            return DownloadResult(url, DownloadStatus.FAILED, None, "File not found after download")

        if not check_video_integrity(out_path):
            out_path.unlink(missing_ok=True)
            return DownloadResult(url, DownloadStatus.INTEGRITY_ERROR, None, "Integrity check failed")

        # New: Duplicate check via perceptual hash
        if utils.is_duplicate(out_path):
            out_path.unlink(missing_ok=True)
            _log_failed(url, "Duplicate content detected via perceptual hash")
            return DownloadResult(url, DownloadStatus.FAILED, None, "Duplicate content")

        return DownloadResult(url, DownloadStatus.OK, out_path)
    except Exception as e:
        return DownloadResult(url, DownloadStatus.FAILED, None, str(e))


# ── Скачивание всех URL ──────────────────────────────────────────────────────

def download_all(max_workers: int = cfg.MAX_WORKERS, proxy: Optional[str] = None) -> DownloadStats:
    """Скачивает все URL параллельно, проверяет целостность."""
    urls = utils.unique_lines(cfg.URLS_FILE)
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