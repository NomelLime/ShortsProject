"""
download.py — Этап «Скачивание видео из urls.txt»
Параллельно скачивает все URL из urls.txt в папку preparing_shorts/,
проверяет целостность через ffprobe и ведёт лог ошибок.

Поддерживает чекпоинт: уже обработанные URL сохраняются в
data/download_checkpoint.json и пропускаются при повторном запуске.
"""

from __future__ import annotations

import json
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from threading import Lock
from typing import Final, Optional

from yt_dlp import YoutubeDL

from pipeline import config as cfg
from pipeline import utils
from pipeline.utils import check_video_integrity

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
    ok:              int        = 0
    failed:          int        = 0
    integrity_error: int        = 0
    skipped:         int        = 0
    files:           list[Path] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._lock: Lock = Lock()

    @property
    def total(self) -> int:
        return self.ok + self.failed + self.integrity_error + self.skipped

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


# ── Чекпоинт ─────────────────────────────────────────────────────────────────

_checkpoint_lock: Final[Lock] = Lock()


def _load_checkpoint() -> dict:
    """
    Загружает чекпоинт. Структура:
      {
        "done":   ["url1", "url2", ...],   # успешно скачано
        "failed": ["url3", ...]            # провалено — не повторяем
      }
    """
    path = cfg.DOWNLOAD_CHECKPOINT
    if not path.exists():
        return {"done": [], "failed": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"done": [], "failed": []}


def _save_checkpoint(checkpoint: dict) -> None:
    with _checkpoint_lock:
        try:
            cfg.DOWNLOAD_CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
            cfg.DOWNLOAD_CHECKPOINT.write_text(
                json.dumps(checkpoint, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            log.warning("Не удалось сохранить чекпоинт: %s", exc)


def _mark_checkpoint(url: str, success: bool, checkpoint: dict) -> None:
    """Добавляет URL в нужный раздел чекпоинта и сохраняет на диск."""
    with _checkpoint_lock:
        key = "done" if success else "failed"
        if url not in checkpoint[key]:
            checkpoint[key].append(url)
    _save_checkpoint(checkpoint)


def reset_checkpoint() -> None:
    """Сбрасывает чекпоинт — следующий запуск обработает все URL заново."""
    if cfg.DOWNLOAD_CHECKPOINT.exists():
        cfg.DOWNLOAD_CHECKPOINT.unlink()
        log.info("Чекпоинт сброшен: %s", cfg.DOWNLOAD_CHECKPOINT)


# ── Файл ошибок (потокобезопасная запись) ────────────────────────────────────

_failed_lock: Final[Lock] = Lock()


def _log_failed(url: str, reason: str) -> None:
    with _failed_lock:
        with cfg.FAILED_URLS_FILE.open("a", encoding="utf-8") as f:
            f.write(f"{url} # {reason}\n")


# ── Скачивание одного URL ────────────────────────────────────────────────────

def download_single(
    url: str,
    proxy: Optional[str] = None,
    checkpoint: dict | None = None,
) -> DownloadResult:
    """
    Скачивает одно видео через yt-dlp.
    Если URL уже есть в чекпоинте — пропускает.
    После завершения обновляет чекпоинт.
    """
    if checkpoint is not None:
        if url in checkpoint.get("done", []):
            return DownloadResult(url, DownloadStatus.OK, None, "checkpoint:skipped")
        if url in checkpoint.get("failed", []):
            return DownloadResult(url, DownloadStatus.FAILED, None, "checkpoint:previously_failed")

    ydl_opts = {
        "outtmpl":   str(cfg.PREPARING_DIR / "%(id)s.%(ext)s"),
        "noplaylist": True,
        "quiet":      True,
        "no_warnings": True,
        "format":     "bv*+ba/b",
        "retries":    3,
        "continuedl": True,
        "proxy":      proxy,
    }

    try:
        with YoutubeDL(ydl_opts) as ydl:
            info     = ydl.extract_info(url, download=True)
            out_path = Path(ydl.prepare_filename(info))

        if not out_path.exists():
            if checkpoint is not None:
                _mark_checkpoint(url, success=False, checkpoint=checkpoint)
            return DownloadResult(url, DownloadStatus.FAILED, None, "File not found after download")

        if not check_video_integrity(out_path):
            out_path.unlink(missing_ok=True)
            if checkpoint is not None:
                _mark_checkpoint(url, success=False, checkpoint=checkpoint)
            return DownloadResult(url, DownloadStatus.INTEGRITY_ERROR, None, "Integrity check failed")

        if utils.is_duplicate(out_path):
            out_path.unlink(missing_ok=True)
            _log_failed(url, "Duplicate content detected via perceptual hash")
            if checkpoint is not None:
                _mark_checkpoint(url, success=False, checkpoint=checkpoint)
            return DownloadResult(url, DownloadStatus.FAILED, None, "Duplicate content")

        if checkpoint is not None:
            _mark_checkpoint(url, success=True, checkpoint=checkpoint)
        return DownloadResult(url, DownloadStatus.OK, out_path)

    except Exception as e:
        if checkpoint is not None:
            _mark_checkpoint(url, success=False, checkpoint=checkpoint)
        return DownloadResult(url, DownloadStatus.FAILED, None, str(e))


# ── Скачивание всех URL ──────────────────────────────────────────────────────

def download_all(
    max_workers: int = cfg.MAX_WORKERS,
    proxy: Optional[str] = None,
    reset: bool = False,
) -> DownloadStats:
    """
    Скачивает все URL параллельно, проверяет целостность.

    reset=True — сбрасывает чекпоинт и обрабатывает все URL заново.
    По умолчанию (reset=False) — пропускает уже обработанные URL из чекпоинта.
    """
    urls = utils.unique_lines(cfg.URLS_FILE)
    if not urls:
        log.error("urls.txt пуст или не найден: %s", cfg.URLS_FILE)
        return DownloadStats()
    log.info("Загружено %d URL из %s", len(urls), cfg.URLS_FILE)

    if proxy is None:
        proxy = utils.load_proxy()

    if reset:
        reset_checkpoint()

    checkpoint   = _load_checkpoint()
    already_done = len([u for u in urls if u in checkpoint.get("done", [])])
    already_fail = len([u for u in urls if u in checkpoint.get("failed", [])])
    pending      = [
        u for u in urls
        if u not in checkpoint.get("done", []) and u not in checkpoint.get("failed", [])
    ]

    log.info(
        "Всего URL: %d | Уже скачано: %d | Ранее упало: %d | К обработке: %d | Потоков: %d",
        len(urls), already_done, already_fail, len(pending), max_workers,
    )

    cfg.FAILED_URLS_FILE.write_text(
        f"# Проблемные URL — {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
        encoding="utf-8",
    )

    stats = DownloadStats()
    stats.skipped = already_done + already_fail

    if not pending:
        log.info("Все URL уже обработаны (чекпоинт). Нечего скачивать.")
        _print_summary(stats)
        return stats

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="dl") as pool:
        futures = {
            pool.submit(download_single, url, proxy, checkpoint): url
            for url in pending
        }

        for done, future in enumerate(as_completed(futures), start=1):
            result = future.result()
            # Пропущенные по чекпоинту не попадают сюда (pending не содержит их)
            stats.record(result)

            if done % 10 == 0 or done == len(pending):
                log.info(
                    "Прогресс %d/%d | ✓%d ✗%d ⚠%d",
                    done, len(pending), stats.ok, stats.failed, stats.integrity_error,
                )

    _print_summary(stats)
    return stats


def _print_summary(stats: DownloadStats) -> None:
    problems = stats.failed + stats.integrity_error
    log.info("═══ ИТОГИ ═══")
    log.info("  Всего URL:     %d", stats.total)
    log.info("  Успешно:       %d", stats.ok)
    log.info("  Пропущено:     %d", stats.skipped)
    log.info("  Ошибки:        %d", stats.failed)
    log.info("  Повреждённые:  %d", stats.integrity_error)
    log.info("  Папка:         %s", cfg.PREPARING_DIR.resolve())
    if problems:
        log.info("  Проблемные URL → %s", cfg.FAILED_URLS_FILE)
    log.info("═════════════")


def retry_failed(
    max_retries: int = 3,
    proxy: Optional[str] = None,
) -> DownloadStats:
    """
    Повторно скачивает URLs из FAILED_URLS_FILE с экспоненциальным backoff.

    После успешного скачивания URL удаляется из файла.
    Если файл пуст после retry — удаляется.
    Возвращает статистику попыток.
    """
    if not cfg.FAILED_URLS_FILE.exists():
        log.info("[retry] Нет файла failed URLs (%s) — нечего ретраить", cfg.FAILED_URLS_FILE)
        return DownloadStats()

    failed_urls = [
        u.strip() for u in cfg.FAILED_URLS_FILE.read_text(encoding="utf-8").splitlines()
        if u.strip()
    ]
    if not failed_urls:
        cfg.FAILED_URLS_FILE.unlink(missing_ok=True)
        return DownloadStats()

    log.info("[retry] Ретрай %d URL(s) из %s", len(failed_urls), cfg.FAILED_URLS_FILE)
    stats      = DownloadStats()
    still_failed: list[str] = []

    for url in failed_urls:
        success = False
        for attempt in range(1, max_retries + 1):
            if attempt > 1:
                backoff = min(2 ** attempt, 60)   # 2, 4, 8 … max 60 сек
                log.info("[retry] Ожидание %ds перед попыткой %d/%d: %s",
                         backoff, attempt, max_retries, url[:80])
                import time as _time
                _time.sleep(backoff)

            result = download_single(url, proxy, checkpoint=None)
            stats.record(result)

            if result.status is DownloadStatus.OK:
                log.info("[retry] ✅ Успешно (попытка %d): %s", attempt, url[:80])
                success = True
                break
            log.warning("[retry] ❌ Попытка %d/%d failed: %s — %s",
                        attempt, max_retries, url[:80], result.error or "")

        if not success:
            still_failed.append(url)

    # Перезаписываем файл — только оставшиеся неудачи
    if still_failed:
        cfg.FAILED_URLS_FILE.write_text("\n".join(still_failed) + "\n", encoding="utf-8")
        log.info("[retry] Всё ещё не скачано: %d URL(s)", len(still_failed))
    else:
        cfg.FAILED_URLS_FILE.unlink(missing_ok=True)
        log.info("[retry] Все URL успешно скачаны — файл удалён")

    log.info("[retry] Итог: %d/%d восстановлено", stats.ok, len(failed_urls))
    return stats


def main() -> None:
    workers = int(os.environ.get("DOWNLOAD_WORKERS", cfg.MAX_WORKERS))
    proxy   = os.environ.get("PROXY") or None
    reset   = os.environ.get("RESET_CHECKPOINT", "").lower() in ("1", "true", "yes")
    stats   = download_all(max_workers=workers, proxy=proxy, reset=reset)
    print(f"\n[download.py] {stats.ok}/{stats.total} видео → {cfg.PREPARING_DIR}")


if __name__ == "__main__":
    main()