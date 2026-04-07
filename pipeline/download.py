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
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from threading import Lock
from typing import Final, Optional
from urllib.parse import urlparse
from collections import Counter

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
        # extractor в имени снижает риск коллизий ID между платформами.
        "outtmpl":   str(cfg.PREPARING_DIR / "%(extractor)s_%(id)s.%(ext)s"),
        "noplaylist": True,
        "quiet":      True,
        "no_warnings": True,
        "format":     "bv*+ba/b",
        "retries":    3,
        "continuedl": True,
        "proxy":      proxy,
    }
    ydl_opts.update(cfg.get_ytdlp_cookie_options())

    try:
        try:
            with YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                out_path = Path(ydl.prepare_filename(info))
        except Exception as first_exc:
            if "failed to load cookies" not in str(first_exc).lower():
                raise
            log.warning("Cookies не загрузились для URL, повторяем без cookies: %s", url[:100])
            retry_opts = dict(ydl_opts)
            retry_opts.pop("cookiefile", None)
            retry_opts.pop("cookiesfrombrowser", None)
            with YoutubeDL(retry_opts) as ydl:
                info = ydl.extract_info(url, download=True)
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
    priority_urls = _load_priority_urls(cfg.DOWNLOAD_PRIORITY_TOP_N)
    if priority_urls:
        urls = priority_urls
        log.info(
            "Источник URL: priority queue (%s), top-N=%d, выбрано=%d",
            cfg.URL_PRIORITY_QUEUE_FILE,
            cfg.DOWNLOAD_PRIORITY_TOP_N,
            len(urls),
        )
    else:
        urls = utils.unique_lines(cfg.URLS_FILE)
        if not urls:
            log.error("urls.txt пуст или не найден: %s", cfg.URLS_FILE)
            return DownloadStats()
        log.info("Priority queue пуста — fallback на urls.txt")
    urls = [u for u in urls if _is_downloadable_url(u)]
    log.info("Загружено %d URL из %s (после фильтра: %d)", len(utils.unique_lines(cfg.URLS_FILE)), cfg.URLS_FILE, len(urls))
    if not urls:
        log.error("После фильтрации нет валидных URL для скачивания.")
        return DownloadStats()

    no_proxy_mode = os.environ.get("NO_PROXY_DOWNLOAD", "").strip().lower() in ("1", "true", "yes", "on")
    if no_proxy_mode:
        proxy = ""
        log.info("[diag] NO_PROXY_DOWNLOAD=1: запускаю download без прокси")
    elif proxy is None:
        proxy = utils.load_proxy()

    diag_mode = os.environ.get("DIAGNOSTIC_DOWNLOAD", "").strip().lower() in ("1", "true", "yes", "on")
    if diag_mode:
        max_workers = min(max_workers, 4)
        log.info("[diag] DIAGNOSTIC_DOWNLOAD=1: ограничиваю потоки до %d", max_workers)

    if reset:
        reset_checkpoint()

    checkpoint   = _load_checkpoint()
    already_done = len([u for u in urls if u in checkpoint.get("done", [])])
    already_fail = len([u for u in urls if u in checkpoint.get("failed", [])])
    pending      = [
        u for u in urls
        if u not in checkpoint.get("done", []) and u not in checkpoint.get("failed", [])
    ]
    if cfg.DOWNLOAD_MAX_FILES_PER_RUN > 0 and len(pending) > cfg.DOWNLOAD_MAX_FILES_PER_RUN:
        pending = pending[: cfg.DOWNLOAD_MAX_FILES_PER_RUN]
        log.info(
            "Лимит DOWNLOAD_MAX_FILES_PER_RUN=%d: беру только первые %d URL",
            cfg.DOWNLOAD_MAX_FILES_PER_RUN,
            len(pending),
        )

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
    platform_done: Counter[str] = Counter()
    platform_failed: Counter[str] = Counter()
    bytes_downloaded = 0
    success_urls: set[str] = set()

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
            platform = _platform_from_url(result.url)
            if result.status is DownloadStatus.OK:
                platform_done[platform] += 1
                success_urls.add(result.url)
                if result.file and result.file.exists():
                    try:
                        bytes_downloaded += result.file.stat().st_size
                    except OSError:
                        pass
            else:
                platform_failed[platform] += 1

            log.info(
                "%s | src=%s | ok=%d fail=%d | size=%s",
                _render_progress_bar(done, len(pending)),
                platform,
                stats.ok,
                stats.failed + stats.integrity_error,
                _format_bytes(bytes_downloaded),
            )

            # Жёсткий стоп по бюджету объёма за запуск.
            if _gb_from_bytes(bytes_downloaded) >= cfg.DOWNLOAD_MAX_GB_PER_RUN:
                log.warning(
                    "Останов: достигнут лимит DOWNLOAD_MAX_GB_PER_RUN=%.2f GB (факт %.2f GB)",
                    cfg.DOWNLOAD_MAX_GB_PER_RUN,
                    _gb_from_bytes(bytes_downloaded),
                )
                for f in futures:
                    f.cancel()
                break

            # Стоп по свободному месту на диске.
            free_gb = _free_disk_gb(cfg.PREPARING_DIR)
            if free_gb < cfg.DOWNLOAD_MIN_FREE_GB:
                log.warning(
                    "Останов: свободно %.2f GB, ниже DOWNLOAD_MIN_FREE_GB=%.2f GB",
                    free_gb,
                    cfg.DOWNLOAD_MIN_FREE_GB,
                )
                for f in futures:
                    f.cancel()
                break

            if done % 10 == 0 or done == len(pending):
                log.info(
                    "Прогресс %d/%d | ✓%d ✗%d ⚠%d",
                    done, len(pending), stats.ok, stats.failed, stats.integrity_error,
                )
                log.info(
                    "По платформам: done=%s | fail=%s",
                    dict(platform_done),
                    dict(platform_failed),
                )

    _print_summary(stats)
    if success_urls:
        _prune_priority_queue(success_urls)
    return stats


def _is_downloadable_url(url: str) -> bool:
    """
    Отсекает витринные/поисковые страницы, которые не являются URL конкретного видео.
    """
    try:
        p = urlparse(url.strip())
    except Exception:
        return False
    host = (p.netloc or "").lower()
    path = (p.path or "").rstrip("/")
    if not host or not p.scheme.startswith("http"):
        return False

    # OK showcase/feed страницы (не конкретные ролики)
    if "ok.ru" in host and path in ("/video", "/video/showcase"):
        return False

    # YouTube search/results pages
    if "youtube.com" in host and path == "/results":
        return False

    # TikTok search page
    if "tiktok.com" in host and path == "/search":
        return False

    # Instagram полностью исключён из этапа download
    if "instagram.com" in host:
        return False

    return True


def _load_priority_urls(limit: int) -> list[str]:
    path = cfg.URL_PRIORITY_QUEUE_FILE
    if not path.exists() or limit <= 0:
        return []
    rows: list[tuple[int, str]] = []
    seen = set()
    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue
            url = str(item.get("url", "")).strip()
            if not url or url in seen or not _is_downloadable_url(url):
                continue
            score = int(item.get("score", 0))
            rows.append((score, url))
            seen.add(url)
    except Exception as exc:
        log.warning("Не удалось прочитать priority queue %s: %s", path, exc)
        return []
    rows.sort(key=lambda x: x[0], reverse=True)
    return [url for _, url in rows[:limit]]


def _prune_priority_queue(consumed_urls: set[str]) -> None:
    if not consumed_urls:
        return
    path = cfg.URL_PRIORITY_QUEUE_FILE
    if not path.exists():
        return
    try:
        kept: list[str] = []
        removed = 0
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
                url = str(item.get("url", "")).strip()
            except Exception:
                kept.append(line)
                continue
            if url and url in consumed_urls:
                removed += 1
                continue
            kept.append(line)
        path.write_text(("\n".join(kept) + ("\n" if kept else "")), encoding="utf-8")
        log.info("Priority queue cleanup: удалено %d успешно скачанных URL", removed)
    except Exception as exc:
        log.warning("Не удалось очистить priority queue: %s", exc)


def _platform_from_url(url: str) -> str:
    host = (urlparse(url).netloc or "").lower()
    if "youtube.com" in host or "youtu.be" in host:
        return "youtube"
    if "tiktok.com" in host:
        return "tiktok"
    if "vkvideo.ru" in host or "vk.com" in host:
        return "vk"
    if "rutube.ru" in host:
        return "rutube"
    if "ok.ru" in host:
        return "ok"
    if "instagram.com" in host:
        return "instagram"
    return "other"


def _format_bytes(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(max(0, num_bytes))
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024
        idx += 1
    return f"{size:.1f}{units[idx]}"


def _render_progress_bar(done: int, total: int, width: int = 24) -> str:
    if total <= 0:
        return "[------------------------] 0%"
    ratio = max(0.0, min(1.0, done / total))
    filled = int(ratio * width)
    bar = "#" * filled + "-" * (width - filled)
    return f"[{bar}] {int(ratio * 100):3d}% ({done}/{total})"


def _gb_from_bytes(num_bytes: int) -> float:
    return max(0.0, float(num_bytes) / (1024 ** 3))


def _free_disk_gb(path: Path) -> float:
    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    usage = shutil.disk_usage(path)
    return float(usage.free) / (1024 ** 3)


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