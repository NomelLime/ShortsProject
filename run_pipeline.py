#!/usr/bin/env python3
"""
Главный оркестратор этапов пайплайна. Поддержка --only, --resume и чекпоинтов pipeline_state.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from pipeline.logging_setup import setup_logger

logger = setup_logger("orchestrator")

from pipeline import (
    downloader,
    download,
    main_processing,
    distributor,
    uploader,
    finalize,
)
from pipeline.pipeline_state import (
    STAGE_ORDER,
    is_stage_done,
    load_state,
    reset_state,
    save_stage_result,
    set_current_stage,
)
from pipeline.scheduler import ActivityScheduler
from pipeline import config
from pipeline.utils import ensure_dirs, validate_config, get_all_accounts


def _interactive_login_preflight(*, all_platforms: bool = False) -> None:
    """
    До старта ActivityScheduler проверяем/обновляем логин в главном потоке.
    Это гарантирует появление окна ручной авторизации, если сессия невалидна.
    """
    enabled = os.getenv("LOGIN_PREFLIGHT_ENABLED", "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )
    is_tty = bool(getattr(sys.stdin, "isatty", lambda: False)())
    if not enabled or not is_tty:
        return

    try:
        accounts = get_all_accounts()
    except Exception as exc:
        logger.warning("[preflight] Не удалось получить аккаунты: %s", exc)
        return
    if not accounts:
        return

    from pipeline.browser import launch_browser, close_browser

    logger.info("[preflight] Проверка сессий перед запуском пайплайна...")
    for acc in accounts:
        acc_name = acc.get("name", "?")
        acc_cfg = acc.get("config", {})
        profile_dir = Path(acc.get("dir")) / "browser_profile"
        platforms = acc.get("platforms") or acc_cfg.get("platforms") or ["vk"]
        if isinstance(platforms, str):
            platforms = [platforms]
        targets = [str(platforms[0]).lower()] if platforms and not all_platforms else [str(p).lower() for p in platforms]
        for platform in targets:
            cfg_one = dict(acc_cfg)
            cfg_one["platforms"] = [platform]
            try:
                logger.info("[preflight] [%s] launch_browser (platform=%s)...", acc_name, platform)
                pw, ctx = launch_browser(
                    cfg_one,
                    profile_dir,
                    platform=platform,
                    allow_direct_fallback=True,
                    use_ip_registry=False,
                    force_manual_login=True,
                )
                close_browser(pw, ctx)
                logger.info("[preflight] [%s][%s] ОК", acc_name, platform)
            except Exception as exc:
                logger.warning("[preflight] [%s][%s] Проверка сессии пропущена: %s", acc_name, platform, exc)


def parse_args():
    parser = argparse.ArgumentParser(description="Запуск полного пайплайна обработки и загрузки видео")
    parser.add_argument("--skip-search", action="store_true", help="Пропустить поиск трендов (downloader)")
    parser.add_argument("--skip-download", action="store_true", help="Пропустить скачивание (download)")
    parser.add_argument("--skip-processing", action="store_true", help="Пропустить обработку")
    parser.add_argument("--skip-distribute", action="store_true", help="Пропустить распределение")
    parser.add_argument("--skip-upload", action="store_true", help="Пропустить загрузку")
    parser.add_argument("--skip-finalize", action="store_true", help="Пропустить финализацию")
    parser.add_argument("--dry-run", action="store_true", help="Пробный запуск без реальных изменений")
    parser.add_argument("--only", choices=STAGE_ORDER, help="Выполнить один этап и выйти")
    parser.add_argument("--resume", action="store_true", help="Продолжить с последнего незавершённого этапа")
    parser.add_argument("--login-only", action="store_true", help="Только ручной вход по платформам без запуска этапов")
    return parser.parse_args()


def run_stage(stage_func, stage_name, *args, **kwargs):
    """Обёртка для запуска этапа с логированием и обработкой ошибок."""
    logger.info("=" * 60)
    logger.info("ЗАПУСК ЭТАПА: %s", stage_name)
    logger.info("=" * 60)
    try:
        result = stage_func(*args, **kwargs)
        logger.info("Этап %s завершён успешно", stage_name)
        return result
    except Exception as e:
        logger.error("Критическая ошибка на этапе %s: %s", stage_name, e, exc_info=True)
        return False


def _stage_ok(result) -> bool:
    return result is not False


def _finalize_upload_results() -> list:
    st = load_state().get("stages", {}).get("upload", {})
    detail = st.get("detail") or {}
    ur = detail.get("upload_results")
    return ur if isinstance(ur, list) else []


def execute_stage(name: str, dry_run: bool) -> int:
    """Запуск одного именованного этапа. Возвращает код выхода 0/1."""
    set_current_stage(name)
    detail: dict = {}
    if name == "search":
        r = run_stage(downloader.search_and_save, "downloader")
    elif name == "download":
        r = run_stage(download.download_all, "download")
    elif name == "processing":
        r = run_stage(main_processing.run_processing, "processing", dry_run=dry_run)
    elif name == "distribute":
        r = run_stage(distributor.distribute_shorts, "distributor", dry_run=dry_run)
    elif name == "upload":
        r = run_stage(uploader.upload_all, "uploader", dry_run=dry_run)
        if not isinstance(r, list):
            logger.warning("Загрузка не вернула список — сохраняем пустой.")
            r = []
        detail["upload_results"] = r
    elif name == "finalize":
        upload_results = _finalize_upload_results()
        r = run_stage(finalize.finalize_and_report, "finalize", upload_results, dry_run=dry_run)
    else:
        return 2

    ok = _stage_ok(r)
    save_stage_result(name, ok, detail=detail if detail else None)
    return 0 if ok else 1


def _any_skip(args) -> bool:
    return any(
        [
            args.skip_search,
            args.skip_download,
            args.skip_processing,
            args.skip_distribute,
            args.skip_upload,
            args.skip_finalize,
        ]
    )


def main():
    args = parse_args()

    if os.getenv("LOG_FORMAT", "").lower() == "json":
        try:
            from pipeline.json_logging import setup_json_logging

            setup_json_logging("shorts_project")
        except ImportError:
            logger.warning("LOG_FORMAT=json: установите python-json-logger")
        except Exception:
            pass

    validate_config()
    ensure_dirs()

    try:
        from pipeline.warmup_report import log_warmup_dashboard

        log_warmup_dashboard(logger)
    except Exception:
        pass

    _interactive_login_preflight(all_platforms=args.login_only)
    if args.login_only:
        logger.info("[preflight] Режим --login-only завершён.")
        return

    if args.only:
        with ActivityScheduler():
            code = execute_stage(args.only, args.dry_run)
        sys.exit(code)

    if args.resume or not _any_skip(args):
        with ActivityScheduler():
            if not args.resume:
                reset_state()
            for stage in STAGE_ORDER:
                if args.resume and is_stage_done(stage):
                    continue
                code = execute_stage(stage, args.dry_run)
                if code != 0:
                    sys.exit(code)
        logger.info("=" * 60)
        logger.info("ПАЙПЛАЙН ЗАВЕРШЁН")
        logger.info("=" * 60)
        return

    # Legacy: только skip-флаги без state (--resume/--only не заданы)
    with ActivityScheduler():
        if not args.skip_search:
            run_stage(downloader.search_and_save, "downloader")
        else:
            logger.info("Пропуск этапа поиска (--skip-search)")

        if not args.skip_download:
            run_stage(download.download_all, "download")
        else:
            logger.info("Пропуск этапа скачивания (--skip-download)")

        if not args.skip_processing:
            run_stage(main_processing.run_processing, "processing", dry_run=args.dry_run)
        else:
            logger.info("Пропуск этапа обработки (--skip-processing)")

        if not args.skip_distribute:
            run_stage(distributor.distribute_shorts, "distributor", dry_run=args.dry_run)
        else:
            logger.info("Пропуск этапа распределения (--skip-distribute)")

        if not args.skip_upload:
            upload_results = run_stage(uploader.upload_all, "uploader", dry_run=args.dry_run)
            if not isinstance(upload_results, list):
                logger.warning("Загрузка не вернула список результатов — финализация получит пустой список.")
                upload_results = []
        else:
            upload_results = []
            logger.info("Пропуск этапа загрузки (--skip-upload)")

        if not args.skip_finalize:
            run_stage(finalize.finalize_and_report, "finalize", upload_results, dry_run=args.dry_run)
        else:
            logger.info("Пропуск этапа финализации (--skip-finalize)")

    logger.info("=" * 60)
    logger.info("ПАЙПЛАЙН ЗАВЕРШЁН")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
