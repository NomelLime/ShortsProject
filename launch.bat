@echo off
chcp 65001 > nul
title ShortsProject Launcher
setlocal enabledelayedexpansion

set "ROOT=%~dp0"
cd /d "%ROOT%"
set "PY=%ROOT%venv\Scripts\python.exe"
set "PIP=%ROOT%venv\Scripts\pip.exe"

REM ══════════════════════════════════════════════════════════════════════
REM  ПРОВЕРКА ОКРУЖЕНИЯ
REM ══════════════════════════════════════════════════════════════════════
if not exist "%PY%" (
    echo.
    echo  [ОШИБКА] Виртуальное окружение не найдено.
    echo  Создайте его: python -m venv venv
    echo               venv\Scripts\activate
    echo               pip install -r requirements.txt
    echo               playwright install chromium
    echo.
    pause
    exit /b 1
)

"%PY%" -c "import yt_dlp" 2>nul
if errorlevel 1 (
    echo  Установка зависимостей...
    "%PIP%" install -r requirements.txt
    if errorlevel 1 ( echo  [ОШИБКА] Не удалось установить зависимости. & pause & exit /b 1 )
    "%PY%" -m playwright install chromium
)

REM ══════════════════════════════════════════════════════════════════════
REM  ГЛАВНОЕ МЕНЮ
REM ══════════════════════════════════════════════════════════════════════
:MAIN_MENU
cls
echo.
echo  ╔══════════════════════════════════════════════════════╗
echo  ║          ShortsProject  —  Главное меню              ║
echo  ╚══════════════════════════════════════════════════════╝
echo.
echo    [1]  Запуск пайплайна
echo    [2]  Непрерывный режим (расписание 24/7)
echo    [3]  Агентный режим (AI Crew)
echo    [4]  Тесты
echo    [5]  Инструменты
echo    [6]  Статус проекта
echo    [7]  Выход
echo.
set /p "CHOICE=  Выбор: "

if "%CHOICE%"=="1" goto PIPELINE_MENU
if "%CHOICE%"=="2" goto SCHEDULED_MENU
if "%CHOICE%"=="3" goto CREW_MENU
if "%CHOICE%"=="4" goto TESTS_MENU
if "%CHOICE%"=="5" goto TOOLS_MENU
if "%CHOICE%"=="6" goto STATUS_MENU
if "%CHOICE%"=="7" exit /b 0
goto MAIN_MENU


REM ══════════════════════════════════════════════════════════════════════
REM  1. ЗАПУСК ПАЙПЛАЙНА
REM ══════════════════════════════════════════════════════════════════════
:PIPELINE_MENU
cls
echo.
echo  ┌──────────────────────────────────────────────────────┐
echo  │  Запуск пайплайна                                    │
echo  └──────────────────────────────────────────────────────┘
echo.
echo    [1]  Полный пайплайн
echo    [2]  Пресеты
echo    [3]  Настроить этапы вручную
echo    [4]  Назад
echo.
set /p "C=  Выбор: "
if "%C%"=="1" goto PIPELINE_FULL
if "%C%"=="2" goto PIPELINE_PRESETS
if "%C%"=="3" goto PIPELINE_CONFIGURE
if "%C%"=="4" goto MAIN_MENU
goto PIPELINE_MENU

REM ── Полный пайплайн ───────────────────────────────────────────────
:PIPELINE_FULL
set "P_ARGS="
set "P_DRY="
goto PIPELINE_CONFIRM

REM ── Пресеты ───────────────────────────────────────────────────────
:PIPELINE_PRESETS
cls
echo.
echo  ┌──────────────────────────────────────────────────────┐
echo  │  Пресеты                                             │
echo  └──────────────────────────────────────────────────────┘
echo.
echo    [1]  Полный пайплайн
echo    [2]  Поиск + Скачивание
echo    [3]  Обработка + Клонирование
echo    [4]  Распределение + Загрузка
echo    [5]  Только загрузка
echo    [6]  Только финализация
echo    [7]  Полный пайплайн  (dry-run)
echo    [8]  Назад
echo.
set /p "C=  Выбор: "
if "%C%"=="1" ( set "P_ARGS=" & set "P_DRY=" & goto PIPELINE_CONFIRM )
if "%C%"=="2" ( set "P_ARGS=--skip-processing --skip-distribute --skip-upload --skip-finalize" & set "P_DRY=" & goto PIPELINE_CONFIRM )
if "%C%"=="3" ( set "P_ARGS=--skip-search --skip-download --skip-upload --skip-finalize" & set "P_DRY=" & goto PIPELINE_CONFIRM )
if "%C%"=="4" ( set "P_ARGS=--skip-search --skip-download --skip-processing" & set "P_DRY=" & goto PIPELINE_CONFIRM )
if "%C%"=="5" ( set "P_ARGS=--skip-search --skip-download --skip-processing --skip-distribute" & set "P_DRY=" & goto PIPELINE_CONFIRM )
if "%C%"=="6" ( set "P_ARGS=--skip-search --skip-download --skip-processing --skip-distribute --skip-upload" & set "P_DRY=" & goto PIPELINE_CONFIRM )
if "%C%"=="7" ( set "P_ARGS=" & set "P_DRY=--dry-run" & goto PIPELINE_CONFIRM )
if "%C%"=="8" goto PIPELINE_MENU
goto PIPELINE_PRESETS

REM ── Ручная настройка этапов ───────────────────────────────────────
:PIPELINE_CONFIGURE
cls
echo.
echo  ┌──────────────────────────────────────────────────────┐
echo  │  Настройка этапов  (1=включить  0=пропустить)       │
echo  └──────────────────────────────────────────────────────┘
echo.
set "F_SEARCH=" & set "F_DOWNLOAD=" & set "F_PROC=" & set "F_DIST=" & set "F_UPLOAD=" & set "F_FIN="
call :ASK_STAGE "  Поиск трендов    (downloader)" F_SEARCH   --skip-search
call :ASK_STAGE "  Скачивание        (download)  " F_DOWNLOAD --skip-download
call :ASK_STAGE "  Обработка         (processing)" F_PROC     --skip-processing
call :ASK_STAGE "  Распределение     (distributor" F_DIST     --skip-distribute
call :ASK_STAGE "  Загрузка          (uploader)  " F_UPLOAD   --skip-upload
call :ASK_STAGE "  Финализация       (finalize)  " F_FIN      --skip-finalize
echo.
set /p "DR=  Dry-run? [1=да / 0=нет]: "
if "%DR%"=="1" (set "P_DRY=--dry-run") else (set "P_DRY=")
set "P_ARGS=%F_SEARCH% %F_DOWNLOAD% %F_PROC% %F_DIST% %F_UPLOAD% %F_FIN%"
goto PIPELINE_CONFIRM

REM ── Подтверждение и запуск ────────────────────────────────────────
:PIPELINE_CONFIRM
cls
echo.
echo  ┌──────────────────────────────────────────────────────┐
echo  │  Команда запуска                                     │
echo  └──────────────────────────────────────────────────────┘
echo.
echo    python run_pipeline.py %P_ARGS% %P_DRY%
echo.
echo    [1]  Запустить
echo    [2]  Назад
echo.
set /p "C=  Выбор: "
if "%C%"=="2" goto PIPELINE_MENU
if not "%C%"=="1" goto PIPELINE_CONFIRM
goto PIPELINE_RUN

:PIPELINE_RUN
cls
echo.
echo  Старт: %DATE% %TIME%
echo  ──────────────────────────────────────────────────────
echo.
"%PY%" run_pipeline.py %P_ARGS% %P_DRY%
echo.
echo  ──────────────────────────────────────────────────────
echo  Финиш: %DATE% %TIME%
echo.
echo    [1]  Запустить снова    [2]  Главное меню    [3]  Выход
echo.
set /p "C=  Выбор: "
if "%C%"=="1" goto PIPELINE_RUN
if "%C%"=="3" exit /b 0
goto MAIN_MENU


REM ══════════════════════════════════════════════════════════════════════
REM  2. НЕПРЕРЫВНЫЙ РЕЖИМ
REM ══════════════════════════════════════════════════════════════════════
:SCHEDULED_MENU
cls
echo.
echo  ┌──────────────────────────────────────────────────────┐
echo  │  Непрерывный режим (24/7)                            │
echo  └──────────────────────────────────────────────────────┘
echo.
echo    [1]  Запуск с подготовкой (поиск+скачивание+обработка)
echo    [2]  Только планировщик (очереди уже заполнены)
echo    [3]  Назад
echo.
set /p "C=  Выбор: "
if "%C%"=="3" goto MAIN_MENU
if "%C%"=="1" (
    cls
    echo  Запуск run_scheduled.py...
    echo  Для остановки нажмите Ctrl+C
    echo.
    "%PY%" run_scheduled.py
    pause
    goto MAIN_MENU
)
if "%C%"=="2" (
    cls
    echo  Запуск run_scheduled.py --skip-preparation...
    echo  Для остановки нажмите Ctrl+C
    echo.
    "%PY%" run_scheduled.py --skip-preparation
    pause
    goto MAIN_MENU
)
goto SCHEDULED_MENU


REM ══════════════════════════════════════════════════════════════════════
REM  3. АГЕНТНЫЙ РЕЖИМ (AI CREW)
REM ══════════════════════════════════════════════════════════════════════
:CREW_MENU
cls
echo.
echo  ┌──────────────────────────────────────────────────────┐
echo  │  Агентный режим — 12 AI агентов                     │
echo  └──────────────────────────────────────────────────────┘
echo.
echo    [1]  Запуск (интерактивный CLI)
echo    [2]  Запуск (daemon — Telegram команды)
echo    [3]  Установка Kokoro TTS
echo    [4]  Проверка TTS
echo    [5]  Статус агентов
echo    [6]  Назад
echo.
set /p "C=  Выбор: "
if "%C%"=="1" goto CREW_CLI
if "%C%"=="2" goto CREW_DAEMON
if "%C%"=="3" goto CREW_SETUP_TTS
if "%C%"=="4" goto CREW_TEST_TTS
if "%C%"=="5" goto CREW_STATUS
if "%C%"=="6" goto MAIN_MENU
goto CREW_MENU

:CREW_CLI
cls
echo  Запуск агентного режима (CLI)...
echo  Для выхода введи: выход
echo.
"%PY%" run_crew.py
pause
goto CREW_MENU

:CREW_DAEMON
cls
echo  Запуск в daemon-режиме (управление через Telegram)...
echo  Убедись что TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID в .env
echo  Для остановки нажмите Ctrl+C
echo.
"%PY%" run_crew.py --daemon
pause
goto CREW_MENU

:CREW_SETUP_TTS
cls
echo  Установка Kokoro-82M TTS...
echo  (скачивание ~530MB, может занять несколько минут)
echo.
"%PY%" scripts\setup_tts.py
pause
goto CREW_MENU

:CREW_TEST_TTS
cls
echo  Тест TTS синтеза...
echo.
echo    [1]  Тест на английском
echo    [2]  Тест на русском
echo    [3]  Назад
echo.
set /p "C=  Выбор: "
if "%C%"=="1" ( "%PY%" scripts\setup_tts.py --test-only --lang en & pause & goto CREW_MENU )
if "%C%"=="2" ( "%PY%" scripts\setup_tts.py --test-only --lang ru & pause & goto CREW_MENU )
goto CREW_MENU

:CREW_STATUS
cls
"%PY%" run_crew.py --cmd "статус"
pause
goto CREW_MENU


REM ══════════════════════════════════════════════════════════════════════
REM  4. ТЕСТЫ
REM ══════════════════════════════════════════════════════════════════════
:TESTS_MENU
cls
echo.
echo  ┌──────────────────────────────────────────────────────┐
echo  │  Тесты                                               │
echo  └──────────────────────────────────────────────────────┘
echo.
echo    [1]  Все тесты
echo    [2]  Только быстрые  (без @slow)
echo    [3]  Только медленные (@slow)
echo    [4]  Конкретный модуль
echo    [5]  С отчётом покрытия (coverage)
echo    [6]  Назад
echo.
set /p "C=  Выбор: "
if "%C%"=="1" goto TEST_ALL
if "%C%"=="2" goto TEST_FAST
if "%C%"=="3" goto TEST_SLOW
if "%C%"=="4" goto TEST_MODULE
if "%C%"=="5" goto TEST_COVERAGE
if "%C%"=="6" goto MAIN_MENU
goto TESTS_MENU

:TEST_ALL
cls
echo  Запуск всех тестов...
echo  ──────────────────────────────────────────────────────
"%PY%" -m pytest tests\test_pipeline.py -v
goto TEST_DONE

:TEST_FAST
cls
echo  Запуск быстрых тестов (без @slow)...
echo  ──────────────────────────────────────────────────────
"%PY%" -m pytest tests\test_pipeline.py -v -m "not slow"
goto TEST_DONE

:TEST_SLOW
cls
echo  Запуск медленных тестов (@slow)...
echo  ──────────────────────────────────────────────────────
"%PY%" -m pytest tests\test_pipeline.py -v -m slow
goto TEST_DONE

:TEST_MODULE
cls
echo.
echo  Доступные модули:
echo.
echo    [1]  download        [7]  finalize
echo    [2]  slicer          [8]  config
echo    [3]  utils           [9]  notifications
echo    [4]  distributor    [10]  analytics
echo    [5]  uploader       [11]  quarantine
echo    [6]  run_pipeline   [12]  Назад
echo.
set /p "M=  Выбор: "
if "%M%"=="1"  ( set "CLS=TestDownload"      & goto TEST_BY_CLASS )
if "%M%"=="2"  ( set "CLS=TestGroup TestStage" & goto TEST_BY_CLASS )
if "%M%"=="3"  ( set "CLS=TestProbe TestIs TestUnique TestSave" & goto TEST_BY_CLASS )
if "%M%"=="4"  ( set "CLS=TestParse TestCollect TestDistribute" & goto TEST_BY_CLASS )
if "%M%"=="5"  ( set "CLS=TestClean TestUpload" & goto TEST_BY_CLASS )
if "%M%"=="6"  ( set "CLS=TestRunPipeline" & goto TEST_BY_CLASS )
if "%M%"=="7"  ( set "CLS=TestExtract TestUpdate TestFind TestCollectStat" & goto TEST_BY_CLASS )
if "%M%"=="8"  ( set "CLS=TestPlatform TestConfig" & goto TEST_BY_CLASS )
if "%M%"=="9"  ( set "CLS=TestSendTelegram" & goto TEST_BY_CLASS )
if "%M%"=="10" ( echo  Тестов для analytics пока нет — добавьте в test_pipeline.py & pause & goto TESTS_MENU )
if "%M%"=="11" ( echo  Тестов для quarantine пока нет — добавьте в test_pipeline.py & pause & goto TESTS_MENU )
if "%M%"=="12" goto TESTS_MENU
goto TEST_MODULE

:TEST_BY_CLASS
cls
echo  Запуск тестов: %CLS%
echo  ──────────────────────────────────────────────────────
for %%K in (%CLS%) do (
    "%PY%" -m pytest tests\test_pipeline.py -v -k "%%K"
)
goto TEST_DONE

:TEST_COVERAGE
cls
echo  Проверка наличия pytest-cov...
"%PY%" -c "import pytest_cov" 2>nul
if errorlevel 1 (
    echo  Установка pytest-cov...
    "%PIP%" install pytest-cov
)
echo  Запуск с отчётом покрытия...
echo  ──────────────────────────────────────────────────────
"%PY%" -m pytest tests\test_pipeline.py -v --cov=pipeline --cov-report=term-missing
goto TEST_DONE

:TEST_DONE
echo.
echo  ──────────────────────────────────────────────────────
echo    [1]  Запустить снова    [2]  Меню тестов    [3]  Главное меню
echo.
set /p "C=  Выбор: "
if "%C%"=="1" goto TEST_AGAIN_%LAST_TEST%
if "%C%"=="2" goto TESTS_MENU
goto MAIN_MENU


REM ══════════════════════════════════════════════════════════════════════
REM  4. ИНСТРУМЕНТЫ
REM ══════════════════════════════════════════════════════════════════════
:TOOLS_MENU
cls
echo.
echo  ┌──────────────────────────────────────────────────────┐
echo  │  Инструменты                                         │
echo  └──────────────────────────────────────────────────────┘
echo.
echo    [1]  Настройка аккаунта  (setup_account.py)
echo    [2]  Сбор аналитики вручную
echo    [3]  Запустить авто-репост
echo    [4]  Снять аккаунт с карантина
echo    [5]  Сбросить чекпоинт скачивания
echo    [6]  Назад
echo.
set /p "C=  Выбор: "
if "%C%"=="1" goto TOOL_SETUP_ACCOUNT
if "%C%"=="2" goto TOOL_ANALYTICS
if "%C%"=="3" goto TOOL_REPOST
if "%C%"=="4" goto TOOL_UNQUARANTINE
if "%C%"=="5" goto TOOL_RESET_CHECKPOINT
if "%C%"=="6" goto MAIN_MENU
goto TOOLS_MENU

:TOOL_SETUP_ACCOUNT
cls
"%PY%" setup_account.py
pause
goto TOOLS_MENU

:TOOL_ANALYTICS
cls
echo  Запуск сбора аналитики...
echo.
"%PY%" -c "from pipeline.analytics import collect_pending_analytics; n = collect_pending_analytics(); print(f'Собрано записей: {n}')"
pause
goto TOOLS_MENU

:TOOL_REPOST
cls
echo  Поиск кандидатов и постановка в очередь репоста...
echo.
"%PY%" -c "from pipeline.analytics import queue_reposts; n = queue_reposts(); print(f'Поставлено в очередь: {n}')"
pause
goto TOOLS_MENU

:TOOL_UNQUARANTINE
cls
echo.
set /p "ACC=  Имя аккаунта: "
set /p "PLT=  Платформа (youtube/tiktok/instagram): "
"%PY%" -c "from pipeline.quarantine import lift_quarantine; lift_quarantine('%ACC%', '%PLT%'); print('Карантин снят.')"
pause
goto TOOLS_MENU

:TOOL_RESET_CHECKPOINT
cls
echo.
echo  ВНИМАНИЕ: сброс чекпоинта заставит скачать все URL заново.
echo.
set /p "CONFIRM=  Подтвердите (yes): "
if /i "%CONFIRM%"=="yes" (
    "%PY%" -c "from pipeline.download import reset_checkpoint; reset_checkpoint(); print('Чекпоинт сброшен.')"
) else (
    echo  Отменено.
)
pause
goto TOOLS_MENU


REM ══════════════════════════════════════════════════════════════════════
REM  5. СТАТУС
REM ══════════════════════════════════════════════════════════════════════
:STATUS_MENU
cls
echo.
echo  ┌──────────────────────────────────────────────────────┐
echo  │  Статус проекта                                      │
echo  └──────────────────────────────────────────────────────┘
echo.
echo    [1]  Всё сразу
echo    [2]  Зависимости
echo    [3]  Очереди загрузки
echo    [4]  Карантин аккаунтов
echo    [5]  Состояние сессий
echo    [6]  Репост-кандидаты
echo    [7]  Аналитика (топ видео)
echo    [8]  Чекпоинт скачивания
echo    [9]  Назад
echo.
set /p "C=  Выбор: "
if "%C%"=="1" ( cls & "%PY%" status.py all        & pause & goto STATUS_MENU )
if "%C%"=="2" ( cls & "%PY%" status.py deps       & pause & goto STATUS_MENU )
if "%C%"=="3" ( cls & "%PY%" status.py queues     & pause & goto STATUS_MENU )
if "%C%"=="4" ( cls & "%PY%" status.py quarantine & pause & goto STATUS_MENU )
if "%C%"=="5" ( cls & "%PY%" status.py sessions   & pause & goto STATUS_MENU )
if "%C%"=="6" ( cls & "%PY%" status.py reposts    & pause & goto STATUS_MENU )
if "%C%"=="7" ( cls & "%PY%" status.py analytics  & pause & goto STATUS_MENU )
if "%C%"=="8" ( cls & "%PY%" status.py checkpoint & pause & goto STATUS_MENU )
if "%C%"=="9" goto MAIN_MENU
goto STATUS_MENU


REM ══════════════════════════════════════════════════════════════════════
REM  ПОДПРОГРАММЫ
REM ══════════════════════════════════════════════════════════════════════

REM Запрашивает включение/выключение этапа.
REM %1=метка  %2=имя_переменной  %3=флаг_пропуска
:ASK_STAGE
set /p "_ANS=%~1 [1=вкл / 0=пропустить]: "
if "%_ANS%"=="0" (set "%~2=%~3") else (set "%~2=")
exit /b 0
