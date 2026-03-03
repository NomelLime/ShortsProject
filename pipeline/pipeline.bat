@echo off
chcp 65001 > nul
title Unified Video Shorts Pipeline
setlocal enabledelayedexpansion

REM ══════════════════════════════════════════════════════════════
REM  Корневая папка
REM ══════════════════════════════════════════════════════════════
set "ROOT_DIR=%~dp0"
cd /d "%ROOT_DIR%"

REM ══════════════════════════════════════════════════════════════
REM  Проверка окружения
REM ══════════════════════════════════════════════════════════════
if not exist "%ROOT_DIR%venv\Scripts\python.exe" (
    echo.
    echo  [ОШИБКА] Виртуальное окружение не найдено.
    echo  Создайте его командой:  python -m venv venv
    echo.
    pause
    exit /b 1
)

call "%ROOT_DIR%venv\Scripts\activate.bat"

python -c "import yt_dlp" 2>nul
if errorlevel 1 (
    echo  Установка зависимостей...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo  [ОШИБКА] Не удалось установить зависимости.
        pause
        exit /b 1
    )
    playwright install chromium
)

REM ══════════════════════════════════════════════════════════════
REM  Флаги (по умолчанию — всё включено, dry-run выключен)
REM ══════════════════════════════════════════════════════════════
set "SKIP_SEARCH="
set "SKIP_DOWNLOAD="
set "SKIP_PROCESSING="
set "SKIP_DISTRIBUTE="
set "SKIP_UPLOAD="
set "SKIP_FINALIZE="
set "DRY_RUN="

REM ══════════════════════════════════════════════════════════════
REM  ГЛАВНОЕ МЕНЮ
REM ══════════════════════════════════════════════════════════════
:MAIN_MENU
cls
echo.
echo  [==========================================================]
echo     Unified Video Shorts Pipeline
echo  [==========================================================]
echo.
echo   [1]  Запустить полный пайплайн
echo   [2]  Настроить этапы вручную
echo   [3]  Быстрые пресеты
echo   [4]  Показать текущие настройки
echo   [5]  Выход
echo.
set /p "CHOICE=  Выбор: "

if "%CHOICE%"=="1" goto RUN_FULL
if "%CHOICE%"=="2" goto CONFIGURE_STAGES
if "%CHOICE%"=="3" goto PRESETS
if "%CHOICE%"=="4" goto SHOW_SETTINGS
if "%CHOICE%"=="5" exit /b 0
goto MAIN_MENU

REM ══════════════════════════════════════════════════════════════
REM  НАСТРОЙКА ЭТАПОВ
REM ══════════════════════════════════════════════════════════════
:CONFIGURE_STAGES
cls
echo.
echo  [==========================================================]
echo     Настройка этапов
echo  [==========================================================]
echo.
echo  Для каждого этапа введите:  1 = включить,  0 = пропустить
echo.

call :ASK_STAGE "  Поиск трендов    (downloader) " SKIP_SEARCH
call :ASK_STAGE "  Скачивание видео (download)   " SKIP_DOWNLOAD
call :ASK_STAGE "  Обработка        (processing) " SKIP_PROCESSING
call :ASK_STAGE "  Распределение    (distributor)" SKIP_DISTRIBUTE
call :ASK_STAGE "  Загрузка         (uploader)   " SKIP_UPLOAD
call :ASK_STAGE "  Финализация      (finalize)   " SKIP_FINALIZE

echo.
set /p "DR=  Пробный запуск (dry-run)? [1=да / 0=нет]: "
if "%DR%"=="1" (set "DRY_RUN=--dry-run") else (set "DRY_RUN=")

goto SHOW_SETTINGS

REM ══════════════════════════════════════════════════════════════
REM  ПРЕСЕТЫ
REM ══════════════════════════════════════════════════════════════
:PRESETS
cls
echo.
echo  [==========================================================]
echo     Пресеты
echo  [==========================================================]
echo.
echo   [1]  Полный пайплайн
echo   [2]  Только поиск + скачивание
echo   [3]  Только обработка + клонирование
echo   [4]  Только распределение + загрузка
echo   [5]  Только загрузка
echo   [6]  Полный пайплайн  (dry-run — без реальных изменений)
echo   [7]  Назад
echo.
set /p "PRESET=  Выбор: "

if "%PRESET%"=="1" (
    set "SKIP_SEARCH="
    set "SKIP_DOWNLOAD="
    set "SKIP_PROCESSING="
    set "SKIP_DISTRIBUTE="
    set "SKIP_UPLOAD="
    set "SKIP_FINALIZE="
    set "DRY_RUN="
    goto SHOW_SETTINGS
)
if "%PRESET%"=="2" (
    set "SKIP_SEARCH="
    set "SKIP_DOWNLOAD="
    set "SKIP_PROCESSING=--skip-processing"
    set "SKIP_DISTRIBUTE=--skip-distribute"
    set "SKIP_UPLOAD=--skip-upload"
    set "SKIP_FINALIZE=--skip-finalize"
    set "DRY_RUN="
    goto SHOW_SETTINGS
)
if "%PRESET%"=="3" (
    set "SKIP_SEARCH=--skip-search"
    set "SKIP_DOWNLOAD=--skip-download"
    set "SKIP_PROCESSING="
    set "SKIP_DISTRIBUTE="
    set "SKIP_UPLOAD=--skip-upload"
    set "SKIP_FINALIZE=--skip-finalize"
    set "DRY_RUN="
    goto SHOW_SETTINGS
)
if "%PRESET%"=="4" (
    set "SKIP_SEARCH=--skip-search"
    set "SKIP_DOWNLOAD=--skip-download"
    set "SKIP_PROCESSING=--skip-processing"
    set "SKIP_DISTRIBUTE="
    set "SKIP_UPLOAD="
    set "SKIP_FINALIZE="
    set "DRY_RUN="
    goto SHOW_SETTINGS
)
if "%PRESET%"=="5" (
    set "SKIP_SEARCH=--skip-search"
    set "SKIP_DOWNLOAD=--skip-download"
    set "SKIP_PROCESSING=--skip-processing"
    set "SKIP_DISTRIBUTE=--skip-distribute"
    set "SKIP_UPLOAD="
    set "SKIP_FINALIZE="
    set "DRY_RUN="
    goto SHOW_SETTINGS
)
if "%PRESET%"=="6" (
    set "SKIP_SEARCH="
    set "SKIP_DOWNLOAD="
    set "SKIP_PROCESSING="
    set "SKIP_DISTRIBUTE="
    set "SKIP_UPLOAD="
    set "SKIP_FINALIZE="
    set "DRY_RUN=--dry-run"
    goto SHOW_SETTINGS
)
if "%PRESET%"=="7" goto MAIN_MENU
goto PRESETS

REM ══════════════════════════════════════════════════════════════
REM  ПОКАЗ ТЕКУЩИХ НАСТРОЕК И ПОДТВЕРЖДЕНИЕ
REM ══════════════════════════════════════════════════════════════
:SHOW_SETTINGS
cls
echo.
echo  [==========================================================]
echo     Текущие настройки
echo  [==========================================================]
echo.

call :SHOW_FLAG "  Поиск трендов  " "%SKIP_SEARCH%"     "--skip-search"
call :SHOW_FLAG "  Скачивание     " "%SKIP_DOWNLOAD%"   "--skip-download"
call :SHOW_FLAG "  Обработка      " "%SKIP_PROCESSING%" "--skip-processing"
call :SHOW_FLAG "  Распределение  " "%SKIP_DISTRIBUTE%" "--skip-distribute"
call :SHOW_FLAG "  Загрузка       " "%SKIP_UPLOAD%"     "--skip-upload"
call :SHOW_FLAG "  Финализация    " "%SKIP_FINALIZE%"   "--skip-finalize"

echo.
if defined DRY_RUN (
    echo   Dry-run:           [ДА - изменений не будет]
) else (
    echo   Dry-run:           [нет]
)

echo.
echo  ----------------------------------------------------------
echo   Команда:
echo   python run_pipeline.py %SKIP_SEARCH% %SKIP_DOWNLOAD% %SKIP_PROCESSING% %SKIP_DISTRIBUTE% %SKIP_UPLOAD% %SKIP_FINALIZE% %DRY_RUN%
echo  ----------------------------------------------------------
echo.
echo   [1]  Запустить
echo   [2]  Изменить настройки
echo   [3]  Главное меню
echo.
set /p "CONFIRM=  Выбор: "

if "%CONFIRM%"=="1" goto RUN_PIPELINE
if "%CONFIRM%"=="2" goto CONFIGURE_STAGES
if "%CONFIRM%"=="3" goto MAIN_MENU
goto SHOW_SETTINGS

REM ══════════════════════════════════════════════════════════════
REM  ПОЛНЫЙ ЗАПУСК (без настройки)
REM ══════════════════════════════════════════════════════════════
:RUN_FULL
set "SKIP_SEARCH="
set "SKIP_DOWNLOAD="
set "SKIP_PROCESSING="
set "SKIP_DISTRIBUTE="
set "SKIP_UPLOAD="
set "SKIP_FINALIZE="
set "DRY_RUN="

REM ══════════════════════════════════════════════════════════════
REM  ЗАПУСК
REM ══════════════════════════════════════════════════════════════
:RUN_PIPELINE
cls
echo.
echo  [==========================================================]
echo     Запуск пайплайна
echo  [==========================================================]
echo.
echo  Начало: %DATE% %TIME%
echo.

python run_pipeline.py %SKIP_SEARCH% %SKIP_DOWNLOAD% %SKIP_PROCESSING% %SKIP_DISTRIBUTE% %SKIP_UPLOAD% %SKIP_FINALIZE% %DRY_RUN%

echo.
echo  ==========================================================
echo  Завершено: %DATE% %TIME%
echo  ==========================================================
echo.
echo   [1]  Запустить снова с теми же настройками
echo   [2]  Главное меню
echo   [3]  Выход
echo.
set /p "AFTER=  Выбор: "
if "%AFTER%"=="1" goto RUN_PIPELINE
if "%AFTER%"=="2" goto MAIN_MENU
exit /b 0

REM ══════════════════════════════════════════════════════════════
REM  ПОДПРОГРАММЫ
REM ══════════════════════════════════════════════════════════════

REM Запрашивает включение/выключение этапа.
REM %1 — метка,  %2 — имя переменной флага
:ASK_STAGE
set /p "ANS=%~1 [1=вкл / 0=пропустить]: "
set "_VAR=%~2"
set "_FLAG="
if "%_VAR%"=="SKIP_SEARCH"      set "_FLAG=--skip-search"
if "%_VAR%"=="SKIP_DOWNLOAD"    set "_FLAG=--skip-download"
if "%_VAR%"=="SKIP_PROCESSING"  set "_FLAG=--skip-processing"
if "%_VAR%"=="SKIP_DISTRIBUTE"  set "_FLAG=--skip-distribute"
if "%_VAR%"=="SKIP_UPLOAD"      set "_FLAG=--skip-upload"
if "%_VAR%"=="SKIP_FINALIZE"    set "_FLAG=--skip-finalize"
if "%ANS%"=="0" (set "%_VAR%=%_FLAG%") else (set "%_VAR%=")
exit /b 0

REM Отображает статус этапа.
REM %1 — метка,  %2 — текущее значение,  %3 — флаг пропуска
:SHOW_FLAG
if "%~2"=="%~3" (
    echo   %~1  [ ПРОПУЩЕН ]
) else (
    echo   %~1  [ ВКЛ      ]
)
exit /b 0
