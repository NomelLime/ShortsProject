https://github.com/NomelLime/ShortsProject
Имеется в привязке к проекту NomelLime/ShortsProject
Сразу изучи этот Git.
⚠️ GitHub Token передаётся отдельно в чате. Получи новый на github.com/settings/tokens.


# ShortsProject — Status

**Дата последнего обновления:** 06.03.2026  
**Ветка:** `main`  
**Репозиторий:** `NomelLime/ShortsProject` (private)

---

## Сессии

### Сессия 1 (04.03.2026) — Bugfix + тесты

18 критических исправлений (runtime crashes, логические ошибки, минорное).  
55 реальных pytest-тестов, 9 модулей. Документация docx.  
Подробности — в блоке «Сессия 04.03» ниже.

---

### Сессия 2 (05.03.2026) — AI Агентная архитектура

**Цель:** Добавить 12-агентную CrewAI систему поверх существующего пайплайна.  
**Железо:** RTX 5070 Ti 12GB VRAM, 128GB RAM, Intel Core Ultra 9 275HX.  
**Целевая нагрузка:** 100 аккаунтов × 1500 загрузок/день (YouTube / TikTok / Instagram).

#### Архитектура

```
Пользователь → COMMANDER (Telegram / CLI)
                    ↓  Ollama intent parsing
               DIRECTOR (watchdog + оркестрация)
                    ↓
  ┌──────────┬──────────┬──────────┬──────────┬──────────┐
SCOUT     CURATOR   VISIONARY  NARRATOR   EDITOR   STRATEGIST
(поиск)  (фильтр)  (AI-мета)   (TTS)    (монтаж)  (A/B)
  └──────────┴──────────┴──────────┴──────────┴──────────┘
               GUARDIAN  PUBLISHER  ACCOUNTANT  SENTINEL
              (антибан)  (загрузка)  (лимиты)  (мониторинг)
```

GPU Manager гарантирует: только одна тяжёлая задача (LLM / TTS / VideoGen / Encode) одновременно на RTX 5070 Ti.

---

#### Этап 1 — Инфраструктура агентов (`c494519`)

**Новые файлы:**

| Файл | Суть |
|------|------|
| `pipeline/agent_memory.py` | Thread-safe KV + персистентность `data/agent_memory.json`. Методы: `get/set/delete`, `log_event`, `set_agent_status`, `register_agent` |
| `pipeline/agents/base_agent.py` | ABC для всех агентов. `AgentStatus` enum. Lifecycle: `start→run→stop`. Авто-регистрация в memory, `sleep()` с interrupt |
| `pipeline/agents/gpu_manager.py` | Priority queue для RTX 5070 Ti 12GB. `GPUPriority`: CRITICAL / LLM / TTS / VIDEO_GEN / ENCODE. Context manager + decorator |
| `pipeline/agents/director.py` | Центральный оркестратор. Watchdog каждые 60с, авто-рестарт при ERROR (max 3) |
| `pipeline/agents/commander.py` | Telegram/CLI интерфейс. Ollama intent parsing (qwen2.5-vl:7b). Подтверждение рисковых команд |
| `pipeline/agents/{scout,curator,visionary,narrator,editor,strategist,guardian,publisher,accountant,sentinel}.py` | Скелеты 10 специализированных агентов |
| `pipeline/agents/__init__.py` | Экспорты |
| `pipeline/crew.py` | `ShortsProjectCrew` — сборка системы, context manager |
| `run_crew.py` | Точка входа: CLI / daemon / `--cmd` |

**Dependencies добавлены в `requirements.txt`:**  
`crewai`, `crewai-tools`, `psutil`, `kokoro-onnx`, `soundfile`

---

#### Этап 2 — Подключение агентов к модулям pipeline (`c5f5da2`)

Каждый агент заменён с заглушки на реальную реализацию:

| Агент | Вызывает | Особенности |
|-------|----------|-------------|
| **SCOUT** | `_search_ytdlp`, `_search_browser`, `_expand_keywords_with_ai` | GPU lock (LLM) для AI расширения KW; COMMANDER override через AgentMemory |
| **CURATOR** | `probe_video`, `is_duplicate` | Фильтр: длина 5с–10мин, разрешение ≥320px, phash dedup |
| **VISIONARY** | `generate_video_metadata`, `load_trending_hashtags`, `check_ollama` | GPU lock LLM, fallback мета если Ollama недоступен |
| **NARRATOR** | Kokoro-82M TTS | GPU lock TTS, RU/EN/+10 языков, тихая деградация без модели |
| **EDITOR** | `run_processing()` | GPU lock encode; умный выбор фона: по теме → ротация → AnimateDiff (заглушка) |
| **STRATEGIST** | `compare_ab_results`, `queue_reposts`, `collect_pending_analytics` | Анализ лучших часов публикаций |
| **GUARDIAN** | `is_session_stale`, `check_proxy_health`, `get_status` (quarantine) | Авто-уведомления при проблемах |
| **PUBLISHER** | `upload_all()` + retry | Уведомляет GUARDIAN (карантин) и ACCOUNTANT (лимиты) |
| **ACCOUNTANT** | `is_daily_limit_reached`, `get_all_accounts` | Кастомные лимиты через AgentMemory |
| **SENTINEL** | `psutil` (CPU/RAM/Disk) + `nvidia-smi` | Cooldown алёртов 30 мин |

Перекрёстные ссылки в `crew.py`: `EDITOR↔VISIONARY`, `PUBLISHER↔GUARDIAN/ACCOUNTANT`.

---

#### Этап 3 — Kokoro TTS в монтажный конвейер (`5fb8220`)

**Новые файлы:**

| Файл | Суть |
|------|------|
| `pipeline/tts_utils.py` | `detect_language()` (RU/EN/+), `clean_tts_text()` (URL/хэштеги/markdown), `pick_tts_text()` (hook→title→desc), `get_voice_for_lang()` |
| `scripts/setup_tts.py` | Установщик: `pip install kokoro-onnx soundfile langdetect` + скачивание моделей (~530MB) + тест синтеза |

**Изменённые файлы:**

| Файл | Что добавлено |
|------|---------------|
| `pipeline/config.py` | Блок TTS констант: `TTS_ENABLED`, `TTS_DIR`, `TTS_MODEL_FILE`, `TTS_VOICES_FILE`, `TTS_DEFAULT_LANG`, `TTS_SPEED`, `TTS_VOLUME`, `TTS_VOICE_OVER_MIX`, `TTS_TEMP_DIR` |
| `pipeline/postprocessor.py` | `_postprocess_single(..., tts_audio_path=None)` — если передан `.wav`: `amix` голос + оригинальный аудио (баланс: `TTS_VOICE_OVER_MIX=0.85`). Без TTS — поведение без изменений |
| `pipeline/agents/editor.py` | Полный конвейер: `slicer → Visionary (GPU:LLM) → Narrator (GPU:TTS) → postprocessor (GPU:ENCODE)`. `_generate_tts_batch()`, `_cleanup_tts_temp()` |
| `launch.bat` | Добавлен раздел `[3] Агентный режим`: CLI / daemon / установка TTS / тест TTS / статус агентов |

---

#### Этап 4 — GUARDIAN + PUBLISHER полная реализация (`2ceac5b`)

**GUARDIAN** (полная перезапись):
- Ротация прокси: `resolve_working_proxy()` для всех аккаунтов каждые 5 мин
- Мониторинг сессий: `is_session_stale()` каждый час
- Карантин: кэш + `mark_error/mark_success/is_quarantined`
- Публичный API: `is_account_safe()`, `get_safe_delay()` (антибан: 30–240с)
- Бан-сигналы: детектирует 429/403/banned → лог в AgentMemory + Telegram
- `report_upload_error()` / `report_upload_success()` для PUBLISHER

**PUBLISHER** (полная перезапись):
- Параллельная загрузка: `ThreadPoolExecutor(_MAX_PARALLEL=3)`
- Умная очередь: карантин → лимит → сортировка по `uploads_today` (меньше = выше)
- Антибан задержки через `Guardian.get_safe_delay()`
- Полный цикл: `launch_browser → ensure_session_fresh → upload_video`
- Регистрация в analytics: `register_upload()` + `ab_variant`
- Статистика: by_platform breakdown, total/batch counters
- `trigger_now()` для принудительного запуска из COMMANDER

---

### Сессия 3 (06.03.2026) — Этапы 5 и 6: завершение агентов + тесты

#### Исправлен скрытый системный баг

`base_agent.py` в репо был старой версией без `start/stop/sleep/should_stop/AgentStatus.ERROR`.  
Все агенты вызывали несуществующие методы. Полностью переписан.

**`pipeline/agents/base_agent.py`** — полная перезапись (`391de27`):
- Threading lifecycle: `start()` → поток → `stop(timeout)` → join
- `should_stop: bool` — property через `threading.Event`
- `sleep(seconds) → bool` — прерываемый, возвращает `False` при `stop()`
- `AgentStatus`: IDLE / RUNNING / WAITING / ERROR / STOPPED
- `_set_status(status, detail='')` → обновляет AgentMemory
- `report(data: Dict)` → `memory.set_agent_report()`
- `_send(msg)` → Telegram или callable notify
- `_run_wrapper()` — перехватывает исключения, ставит ERROR

---

#### Этап 5В — ACCOUNTANT полная реализация (`a4bb37c`)

**`pipeline/agents/accountant.py`**:
- Умные лимиты с приоритетом: `acc.platform` → `platform` → `all` → `acc_cfg.daily_limits` → global
- `get_account_capacity(platform) → (available: int, total: int)` — для PUBLISHER
- `get_next_upload_times(platform) → List[str]` — читает `upload_schedule` из каждого `config.json`, fallback на `DEFAULT_UPLOAD_TIMES`
- `set_custom_limit(platform, limit, account_name='')` — per-account ключи вида `"acc1.youtube"`
- `get_custom_limits() → Dict` — просмотр лимитов (для COMMANDER)
- `is_limit_reached()` с fallback через `utils` если память пустая

---

#### Этап 5А — SENTINEL авто-рестарт + DIRECTOR интеграция (`391de27`)

**`pipeline/agents/sentinel.py`**:
- `_error_since: Dict[str, float]` — фиксирует момент первого обнаружения ERROR
- Порог `_ERROR_RESTART_SEC = 120`: рестарт только если агент в ERROR > 2 мин (защита от петли)
- Логика `_check_agents()`:
  - `WAITING` (ждёт GPU) → не трогать, сбросить таймер
  - `ERROR` < 2 мин → предупреждение, ждём
  - `ERROR` > 2 мин → `_request_restart(agent_name)`
  - вышел из ERROR сам → сбросить таймер, залогировать
- `_request_restart()` → добавляет имя в `memory["sentinel_restart_requests"]`, уведомляет Telegram

**`pipeline/agents/director.py`**:
- `_process_sentinel_requests()` — читает `sentinel_restart_requests` из AgentMemory
  - пропускает агентов, которые уже восстановились (не ERROR)
  - вызывает `restart_agent()` для каждого в списке
  - очищает список после обработки
- `_watchdog()` теперь сначала вызывает `_process_sentinel_requests()`, затем свой цикл

---

#### Этап 5Б — STRATEGIST применение расписания (`6c31964`)

**`pipeline/agents/strategist.py`**:
- `_apply_schedule_recommendations(best_times: Dict[str, List[int]])`:
  1. Конвертирует часы → строки `"HH:00"` (sorted)
  2. Загружает `config.json` каждого аккаунта через `get_all_accounts()`
  3. Проверяет что платформа входит в `acc["platforms"]` (пропускает чужие)
  4. Обновляет `acc_cfg["upload_schedule"][platform]` только если значение изменилось
  5. `save_json(cfg_path, acc_cfg)` → UploadScheduler подхватит на следующем тике
  6. `log_event` + Telegram при обновлении
- `_analysis_cycle()` вызывает `_apply_schedule_recommendations(schedule_recs)` если `best_times` не пустой

---

#### Этап 6 — Тесты агентов (`7b261af`)

**`tests/test_agents.py`** — 40 тест-кейсов, 11 классов:

| Класс | Тесты |
|-------|-------|
| `TestGPUManager` | priority_order, concurrent_limit=1, stats_tracking, decorator_usage |
| `TestAgentMemory` | set/get, default, thread_safe (100 потоков), persistence, log_event_max_500, status_roundtrip, delete |
| `TestBaseAgentLifecycle` | start_stop, should_stop, status_transitions, sleep_interrupted, error_captured, report_stored |
| `TestScout` | saves_urls (mock yt-dlp), keyword_override |
| `TestCurator` | rejects_short, accepts_valid, rejects_duplicate, rejects_low_resolution |
| `TestPublisher` | notifies_guardian_on_error, notifies_guardian_on_success, skips_quarantined |
| `TestGuardian` | quarantined→unsafe, clean→safe, ban_signal_429, ban_signal_403 |
| `TestSentinel` | no_restart_idle, no_restart_waiting, restart_after_threshold, error_timer_reset |
| `TestDirectorSentinelIntegration` | processes_restart_request, skips_already_recovered |
| `TestAccountant` | get_capacity, set_custom_limit, per_account_limit, get_next_upload_times, get_available_accounts |
| `TestStrategistSchedule` | writes_config, skips_wrong_platform, empty_no_op |

**Запуск:**
```bat
python -m pytest tests/test_agents.py -v
# или через launch.bat → [4] Тесты → [1] Все тесты
```

---

## Текущее состояние

### Что работает полностью

- Оригинальный 6-этапный пайплайн (`run_pipeline.py`)
- Все 12 агентов инициализируются без ошибок
- SCOUT → реальный поиск yt-dlp + браузер
- CURATOR → фильтрация + phash dedup
- VISIONARY → Ollama AI метаданные с GPU lock
- NARRATOR → Kokoro TTS с GPU lock (если модель скачана)
- EDITOR → полный монтажный конвейер с TTS mix
- GUARDIAN → прокси + сессии + карантин + антибан
- PUBLISHER → параллельная загрузка 3 аккаунта одновременно
- **ACCOUNTANT** → умные лимиты + расписание из config.json + UploadScheduler интеграция
- **SENTINEL** → мониторинг CPU/RAM/GPU + авто-рестарт через DIRECTOR (порог 2 мин)
- **STRATEGIST** → A/B анализ + репосты + применение расписания в config.json аккаунтов

### Что ещё не завершено

- **AnimateDiff** — заглушка в `editor._generate_bg_ai()` (низкий приоритет)
- **Интеграционные тесты** — тесты агентного слоя покрывают юниты, E2E пайплайн не тестируется

---

## Структура файлов

```
ShortsProject/
├── run_crew.py                       ← точка входа агентного режима
├── scripts/
│   └── setup_tts.py                  ← установщик Kokoro TTS
├── tests/
│   ├── conftest.py
│   ├── test_pipeline.py              ← 55 тестов оригинального пайплайна
│   └── test_agents.py                ← 40 тестов агентного слоя (Этап 6)
├── pipeline/
│   ├── agent_memory.py               ← shared state всех агентов
│   ├── tts_utils.py                  ← утилиты TTS
│   ├── crew.py                       ← ShortsProjectCrew (сборка 12 агентов)
│   └── agents/
│       ├── base_agent.py             ← threading lifecycle + AgentStatus
│       ├── gpu_manager.py            ← GPUResourceManager + GPUPriority
│       ├── director.py               ← оркестратор + watchdog + sentinel интеграция
│       ├── commander.py              ← Telegram/CLI + Ollama intent
│       ├── scout.py                  ← поиск трендов
│       ├── curator.py                ← фильтрация + dedup
│       ├── visionary.py              ← AI метаданные
│       ├── narrator.py               ← Kokoro TTS
│       ├── editor.py                 ← монтаж + TTS оркестровка
│       ├── strategist.py             ← A/B + репосты + расписание → config.json
│       ├── guardian.py               ← прокси + сессии + антибан
│       ├── publisher.py              ← параллельная загрузка
│       ├── accountant.py             ← умные лимиты + UploadScheduler
│       └── sentinel.py               ← мониторинг + авто-рестарт через DIRECTOR
```

---

## Git история

```
7b261af  test: Этап 6 — тесты агентного слоя (40 кейсов, 11 классов)
6c31964  feat: Этап 5Б — STRATEGIST применение расписания через UploadScheduler
391de27  feat: Этап 5А — SENTINEL авто-рестарт + DIRECTOR интеграция (+ base_agent перезапись)
a4bb37c  feat: Этап 5В — ACCOUNTANT полная реализация
2ceac5b  feat: Этап 4 — GUARDIAN + PUBLISHER полная реализация
5fb8220  feat: Этап 3 — Kokoro TTS интегрирован в монтажный конвейер
c5f5da2  feat: Этап 2 — все агенты подключены к реальным модулям pipeline
c494519  feat: Этап 1 — 12 агентов CrewAI + GPU менеджер + AgentMemory
55d2cc6  Авторан и тесты
e11b8fe  test: добавлены реальные тесты (55 тест-кейсов, 9 модулей)
1342397  fix: исправлены все 18 проблем из code review
```

---

## Что нужно перед запуском агентного режима

### .env
```env
TTS_ENABLED=true
TTS_DEFAULT_LANG=en
TTS_SPEED=1.0
TTS_VOLUME=1.0
TTS_VOICE_OVER_MIX=0.85
```

### Установка TTS
```bat
launch.bat → [3] Агентный режим → [3] Установка Kokoro TTS
```
Скачивает: `kokoro-v1.9.onnx` (~310MB) + `voices-v1.0.bin` (~220MB).

### Запуск
```bat
launch.bat → [3] Агентный режим → [1] Запуск (интерактивный CLI)

# Доступные команды:
>>> статус
>>> помощь
>>> установи лимит tiktok 10
>>> запусти загрузку
>>> покажи карантин
```

---

## Сессия 04.03.2026 — Bugfix + тесты (архив)

### Code Review — 18 исправлений (`1342397`)

**Критические:**
| # | Файл | Проблема | Исправление |
|---|------|----------|-------------|
| 1 | `run_pipeline.py` | `upload_results = False` → `TypeError` в finalize | `isinstance` проверка, fallback `[]` |
| 2 | `utils.py` | Импорт `playwright` вместо `rebrowser_playwright` | Заменён импорт |
| 3 | `utils.py` | `validate_config()` вызывает `check_ollama()` — `NameError` | Ленивый импорт из `ai.py` |
| 4 | `ai.py` | Отсутствовали `json`, `re`, `save_json` импорты | Перенесены в начало |
| 5 | `uploader.py` | Нет `import shutil`, `e` вне scope | Добавлен импорт, `last_error` вынесен |
| 6 | `cloner.py` | Дублирующая заглушка `_clone_task` | Удалена, логика hflip/BG встроена |
| 7–9 | разные | Несовпадение сигнатур `stage_postprocess`, `stage_slice`, `_manual_login_flow` | Подписи выровнены |
| 10 | `config.py` | Нет `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | Добавлены через `os.getenv` |

**Логические / минорные:** `distributor.py` TODAY, `DownloadStats.total`, `requirements.txt`, frozen dataclass, дубликат файла.

### Фичи 05.03 (ранняя часть сессии)

| Фича | Суть |
|------|------|
| **A. Авто-репост** | `analytics.py` — < 500 просмотров за 48 ч → очередь + `_make_unique_variant` |
| **B. A/B тест** | `cloner.py → distributor.py → analytics.py` — варианты заголовков, сравнение через 24 ч |
| **C. Ротация фонов** | `utils.get_unique_bg()` + `bg_usage.json` |
| **D. Карантин** | `quarantine.py` — N ошибок подряд → пауза на X ч |
| **F. Умное расписание** | `upload_scheduler._get_smart_upload_times()` — анализ analytics.json |

**Инфраструктура:** `launch.bat`, `status.py`, `pytest.ini` + `@slow` маркер.
