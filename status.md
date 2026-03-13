https://github.com/NomelLime/ShortsProject
Имеется в привязке к проекту NomelLime/ShortsProject
Сразу изучи этот Git.
⚠️ GitHub Token передаётся отдельно в чате. Получи новый на github.com/settings/tokens.


# ShortsProject — Status

**Дата последнего обновления:** 13.03.2026
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

**Цель:** Добавить 12-агентную систему поверх существующего пайплайна.  
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
`psutil`, `kokoro-onnx`, `soundfile`

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

### Сессия 3 (06.03.2026) — Завершение агентов + тесты + code review

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

---

#### Code Review — раунд 1 (`1ccf1e4`, PR #2)

Полный code review агентного слоя: код, логика, безопасность, архитектура.  
Выявлено 22 проблемы (5 критических, 8 безопасность, 12 логика, 9 архитектура). Исправлено 15:

**Критические исправления:**
- `self._status` → `self.status` в publisher.py, editor.py, scout.py (AttributeError на каждом цикле)
- GPU Manager: thread пересоздаётся в `start()` (RuntimeError при повторном запуске)
- `task.event.wait(timeout=360)` + TimeoutError (зависание навечно при остановке GPU Manager)
- Narrator: убран `_set_status(IDLE)` после `_set_status(ERROR)` (маскировал ошибки от SENTINEL)

**Безопасность:**
- `.gitignore` расширен: accounts/, data/, cookies, TTS модели, browser_profile/, __pycache__ на всех уровнях
- Атомичная запись файлов (tempfile + os.replace) в agent_memory.py и quarantine.py
- TOCTOU fix в quarantine.py: load+modify+save под единым `_lock`
- Telegram polling: whitelist `TELEGRAM_ALLOWED_USER_IDS` (защита от чужих команд)

**Логика:**
- Emoji-статус `.lower().split(":")[0]` в `_dispatch("status")` (было всегда ❓)
- `_restart_count` сбрасывается раз в час (агенты не блокируются навсегда)
- Curator: перемещает rejected файлы в `archive/rejected/` (был бесконечный re-scan)
- Strategist: `load_analytics()` публичная обёртка вместо `_load_analytics()`
- Crew.start(): DIRECTOR.start() после start_all() (watchdog не видел пустой реестр)
- `langdetect` добавлен в requirements.txt
- `crewai` + `crewai-tools` удалены из requirements.txt (мёртвая зависимость, ~200MB)

**Telegram rate limiting** в notifications.py:
- Минимум 2 сек между сообщениями
- Дедупликация: одно и то же сообщение не чаще раза в 5 мин
- Авто-очистка кеша старше 10 мин

**Ollama graceful degradation** в commander.py:
- Кешируется `ollama_available` в AgentMemory
- Проверяется перед каждым вызовом — мгновенный fallback без 30-60с timeout

---

#### Code Review — раунд 2 (`984ba3b`, PR #3)

Повторная проверка выявила 2 новых бага (введённых фиксами) + 6 неисправленных из раунда 1. Все 8 закрыты:

**Новые баги (введены фиксами PR #2):**
- Publisher не передавал `platform=` в `increment_upload_count()` и `get_uploads_today()` → per-platform лимиты не работали
- agent_memory._save(): double `os.close(fd)` при ошибке → подавление реальной ошибки

**Ранее не исправленные:**
- Emoji `❓` в `_status_report()` (второе место, быстрая команда «статус») → `.lower().split(":")[0]`
- Accountant fallback: `get_uploads_today()` без `platform=` → лимиты считались суммарно
- Double watchdog race: SENTINEL + DIRECTOR рестартили одного агента дважды → `just_restarted = set()`
- Ollama response injection: нет валидации intent/targets → `VALID_INTENTS` + `VALID_TARGETS` whitelist
- `list.pop(0)` O(n) в Commander → `deque` + `popleft()` O(1)
- `set_agent_status()` писал на диск при каждом обновлении → убрана персистенция (статусы транзиентны), убран мёртвый параметр `detail`

---

### Сессия 5 (13.03.2026) — Координация, ниша, прокси GEO

#### SP Pipeline под управлением Orchestrator

`run_pipeline.py` больше не запускается вручную. Orchestrator запускает его как subprocess (шаг 3.6 главного цикла) при условии: очередь < порога + интервал выдержан + не запущен.

#### Новые модули

| Файл | Суть |
|------|------|
| `pipeline/activity_vl.py` | VL-эмуляция активности: листание ленты через Qwen2.5-VL. Принимает полный объект аккаунта `{name, dir, config, platforms}` |
| `pipeline/shared_gpu_lock.py` | Кросс-процессная GPU-блокировка через `portalocker` на файле `../../.gpu_lock`. Используется совместно с Orchestrator |
| `pipeline/niche.py` | Автоопределение ниши аккаунта. Вариант B: частотный анализ слов из `.ai_cache.json` в upload_queue. Вариант C (fallback): VL-анализ первого кадра видео. Результат кешируется в `config.json["niche"]` |

#### Изменения в существующих файлах

| Файл | Что изменилось |
|------|---------------|
| `pipeline/browser.py` | GEO-валидация прокси: `_get_proxy_country()` — GET через прокси на httpbin.org/ip → ip-api.com → countryCode. `resolve_working_proxy()` пропускает прокси если страна не совпадает с `config["country"]`. Кеш `_geo_cache` на время сессии |
| `pipeline/scheduler.py` | Окно активности: проверка `ACTIVITY_HOURS_START ≤ now.hour < ACTIVITY_HOURS_END` перед запуском. VL-семафор: `_vl_semaphore = Semaphore(ACTIVITY_VL_CONCURRENCY)` — не более N одновременных VL-сессий. Передача полного `self._account` вместо `acc_cfg` |
| `pipeline/config.py` | + `ACTIVITY_HOURS_START=8`, `ACTIVITY_HOURS_END=23`, `ACTIVITY_VL_CONCURRENCY=2` |
| `setup_account.py` | Шаг 3: страна аккаунта (двухбуквенный ISO-код, напр. US/DE/GB). Сохраняется в `cfg["country"]`. Шаги прокси и UA сдвинуты на 4 и 5 |
| `requirements.txt` | + `portalocker` (кросс-процессные GPU-блокировки) |

#### Координация GPU

Три уровня защиты от конкуренции:
1. `_vl_semaphore` — не более `ACTIVITY_VL_CONCURRENCY=2` одновременных activity-сессий
2. `shared_gpu_lock.py` — сериализация Ollama-вызовов между SP и Orchestrator
3. Окно активности — VL не работает с 23:00 до 08:00

---

### Сессия 4 (11.03.2026) — Интеграция с Orchestrator

Подключение к внешнему Оркестратору. ShortsProject переходит в режим «исполнитель»: стратегические уведомления берёт на себя Orchestrator, SP отправляет только критические алерты.

#### Изменения

| Файл | Что изменилось |
|------|---------------|
| `pipeline/notifications.py` | `send_telegram()` получил параметр `critical=False`. При `SP_TELEGRAM_CRITICAL_ONLY=true` пропускает некритичные сообщения (аналитика, A/B, расписание, репосты). `send_telegram_alert()` теперь всегда `critical=True`. |
| `run_crew.py` | `SP_DISABLE_TELEGRAM_POLLING=true` — не запускает polling-поток. Команды принимает Orchestrator через свой бот. |
| `.env.example` | Создан. Документирует `SP_TELEGRAM_CRITICAL_ONLY` и `SP_DISABLE_TELEGRAM_POLLING`. |

**Критические алерты (всегда доставляются через `send_telegram_alert()`):**
- ⚠️ CAPTCHA обнаружена
- 🔴 2FA / ручное действие требуется
- 🛑 Системные алерты SENTINEL (CPU/RAM/GPU перегрев, crash агента)

**Некритичные (фильтруются при CRITICAL_ONLY=true, Orchestrator читает данные напрямую):**
- 📊 Аналитика собрана
- 🧪 A/B результаты
- 🔁 Авто-репост
- ⏰ Запуск загрузки

---

## Текущее состояние

### Что работает полностью

- Оригинальный 6-этапный пайплайн (`run_pipeline.py`)
- Все 12 агентов инициализируются и работают корректно
- SCOUT → реальный поиск yt-dlp + браузер
- CURATOR → фильтрация + phash dedup + перемещение rejected
- VISIONARY → Ollama AI метаданные с GPU lock
- NARRATOR → Kokoro TTS с GPU lock (если модель скачана)
- EDITOR → полный монтажный конвейер с TTS mix
- GUARDIAN → прокси + сессии + карантин + антибан (thread-safe)
- PUBLISHER → параллельная загрузка с per-platform лимитами
- ACCOUNTANT → умные лимиты + расписание из config.json + UploadScheduler интеграция
- SENTINEL → мониторинг CPU/RAM/GPU + авто-рестарт через DIRECTOR (порог 2 мин, без double restart)
- STRATEGIST → A/B анализ + репосты + применение расписания в config.json аккаунтов
- Telegram → rate limiting, дедупликация, whitelist, `critical` фильтр (Сессия 4)
- Commander → валидация Ollama response, кеширование ollama_available
- Атомичная запись всех JSON файлов (agent_memory, quarantine)
- **Orchestrator-режим** → критические алерты только, polling отключаем
- **Автозапуск через Orchestrator** → `run_pipeline.py` запускается как subprocess, вручную не нужен
- **Ниша** → автоопределение из `.ai_cache.json` (B) или VL-анализ кадра (C fallback)
- **GEO-прокси** → `config["country"]` + httpbin/ip-api.com валидация, кеш в памяти
- **Activity VL** → окно 08:00–23:00, семафор max 2 сессии, кросс-процессный GPU lock

### Что ещё не завершено

- **AnimateDiff** — заглушка в `editor._generate_bg_ai()` (низкий приоритет)
- **Интеграционные тесты** — тесты агентного слоя покрывают юниты, E2E пайплайн не тестируется

---

## Структура файлов

```
ShortsProject/
├── run_crew.py                       ← точка входа агентного режима
├── run_pipeline.py                   ← классический пайплайн
├── scripts/
│   └── setup_tts.py                  ← установщик Kokoro TTS
├── tests/
│   ├── conftest.py
│   ├── test_pipeline.py              ← 55 тестов оригинального пайплайна
│   └── test_agents.py                ← 40 тестов агентного слоя (Этап 6)
├── pipeline/
│   ├── agent_memory.py               ← shared state (атомичная запись, без persist на статусах)
│   ├── tts_utils.py                  ← утилиты TTS
│   ├── quarantine.py                 ← карантин аккаунтов (thread-safe, атомичная запись)
│   ├── notifications.py              ← Telegram с rate limiting + дедупликация
│   ├── activity_vl.py                ← VL-эмуляция активности (полный объект аккаунта)
│   ├── shared_gpu_lock.py            ← кросс-процессный GPU-lock (portalocker, shared с ORC)
│   ├── niche.py                      ← автоопределение ниши (B: ai_cache freq, C: VL frame)
│   ├── crew.py                       ← ShortsProjectCrew (сборка 12 агентов)
│   └── agents/
│       ├── base_agent.py             ← threading lifecycle + AgentStatus
│       ├── gpu_manager.py            ← GPUResourceManager (restartable, timeout 360s)
│       ├── director.py               ← оркестратор + watchdog (без double restart race)
│       ├── commander.py              ← Telegram/CLI + Ollama intent (validated, cached)
│       ├── scout.py                  ← поиск трендов
│       ├── curator.py                ← фильтрация + dedup + перемещение rejected
│       ├── visionary.py              ← AI метаданные
│       ├── narrator.py               ← Kokoro TTS (ERROR виден SENTINEL)
│       ├── editor.py                 ← монтаж + TTS оркестровка
│       ├── strategist.py             ← A/B + репосты + расписание → config.json
│       ├── guardian.py               ← прокси + сессии + антибан
│       ├── publisher.py              ← параллельная загрузка (per-platform limits)
│       ├── accountant.py             ← умные лимиты + UploadScheduler (per-platform)
│       └── sentinel.py               ← мониторинг + авто-рестарт через DIRECTOR
```

---

## Git история

```
81bbdbd  Merge PR #3: fix/review-remaining
984ba3b  fix: оставшиеся баги из review 06.03 (8 штук)
3f1a3f9  Merge PR #2: fix/review-06-03-2026
1ccf1e4  fix: code review 06.03.2026 — критические баги, безопасность, логика
7b261af  test: Этап 6 — тесты агентного слоя (40 кейсов, 11 классов)
6c31964  feat: Этап 5Б — STRATEGIST применение расписания через UploadScheduler
391de27  feat: Этап 5А — SENTINEL авто-рестарт + DIRECTOR интеграция (+ base_agent перезапись)
a4bb37c  feat: Этап 5В — ACCOUNTANT полная реализация
2ceac5b  feat: Этап 4 — GUARDIAN + PUBLISHER полная реализация
5fb8220  feat: Этап 3 — Kokoro TTS интегрирован в монтажный конвейер
c5f5da2  feat: Этап 2 — все агенты подключены к реальным модулям pipeline
c494519  feat: Этап 1 — 12 агентов + GPU менеджер + AgentMemory
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
# Опционально: whitelist Telegram user IDs (через запятую)
TELEGRAM_ALLOWED_USER_IDS=123456789
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

## Сводка всех code review

### Сессия 1 (04.03) — 18 исправлений

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

### Сессия 3 (06.03) — 23 исправления (PR #2 + PR #3)

**Раунд 1 (PR #2, `1ccf1e4`) — 15 исправлений:**

| Категория | Проблема | Файл |
|-----------|----------|------|
| 🔴 Crash | `self._status` → `self.status` (AttributeError) | publisher, editor, scout |
| 🔴 Crash | GPU Manager thread не перезапускался | gpu_manager.py |
| 🔴 Crash | `event.wait()` без timeout → зависание навечно | gpu_manager.py |
| 🟠 Логика | Narrator маскировал ERROR → IDLE немедленно | narrator.py |
| 🟠 Логика | Emoji всегда ❓ (case mismatch) | commander.py (частично) |
| 🟠 Логика | `_restart_count` не сбрасывался → агенты блокированы навсегда | director.py |
| 🟠 Логика | Curator: бесконечный re-scan rejected файлов | curator.py |
| 🟠 Логика | Strategist: приватная `_load_analytics` | strategist.py + analytics.py |
| 🟠 Логика | Crew.start(): watchdog раньше агентов | crew.py |
| 🔒 Security | `.gitignore` не покрывал accounts/, data/, cookies | .gitignore |
| 🔒 Security | Неатомичная запись JSON → коррупция при crash | agent_memory, quarantine |
| 🔒 Security | TOCTOU race в quarantine (load/save не под lock) | quarantine.py |
| 🔒 Security | Telegram polling без авторизации отправителя | run_crew.py |
| 📦 Deps | `crewai` удалён (мёртвая зависимость) | requirements.txt |
| 📦 Deps | `langdetect` добавлен | requirements.txt |

**Раунд 2 (PR #3, `984ba3b`) — 8 исправлений:**

| Категория | Проблема | Файл |
|-----------|----------|------|
| 🔴 NEW | Publisher не писал per-platform счётчик → лимиты сломаны | publisher.py |
| 🔴 NEW | agent_memory: double `os.close(fd)` при ошибке | agent_memory.py |
| 🟠 Partial | Emoji ❓ в `_status_report()` (второе место) | commander.py |
| 🟠 Partial | Accountant fallback без `platform=` | accountant.py |
| 🟠 | Double watchdog race (SENTINEL + DIRECTOR) | director.py |
| 🔒 | Ollama response без валидации intent/targets | commander.py |
| 🟡 | `list.pop(0)` O(n) → deque O(1) | commander.py |
| 🟡 | `set_agent_status` писал на диск + мёртвый `detail` | agent_memory.py |

### Фичи 05.03 (ранняя часть сессии 2)

| Фича | Суть |
|------|------|
| **A. Авто-репост** | `analytics.py` — < 500 просмотров за 48 ч → очередь + `_make_unique_variant` |
| **B. A/B тест** | `cloner.py → distributor.py → analytics.py` — варианты заголовков, сравнение через 24 ч |
| **C. Ротация фонов** | `utils.get_unique_bg()` + `bg_usage.json` |
| **D. Карантин** | `quarantine.py` — N ошибок подряд → пауза на X ч |
| **F. Умное расписание** | `upload_scheduler._get_smart_upload_times()` — анализ analytics.json |

**Инфраструктура:** `launch.bat`, `status.py`, `pytest.ini` + `@slow` маркер.
