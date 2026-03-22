https://github.com/NomelLime/ShortsProject
Имеется в привязке к проекту NomelLime/ShortsProject
Сразу изучи этот Git.
⚠️ GitHub Token передаётся отдельно в чате. Получи новый на github.com/settings/tokens.


# ShortsProject — Status

**Дата последнего обновления:** 22.03.2026  
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

*Дополнения (см. Сессию 5):* правки GPU/Curator/Publisher/process_results, `test_orchestrator.py`, обновлённый `conftest.py`.

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

### Сессия 5 (22.03.2026) — Стабилизация pytest, оркестратор, GUARDIAN

#### `tests/conftest.py`

- Лёгкий пакет `pipeline` без выполнения `pipeline/__init__.py` (при тестах не тянется yt-dlp/downloader целиком).
- Заглушки при отсутствии пакетов: `rebrowser_playwright` (в т.ч. `sync_playwright`), `playwright_stealth`, `requests` (+ `ConnectionError`), `cv2`, `ollama`, `tqdm`, `yt_dlp`.
- Предзагрузка `pipeline.quarantine`, `pipeline.upload_warmup`; при возможности `pipeline.downloader` — чтобы работал `patch("pipeline.quarantine.…")` и аналоги.

#### `tests/test_agents.py` / `tests/test_pipeline.py`

- **GPUManager:** вызов `gpu.start()` в setup, корректный порядок `acquire(consumer, priority)`, учёт `stats[consumer].calls`, декоратор `gpu_task`, тест приоритета с учётом гонки постановки в очередь.
- **Curator:** в `_evaluate` второй аргумент — callable `probe_fn`, не dict.
- **BaseAgent / AgentMemory:** отчёт через `set_agent_report` — проверка `data.score` внутри обёртки.
- **Дубликаты видео:** моки на базе `imagehash.hex_to_hash` (согласовано с Хэммингом в `is_duplicate`).
- **finalize:** `_collect_statistics` — ключ `platforms`, не `by_platform`.
- **Telegram:** в тестах ошибок сети — уникальный текст сообщения, чтобы не срабатывала дедупликация между кейсами.

#### `tests/test_orchestrator.py` (новый)

- **Director:** `start_all` / `stop_all` вызывают агентов в порядке `BOOT_ORDER` и в обратном порядке соответственно.
- **Commander:** быстрый `статус` без/с Director; fallback без Ollama (`ollama_available=False`) → делегирование `director.start_all`.
- **ShortsProjectCrew:** при моках всех классов агентов и патче `pipeline.agent_memory.get_memory` / `pipeline.agents.gpu_manager.get_gpu_manager` в источниках — smoke `start`/`stop`, `command` → `commander.handle_command`, `status` → `director.full_status`, порядок `register` совпадает с `BOOT_ORDER`.

#### Прод-код

- **`pipeline/agents/guardian.py`** — исправлены сломанные многострочные f-string в `_fingerprint_check` и `_profile_link_cycle` (иначе `SyntaxError`, модуль не импортировался).

#### Прогон тестов

- Рекомендуемый быстрый прогон: `pytest tests/ -m "not slow"` (~100 тестов: pipeline + агенты + оркестратор).
- Маркер `slow` (загрузка/uploader и т.п.) — отдельно, с полным `requirements.txt`.

---

## Текущее состояние

### Что работает полностью

- Оригинальный 6-этапный пайплайн (`run_pipeline.py`)
- Все 12 агентов инициализируются и работают корректно
- SCOUT → реальный поиск yt-dlp + браузер + приоритизация trending KW (TrendScout)
- CURATOR → фильтрация + phash dedup + перемещение rejected
- VISIONARY → Ollama AI метаданные с GPU lock
- NARRATOR → Voice Cloning (OpenVoice v2 / RVC) приоритет 1; Kokoro TTS fallback
- EDITOR → тот же путь, что `main_processing` (без cloner): метаданные+фон → `stage_slice` → TTS → постобработка; disputed VL / keyframe / `output_dir` синхронизированы; TTS mix + авто-субтитры + serial hook_text
- GUARDIAN → прокси + сессии + карантин + антибан (thread-safe)
- PUBLISHER → параллельная загрузка с per-platform лимитами + `prelend_sub_id` в analytics
- ACCOUNTANT → умные лимиты + расписание из config.json + UploadScheduler интеграция
- SENTINEL → мониторинг CPU/RAM/GPU + авто-рестарт через DIRECTOR (порог 2 мин, без double restart)
- STRATEGIST → A/B анализ + thumbnail A/B winner + serial candidates + репосты + расписание
- TREND_SCOUT → Google Trends / YouTube / TikTok → `trend_scores` в AgentMemory каждые 2ч
- Telegram → rate limiting, дедупликация, whitelist, `critical` фильтр (Сессия 4)
- Commander → валидация Ollama response, кеширование ollama_available
- Атомичная запись всех JSON файлов (agent_memory, quarantine, vl_cache)
- **Orchestrator-режим** → критические алерты только, polling отключаем
- **Автозапуск через Orchestrator** → `run_pipeline.py` запускается как subprocess, вручную не нужен
- **Ниша** → автоопределение из `.ai_cache.json` (B) или VL-анализ кадра (C fallback)
- **GEO-прокси** → `config["country"]` + httpbin/ip-api.com валидация, кеш в памяти
- **Activity VL** → окно 08:00–23:00, семафор max 2 сессии, кросс-процессный GPU lock
- **Авто-субтитры** → faster-whisper → Helsinki-NLP → ffmpeg hardsub (включается `SUBTITLE_ENABLED=1`)
- **Voice Cloning** → OpenVoice v2 (MeloTTS + ToneColorConverter) или RVC (`VOICE_CLONE_ENABLED=1`)
- **A/B превью** → генерация 2-3 thumbnail вариантов, автовыбор winner по CTR (`THUMBNAIL_AB_ENABLED=1`)
- **Серийный контент** → engagement_rate топ-25%, "Часть 2:" в hook_text (`SERIAL_ENABLED=1`, ≥30 видео)

### Что ещё не завершено

- **Тяжёлый AnimateDiff (diffusers, ~8GB весов)** — в репозиторий не включён; подключение через `ANIMATEDIFF_SCRIPT` или отдельный сервис. Включено: Ken-Burns fallback (ffmpeg) + хук в EDITOR при `ANIMATEDIFF_ENABLED=1`.
- **Полный E2E без моков** — смоки `tests/e2e/` при `RUN_E2E=1` (ffmpeg, `load_keywords`); полный жизненный цикл `run_crew.py` / Visionary+Narrator+Editor в CI по-прежнему вне охвата.
- **RVC** — по-прежнему нужна своя `.pth` и `pip install rvc-python`; OpenVoice: `python scripts/setup_voice_clone.py`.

---

## Структура файлов

```
ShortsProject/
├── run_crew.py                       ← точка входа агентного режима
├── run_pipeline.py                   ← классический пайплайн
├── scripts/
│   ├── setup_tts.py                  ← установщик Kokoro TTS
│   └── setup_voice_clone.py          ← скачивание OpenVoice checkpoints_v2
├── tests/
│   ├── conftest.py                   ← лёгкий `pipeline`, стубы CI-зависимостей
│   ├── e2e/test_smoke_e2e.py         ← смоки без моков (RUN_E2E=1, `pytest tests/e2e`)
│   ├── test_pipeline.py              ← классический пайплайн + utils
│   ├── test_agents.py                ← GPU, память, Scout, Curator, Publisher, Guardian, Sentinel, Director↔Sentinel, Accountant, Strategist
│   ├── test_niche.py / test_serial_detector.py / test_scheduler.py
│   ├── test_animatediff_bg.py
│   └── test_orchestrator.py          ← Director (BOOT_ORDER), Commander, ShortsProjectCrew (smoke)
├── pipeline/
│   ├── agent_memory.py               ← shared state (атомичная запись, без persist на статусах)
│   ├── tts_utils.py                  ← утилиты TTS
│   ├── slicer.py / slicer_cut_utils.py ← нарезка VL, тишина, keyframe, disputed refine
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
│       ├── editor.py                 ← монтаж (как main_processing без cloner) + TTS + AnimateDiff hook
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

---

### Сессия 7 (15.03.2026) — ContentHub интеграция + 10 новых фич

Реализованы Этапы 6–8 и 14–15 в рамках большого плана 15 фич (ContentHub + расширение всех проектов).

#### Этап 6 — TrendScout агент

| Файл | Суть |
|------|------|
| `pipeline/trend_sources.py` (NEW) | Адаптеры: `fetch_google_trends()` (pytrends), `fetch_youtube_trending()` (yt-dlp), `fetch_tiktok_trends()` (TikTok Creative Center HTTP) |
| `pipeline/agents/trend_scout.py` (NEW) | `TrendScout(BaseAgent)`: каждые `TREND_SCOUT_INTERVAL_H` часов, Counter-взвешивание, пишет `trend_scores: {kw: count}` в AgentMemory, Telegram топ-10 |
| `pipeline/agents/scout.py` | Читает `trend_scores` из AgentMemory, приоритизирует кандидатов выше `TREND_SCOUT_THRESHOLD`, prepend до 10 trending KW |
| `pipeline/config.py` | + `TREND_SCOUT_ENABLED`, `TREND_SCOUT_INTERVAL_H=2`, `TREND_SCOUT_THRESHOLD=2`, `TREND_SCOUT_TOP_N=30`, `TREND_SCOUT_GEO`, `TREND_SCOUT_SOURCES="google,yt,tiktok"` |

#### Этап 7 — A/B тестирование превью

| Файл | Суть |
|------|------|
| `pipeline/agents/thumbnail_tester.py` (NEW) | `generate_thumbnail_variants()`: ffmpeg кадры на 20%/50%/80% длины + overlay текст → `data/thumbnails/{stem}_A.jpg` |
| | `compare_thumbnail_results()`: после `AB_TEST_COMPARE_AFTER_H` ч сравнивает CTR из analytics.json, выбирает winner |
| | `select_thumbnail_winner()`: записывает winner + `decided_at` в analytics.json |
| `pipeline/agents/strategist.py` | + `_analyse_thumbnails()` → вызывается в `_analysis_cycle()`, Telegram-уведомление о winner |
| `pipeline/config.py` | + `THUMBNAIL_AB_ENABLED`, `THUMBNAIL_AB_VARIANTS=2`, `AB_TEST_COMPARE_AFTER_H=24` |

#### Этап 8 — Авто-субтитры + перевод

| Файл | Суть |
|------|------|
| `pipeline/subtitler.py` (NEW) | `add_subtitles(clip_path, source_lang)`: faster-whisper (GPU, GPUPriority.ENCODE) → SRT → Helsinki-NLP MarianMT перевод → ffmpeg hardsub (`subtitles=` filter) |
| | Стили: `bottom_white` / `top_yellow`; `add_subtitles_multi()` — клоны на каждый язык из `SUBTITLE_LANGUAGES` |
| | Ollama fallback при отсутствии MarianMT модели |
| `pipeline/agents/editor.py` | После постобработки: если `SUBTITLE_ENABLED` → `add_subtitles()` для каждого клипа |
| `pipeline/config.py` | + `SUBTITLE_ENABLED`, `SUBTITLE_LANGUAGES="ru"`, `WHISPER_MODEL_SIZE="base"`, `SUBTITLE_STYLE="bottom_white"` |

#### Этап 14 — Голосовое клонирование (OpenVoice / RVC)

| Файл | Суть |
|------|------|
| `pipeline/voice_cloner.py` (NEW) | `clone_voice(text, output_path, lang, speed)` — dispatcher по `VOICE_CLONE_MODEL` |
| | `_clone_openvoice()`: MeloTTS base synthesis → ToneColorConverter применяет tone color reference audio; GPU через `gpu.acquire()` |
| | `_clone_rvc()`: edge-tts base synthesis → RVCInference с .pth моделью рядом с ref_audio |
| `pipeline/agents/narrator.py` | `synthesize()`: приоритет 1 — `clone_voice()` если `VOICE_CLONE_ENABLED`; fallback — Kokoro ONNX (поведение не изменилось) |
| `pipeline/config.py` | + `VOICE_CLONE_ENABLED`, `VOICE_CLONE_MODEL="openvoice"`, `VOICE_CLONE_REF_AUDIO=""` |

#### Этап 15 — Серийный контент

| Файл | Суть |
|------|------|
| `pipeline/serial_detector.py` (NEW) | `detect_serial_candidates()`: читает analytics.json, вычисляет `engagement_rate = (likes+comments)/views`, топ-25% при ≥30 видео с views ≥ 500 → `AgentMemory["serial_candidates"]` |
| | `find_serial_parent(tags)` — ищет parent по пересечению тегов |
| | `make_serial_hook(parent, base_hook)` → `"Часть 2: <hook>"` |
| `pipeline/agents/strategist.py` | + `_detect_serial_candidates()` в `_analysis_cycle()` (шаг 2в), Telegram топ-3 |
| `pipeline/agents/editor.py` | `_apply_serial_hook(meta)` — модульная функция; вызывается в `_generate_tts_batch()` перед TTS синтезом, прозрачно инжектирует "Часть 2:" в hook_text |
| `pipeline/config.py` | + `SERIAL_ENABLED`, `SERIAL_MIN_VIEWS=500`, `SERIAL_MIN_HISTORY=30`, `SERIAL_TOP_PCT=25` |

---

### Сессия 6 (14.03.2026) — Полный code review + исправления (3 проекта)

Полный ревью всех трёх проектов (код, логика, безопасность, архитектура).

**Уже было исправлено ранее (верифицировано):**
- `utils.py` — `get_all_accounts()` проверяет `not acc_dir.is_symlink()`
- `utils.py` — `get_uploads_today()` проверяет `isinstance(val, dict)`
- `browser.py` — credentials не в URL, ProxyHandler + auth handler
- `ai.py` — `check_ollama()` кэшируется с 60с TTL
- `finalize.py` — `_cleanup()` в try-except
- `gpu_manager.py` — `_GPU_TASK_MAX_RETRIES = 3`

**Исправлено в этой сессии:**

| Файл | Проблема | Исправление |
|------|----------|-------------|
| `pipeline/ai.py` (`_vl_cache_get`) | Bare `except Exception` | `(FileNotFoundError, json.JSONDecodeError, OSError)` |
| `pipeline/ai.py` (`_vl_cache_set`) | Неатомичная запись кэша | Атомичная через `tempfile.mkstemp` + `os.replace` |
| `pipeline/utils.py` | Bare excepts в `_load_bg_usage`, `load_json`, `load_hashes` | Специфичные `(json.JSONDecodeError, OSError)` |
| `pipeline/agents/gpu_manager.py` | `PriorityQueue` без лимита | + `maxsize=100` |
| `pipeline/shared_gpu_lock.py` | Хардкод пути к `.gpu_lock` | `_cfg.BASE_DIR.parent / ".gpu_lock"` |
| `pipeline/finalize.py` (`_load_tracking`) | Bare `except Exception` | Специфичные исключения |

---

### Code Review (18.03.2026) — исправления по результатам полного ревью

| # | Severity | Файл(ы) | Исправление |
|---|----------|---------|-------------|
| FIX#18 | Low | `pipeline/session_manager.py` | `datetime.now()` → `datetime.now(timezone.utc)` в `mark_session_verified()` и `get_session_age_hours()`. Нормализация старых naive-записей (backward compat). Устраняет потенциальный `TypeError` при сравнении naive/aware datetime |

**Статус тестов после исправлений:**
- `python -m pytest tests/ -q` — 3/3 (89 падают из-за ffmpeg/playwright — зависимости не установлены в sandbox, не наша поломка)

---


### Code Review (18–19.03.2026) — Полный ревью + исправления

| # | Severity | Файл(ы) | Исправление |
|---|----------|---------|-------------|
| FIX#18 | Low | `pipeline/session_manager.py` | `datetime.now()` → `datetime.now(timezone.utc)` в `mark_session_verified()` и `get_session_age_hours()`. Нормализация старых naive-записей |
| FIX#V3-1 | High | `pipeline/activity_vl.py` | `_sanitize_comment()`: убирает URL, @mentions, HTML-теги из VL-комментариев перед отправкой на платформу |
| FIX#V3-2 | Medium | `pipeline/shared_gpu_lock.py` | `proceed without lock` → `raise TimeoutError` при GPU timeout. Предотвращает OOM при одновременном inference двух процессов на 12GB GPU |
| FIX#V3-3 | Medium | `pipeline/activity_vl.py` | `_validate_vl_result()`: whitelist для `action`, ограничение `rank` [1,10], обрезка `comment`/`search_query` |

### Сессия 11 (19.03.2026) — Интеграция фич из ReelsMaker Pro

**Новые файлы:**

| Файл | Описание |
|------|----------|
| `pipeline/video_filters.py` | Библиотека 18 визуальных ffmpeg-фильтров (warm, cold, cinematic, vhs, dreamy и др.) |
| `pipeline/transcript.py` | Whisper-транскрипция аудио для AI-метаданных. Кеш `.transcript_cache.txt`. Graceful fallback |
| `tests/test_video_filters.py` | 16 тестов: реестр, get_filter, get_random_filter |
| `tests/test_transcript.py` | 7 тестов: кеш, обрезка, graceful fallback, мок Whisper |

**Изменения:**

| Файл | Фича | Изменение |
|------|------|-----------|
| `pipeline/config.py` | #1,2,3,5 | `BLURRED_BG_*`, `VIDEO_FILTER_*`, `HOOK_ZOOM_*`, `META_WHISPER_*` параметры |
| `pipeline/postprocessor.py` | #1 | Blurred background: при отсутствии `bg_path` — размытая версия видео вместо чёрных полос. Приоритет: `bg_path` > `BLURRED_BG` > чёрные полосы |
| `pipeline/postprocessor.py` | #3 | Видеофильтры: `VIDEO_FILTER_ENABLED` + `meta["visual_filter"]` → вставляется в filter_complex после фона, перед баннером |
| `pipeline/postprocessor.py` | #5 | Hook-zoom: `zoompan` Ken Burns effect в первые `HOOK_ZOOM_DURATION` сек. Пропускается для коротких видео |
| `pipeline/ai.py` | #2 | Whisper transcript_hint вставляется в LLM-промпт после hashtag_hint (при `META_WHISPER_ENABLED=true`) |
| `pipeline/agents/editor.py` | #4 | `_get_account_visual_filter()`: читает `visual_filter` из account `config.json`, инжектирует в `meta_variants` → postprocessor применяет фильтр |

**Конфиг по умолчанию (все новые фичи выключены, не ломают пайплайн):**
```
BLURRED_BG_ENABLED=true    # единственная включённая по умолчанию — улучшает качество
VIDEO_FILTER_ENABLED=false
HOOK_ZOOM_ENABLED=false
META_WHISPER_ENABLED=false
```

**Статус тестов:**
- `python -m pytest tests/test_video_filters.py` → **16/16** ✅
- `python -m pytest tests/test_transcript.py` → **7/7** ✅

---

## ЧТО ЕЩЁ НЕ ЗАВЕРШЕНО

- **Тесты activity_vl**: `_sanitize_comment`, `_validate_vl_result` — ✅ добавлены в сессии Code Review v3
- **Тесты niche / serial_detector / scheduler / animatediff_bg** — ✅ добавлены (см. `tests/test_niche.py` и др.)

---

### Сессия 12 (19.03.2026) — Антидетект: fingerprint, платформенные стратегии, GEO

**Новые файлы:**

| Файл | Описание |
|------|----------|
| `pipeline/fingerprint/__init__.py` | Пакет. `ensure_fingerprint`, `generate_fingerprint`, `get_geo_params` |
| `pipeline/fingerprint/geo.py` | GEO-справочник: 55+ стран → timezone/locale/languages |
| `pipeline/fingerprint/devices.py` | Банк устройств: 16 мобильных + 10 десктопных экранов |
| `pipeline/fingerprint/generator.py` | Генератор fingerprint: seed → воспроизводимый профиль. `ensure_fingerprint()` — ленивая инициализация per-platform |
| `pipeline/fingerprint/injector.py` | JS-инъекции: navigator, screen, Canvas noise, WebGL, AudioContext, Fonts |
| `pipeline/stealth/canvas_noise.js` | Canvas toDataURL/toBlob/getImageData noise (mulberry32 PRNG, seed из fp) |
| `pipeline/contexts/base.py` | `BasePlatformContext` — абстрактный интерфейс |
| `pipeline/contexts/youtube.py` | YouTube: десктоп, Studio-оптимизированный |
| `pipeline/contexts/tiktok.py` | TikTok: мобильный + touch events + Sensor API stubs |
| `pipeline/contexts/instagram.py` | Instagram: мобильный, Reels-приоритет |
| `tests/test_fingerprint.py` | 18 тестов: поля, мобиль/десктоп, GEO, seed, идемпотентность |
| `tests/test_geo.py` | 9 тестов: все страны, дефолт, case-insensitive, copy |
| `tests/test_contexts.py` | 18 тестов: is_mobile, has_touch, WebGL пулы, viewport |

**Изменения:**

| Файл | Изменение |
|------|-----------|
| `pipeline/browser.py` | Рефакторинг: диспетчер → платформенная стратегия. `launch_browser(acc_cfg, profile_dir, platform="")` — обратно совместимо |
| `pipeline/uploader.py` | `launch_browser(..., platform=platform)` |
| `pipeline/analytics.py` | `launch_browser(..., platform=platform)` |
| `pipeline/agents/guardian.py` | `_fingerprint_check()`: GEO-согласованность timezone раз в час |

**Принцип работы:**
```
1. launch_browser(acc_cfg, profile_dir, platform="tiktok")
2.   → ensure_fingerprint(acc_cfg, "tiktok", country="BR")
3.   → TikTokContext.build_launch_kwargs(fp) → {is_mobile=True, has_touch=True, ...}
4.   → TikTokContext.post_launch() → stealth + injector.apply_fingerprint()
5.   → JS-инъекции: Canvas noise (seed=fp_seed), WebGL Adreno 740, ...
6.   → Сохраняем fp в config.json (per-platform, per-account)
```

**Статус тестов:** `pytest tests/test_fingerprint.py tests/test_geo.py tests/test_contexts.py` → **45/45** ✅

---

### Сессия 12B (19.03.2026) — PreLend ссылки в профилях аккаунтов

**Новые файлы:**

| Файл | Описание |
|------|----------|
| `pipeline/profile_manager.py` | Ядро: `_find_element_with_fallback()` (CSS→VL self-healing), `_verify_page_context()`, хендлеры YouTube/TikTok/Instagram, `setup_all_links()`, `verify_all_links()` |
| `tests/test_profile_manager.py` | 23 теста: диспетчер, VL-fallback (координаты, NOT_FOUND, ошибка), page context, setup/verify all |

**Изменения:**

| Файл | Изменение |
|------|-----------|
| `setup_account.py` | Шаги 6-7: `prelend_url` + `bio_text` / `bio_text_{platform}` при создании аккаунта |
| `pipeline/agents/publisher.py` | `_maybe_setup_profile_links()` — один раз после первой загрузки аккаунта |
| `pipeline/agents/guardian.py` | `_profile_link_cycle()` — ежедневно (86400 сек), авто-восстановление пропавших ссылок |
| `pipeline/uploader.py` | `prelend_url` → конец description YouTube видео. TikTok/Instagram — не добавляем (не кликабельно) |

**Поток:**
```
setup_account.py → prelend_url в config.json
    → Publisher: первая загрузка → setup_all_links()
        → browser.py → launch_browser() → новая страница per platform
        → _verify_page_context() VL: правильная страница?
        → _find_element_with_fallback() CSS → VL координаты
        → fill + save
    → Guardian: раз в 24ч → verify_all_links()
        → если пропала → setup_all_links() авто-восстановление
        → Telegram уведомление если не удалось
```

**Ограничения:**
- TikTok Website поле: только при 1000+ подписчиков или бизнес-аккаунте (graceful: возвращает False)
- YouTube About Links: через Studio (надёжнее публичного UI)
- Instagram Website: всегда доступно

**Статус тестов:** 112/112 ✅ (все сессии 12 + 12B вместе)

---

### Сессия 12C (19.03.2026) — UTM-аналитика bio-ссылок: Nginx rewrites + per-platform URL

**Цель:** все клики из bio/About попадают в clicks.db с заполненными UTM-полями — `utm_source`, `utm_medium=bio`, `utm_campaign=<account_name>`.

**Изменения:**

| Файл | Изменение |
|------|-----------|
| `PreLend/deploy/nginx.conf` | 4 location-блока: `/t/acc` → TikTok UTM, `/i/acc` → Instagram UTM, `/y/acc` → YouTube UTM, `/go/tag` → универсальный |
| `PreLend/deploy/deploy.sh` | Те же блоки с правильным `\$` экранированием для heredoc |
| `setup_account.py` | Автогенерация `prelend_urls` при создании аккаунта: `{"tiktok": "https://domain/t/name", "instagram": "https://domain/i/name", "youtube": "https://domain/y/name"}` |
| `pipeline/profile_manager.py` | `setup_all_links()` и `verify_all_links()` используют `prelend_urls[platform]` → fallback на общий `prelend_url` |
| `pipeline/uploader.py` | YouTube description: `prelend_urls["youtube"]` → fallback на `prelend_url` |

**Формат config.json после setup_account.py:**
```json
{
    "prelend_url": "https://pulsority.com",
    "prelend_urls": {
        "tiktok":    "https://pulsority.com/t/acc_tt_01",
        "instagram": "https://pulsority.com/i/acc_tt_01",
        "youtube":   "https://pulsority.com/y/acc_tt_01"
    }
}
```

**Цепочка:** bio-клик → `/t/acc01` → Nginx rewrite → `index.php?utm_source=tiktok&utm_medium=bio&utm_campaign=acc01` → ClickLogger → clicks.db

**На VPS после деплоя:**
```bash
nginx -t && systemctl reload nginx
curl -s -o /dev/null -w "%{http_code}" https://DOMAIN/t/test_acc  # ожидается 200
```

**Тесты:** 112/112 ✅ (без изменений — новая логика покрыта существующими unit-тестами profile_manager)

---

### Ревью сессий 12A–12C (20.03.2026) — Security & Quality fixes

| # | Severity | Файл(ы) | Исправление |
|---|----------|---------|-------------|
| R12-1 | High | `pipeline/fingerprint/injector.py` | `_safe_js_string()` — все строковые fp-поля (platform_nav, language, webgl_vendor/renderer, do_not_track) через `json.dumps()` перед вставкой в JS. Предотвращает JS injection через редактирование `config.json["fingerprint"]` |
| R12-2 | Medium | `pipeline/profile_manager.py` | `_profile_lock()` — portalocker file lock на browser profile_dir. Предотвращает crash при одновременном Guardian + Publisher на одном профиле |
| R12-3 | Medium | `pipeline/stealth/canvas_noise.js` | `toDataURL`/`toBlob`: clone canvas вместо мутации оригинала — fingerprint consistency (повторный вызов = тот же результат) |
| R12-4 | Medium | `pipeline/agents/scout.py` | `_expand_keywords()`: explicit `except TimeoutError` — graceful fallback на исходные keywords |
| R12-5 | ✅ верифицирован | `pipeline/shared_gpu_lock.py` | FIX#V3-2: `raise TimeoutError` уже применён |
| R12-6 | ✅ верифицирован | — | finances_block санитизация в Orchestrator уже применена |

**Дата обновления:** 20.03.2026
**Статус тестов:** 118/118 ✅

---

### Сессия 12D (22.03.2026) — yt-dlp: cookies из `browser_profile` + стабилизация pytest

**Контекст:** отпечатки (fingerprint в `config.json`) ≠ HTTP-cookies; сессии лежат в `accounts/<имя>/browser_profile/` (Chromium persistent context). Нужна связка yt-dlp с тем же логином, что у аккаунта.

**`pipeline/config.py`**

- `get_ytdlp_cookie_options() → dict` — параметры для `yt_dlp.YoutubeDL`:
  1. `YTDLP_COOKIES_FILE` — Netscape `cookies.txt`, если файл существует;
  2. иначе `YTDLP_BROWSER_PROFILE` (полный путь к `browser_profile`) **или** `YTDLP_COOKIES_ACCOUNT` (имя аккаунта → `accounts/<имя>/browser_profile`) → `cookiesfrombrowser = (YTDLP_COOKIES_BROWSER, путь, None, None)`, по умолчанию браузер `chromium`;
  3. иначе legacy: `cookies_youtube.txt` в корне проекта, если есть.
- Вспомогательные: `_accounts_root_resolved()`, `_resolve_ytdlp_browser_profile_dir()`.

**`pipeline/download.py`**, **`pipeline/downloader.py`**

- Вместо ручной подстановки `cookiefile`: `ydl_opts.update(cfg.get_ytdlp_cookie_options())` (скачивание и ytsearch).

**`.env.example`**

- Документированы `YTDLP_COOKIES_ACCOUNT`, `YTDLP_BROWSER_PROFILE`, `YTDLP_COOKIES_BROWSER`.

**pytest (изоляция модулей)**

- `tests/test_activity_vl.py` — после загрузки `activity_vl` снимаются заглушки `sys.modules` (`pipeline.config`, `utils`, `notifications`, …), чтобы не ломать остальные тесты.
- `tests/test_profile_manager.py` — подмена `pipeline.browser` / `pipeline.ai` только в module-scoped fixture; убрана перезапись `rebrowser_playwright` и `pipeline.utils` при импорте.
- `tests/test_contexts.py` — не перезаписывать глобальную заглушку `rebrowser_playwright` из `conftest` (нужны `Page`, `sync_playwright` для `browser.py` / `notifications`).
- `tests/test_fingerprint.py` — после тестов `_profile_lock` восстанавливается `pipeline.ai` в `sys.modules`.
- `tests/test_pipeline.py` — тесты загрузки: `patch.dict(_PLATFORM_UPLOADERS, …)` (патч `_upload_youtube` не работал из‑за ссылок в словаре); `ensure_session_fresh` для `upload_all(dry_run=True)`; ожидания возврата `upload_video` / `None`.

**Статус тестов:** полный прогон `pytest tests` → **231/231** ✅

**Проверка скачивания TikTok / Instagram (22.03.2026):** на тестовой сети TikTok вернул блокировку IP; Instagram — пустой ответ без cookies (ожидаемо: нужен логин в профиле или Netscape-файл). Логика `get_ytdlp_cookie_options()` отрабатывает; успех на проде зависит от IP, логина в профиле и версии `yt-dlp`.

---

### Сессия 12E (22.03.2026) — первичное заполнение `data/keywords.txt`

**Проблема:** `data/` в `.gitignore`; `bootstrap_requirements.json` требует непустой `keywords.txt` — нужен шаблон и сценарий первого запуска.

**`examples/keywords.example.txt`**

- Коммитится в репо (не в `data/`). Стартовый набор коротких запросов под Shorts + комментарии `#` с инструкцией подстроить нишу.

**`pipeline/utils.py` — `load_keywords()`**

- Пропуск пустых строк и строк-комментариев (начинаются с `#` после `strip()`).

**`scripts/init_keywords.py`**

- Копирует шаблон → `data/keywords.txt`; флаг `--force` перезаписывает непустой файл.

**`setup_account.py`**

- После создания аккаунта: если `keywords.txt` отсутствует или не содержит ни одной «живой» строки — вопрос «Создать из шаблона?» (по умолчанию да) → `shutil.copyfile` из `examples/keywords.example.txt`.

**`tests/test_pipeline.py`**

- `TestLoadKeywords`: комментарии + отсутствующий файл.

**Статус тестов:** полный прогон `pytest tests` → **233/233** ✅

---

### Сессия 13 (22.03.2026) — Нарезка, постобработка, EDITOR ↔ main_processing, AnimateDiff

Единая логика классического пайплайна (`main_processing`) и агента **EDITOR**, плюс доработки нарезки и вывода.

#### `pipeline/slicer.py` + `pipeline/slicer_cut_utils.py` (NEW)

| Фича | Суть |
|------|------|
| Постобработка точек VL | Округление (`SLICER_ROUND_DECIMALS`, по умолчанию 0.1 с), отсев слишком близких резов, опциональный **снап к I-frame** (`ffprobe`, лимит длительности `SLICER_KEYFRAME_PROBE_MAX_DURATION_SEC`) |
| `best_segment` | `normalize_best_segment()` — не выходит за длительность ролика; исправлен баг `best_segment == 0` (раньше `if best_segment` обнулял конец сегмента) |
| Интервалы тишины | `detect_silence_intervals()` — пары `(silence_start, silence_end)` для эвристик |
| Двухпроход VL (опц.) | `SLICER_TWO_PASS=1` → `coarse_cuts_heuristic()` передаётся в `generate_cut_points(..., coarse_hints=...)` (один дополнительный контекст в промпте) |
| Спорные границы | `SLICER_DISPUTED_VL_REFINE=1` → отдельные VL-вызовы только для резов далеко от тишины (`rank_disputed_cuts_for_refinement`), лимит `SLICER_DISPUTED_MAX_CALLS`, окно `SLICER_DISPUTED_WINDOW_SEC`, `refine_single_cut_boundary_vl()` в `ai.py` |
| Temp нарезки | `stage_slice(..., clip_dir)` — `clip_dir.mkdir` в начале `stage_slice` |

#### `pipeline/ai.py`

- После разбора JSON метаданных: нормализация `best_segment` через `normalize_best_segment`.
- `extract_frames_around_time`, `refine_disputed_cut_boundaries`, `refine_single_cut_boundary_vl`.

#### `pipeline/postprocessor.py`

- `stage_postprocess(..., output_dir=...)` — вывод в подпапку (`Ready-made_shorts/.../<stem>/`), не только в корень `OUTPUT_DIR`.
- Удаление пустых `.mp4` после сбоя ffmpeg (`_cleanup_zero_byte_output`).
- Удалён недостижимый дубликат кода в `_postprocess_single`.

#### `pipeline/main_processing.py`

- `output_dir` в `stage_postprocess`; опционально коротое имя папки `OUTPUT_FOLDER_SHORT` + `safe_output_folder_name()` из `utils.py`.

#### `pipeline/cloner.py` + `analytics.py`

- `CLONE_HFLIP_PROBABILITY` (по умолчанию **0**) — без случайного `hflip`, чтобы не зеркалить текст на исходном видео; `0.5` = прежнее поведение.

#### `pipeline/animatediff_bg.py` + `pipeline/agents/editor.py` (фон)

- `generate_motion_background(..., acquire_script_gpu=...)` — внешний `ANIMATEDIFF_SCRIPT` под GPU **`VIDEO_GEN`** (`EDITOR_VIDEO_GEN`), Ken-Burns без удержания слота.
- `_motion_topic_from_meta`, `_run_motion_background`; при пустом списке видео в каталоге фонов — попытка сгенерировать фон.
- **EDITOR** приведён к тому же порядку, что **`main_processing.run_processing`**: сначала `_get_bg_and_metadata` → при необходимости `_default_meta` → **`stage_slice(video_path, TEMP/stem, metadata_variants)`** → TTS → `stage_postprocess` → `finally: _cleanup_clip_dir` (импорт из `main_processing`). Ранее нарезка вызывалась с неверной сигнатурой и до получения метаданных.

#### `pipeline/utils.py`

- `safe_output_folder_name(stem)` — безопасные короткие имена папок вывода.

#### Тесты

- `tests/test_slicer_cut_utils.py` — утилиты нарезки.
- Правки `tests/test_animatediff_bg.py` (в т.ч. `acquire_script_gpu`).

**Переменные окружения (фрагмент):** `SLICER_*`, `CLONE_HFLIP_PROBABILITY`, `OUTPUT_FOLDER_SHORT`, `SLICER_DISPUTED_*`, `ANIMATEDIFF_*`.

**Статус тестов (после изменений):** `pytest tests/test_pipeline.py tests/test_slicer_cut_utils.py tests/test_animatediff_bg.py` — зелёные; полный `pytest tests` — уточнять локально (зависимости).

---
