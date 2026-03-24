"""
pipeline/agents/editor.py — EDITOR: тот же монтажный путь, что main_processing (без cloner).

Порядок как в main_processing.run_processing:
  1. Фон + метаданные (GPU:LLM) — _get_bg_and_metadata / Visionary
  2. Нарезка — stage_slice(video, TEMP/stem, metadata_variants): VL-точки,
     postprocess точек, опционально SLICER_DISPUTED_VL_REFINE, best_segment
  3. TTS (Narrator / Kokoro), если включено
  4. Постобработка — stage_postprocess(..., output_dir=..., tts_audio_paths=...)

Умный выбор фона:
  1. Файл по теме из assets/backgrounds/
  2. Ротация (get_unique_bg) без повторов
  3. ANIMATEDIFF: внешний скрипт или Ken-Burns из .jpg/.png (animatediff_bg.py)
"""
from __future__ import annotations

import logging
import random
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agents.gpu_manager import get_gpu_manager, GPUPriority
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

_SCAN_INTERVAL = 120  # секунды между проверками очереди

# Максимальная длина строки из внешних источников (заголовков, рекомендаций) в промпте
_MAX_PROMPT_FIELD_LEN = 300


def _apply_serial_hook(meta: dict) -> dict:
    """
    Если для тегов видео найден серийный кандидат — добавляет «Часть 2:» к hook_text.
    Возвращает (возможно изменённую) копию meta.
    """
    try:
        from pipeline import config as cfg
        if not getattr(cfg, "SERIAL_ENABLED", False):
            return meta

        from pipeline.serial_detector import find_serial_parent, make_serial_hook
        tags   = meta.get("tags", [])
        parent = find_serial_parent(tags)
        if parent is None:
            return meta

        meta            = dict(meta)  # не мутируем оригинал
        base_hook       = meta.get("hook_text", "")
        meta["hook_text"] = make_serial_hook(parent, base_hook)
    except Exception:
        pass  # serial detection не должна ломать pipeline
    return meta


def _sanitize_llm_input(text: str, max_len: int = _MAX_PROMPT_FIELD_LEN) -> str:
    """Санитизирует строку перед включением в LLM-промпт.

    Защита от prompt injection: YouTube/TikTok заголовки могут содержать
    инструкции типа «Ignore previous instructions and...».

    Убирает:
      - Управляющие символы и переносы строк (сворачивает в пробел)
      - Конструкции типа «ignore», «forget», «disregard» + слово после
      - Подозрительные теги и markdown
      - Обрезает до max_len символов
    """
    import re
    if not text or not isinstance(text, str):
        return ""
    # Сворачиваем переносы и управляющие символы
    text = re.sub(r"[\r\n\t]+", " ", text)
    # Убираем markdown-подобные конструкции
    text = re.sub(r"[`*#<>{}|\[\]\\]", "", text)
    # Нейтрализуем prompt injection паттерны (case-insensitive)
    text = re.sub(
        r"\b(ignore|forget|disregard|override|bypass|jailbreak|pretend|roleplay)\b\s+\S+",
        "[filtered]",
        text,
        flags=re.IGNORECASE,
    )
    # Обрезаем
    return text.strip()[:max_len]


def _motion_topic_from_meta(video_path: Path, meta_variants: List[Dict]) -> str:
    """Тема для AnimateDiff/Ken-Burns: первая строка hook/title/description или stem файла."""
    stem = video_path.stem
    meta = meta_variants[0] if meta_variants else {}
    if not isinstance(meta, dict):
        return stem
    for key in ("hook_text", "title", "description"):
        val = meta.get(key)
        if not val or not isinstance(val, str):
            continue
        first_line = val.strip().split("\n")[0].strip()
        if first_line:
            return _sanitize_llm_input(first_line)
    return stem


class Editor(BaseAgent):
    """
    Оркестрирует весь монтажный конвейер:
      Visionary (мета) → Narrator (TTS) → postprocessor (ffmpeg + TTS mix)
    """

    def __init__(
        self,
        memory: Optional[AgentMemory] = None,
        notify: Any = None,
        visionary: Any = None,   # Visionary агент
        narrator: Any = None,    # Narrator агент
    ) -> None:
        super().__init__("EDITOR", memory or get_memory(), notify)
        self._gpu       = get_gpu_manager()
        self._visionary = visionary
        self._narrator  = narrator
        self._processed = 0
        # Кэш LLM-решений по фонам: {category: filename}, TTL = 1 цикл
        self._bg_cache: Dict[str, str] = {}

    def run(self) -> None:
        logger.info("[EDITOR] Запущен, интервал=%ds", _SCAN_INTERVAL)
        while not self.should_stop:
            self._process_cycle()
            if not self.sleep(_SCAN_INTERVAL):
                break

    # ------------------------------------------------------------------
    # Основной цикл
    # ------------------------------------------------------------------

    def _process_cycle(self) -> None:
        self._set_status(AgentStatus.RUNNING, "проверка очереди")
        # Сбрасываем кэш фонов — он живёт только один цикл
        self._bg_cache.clear()
        try:
            from pipeline import config

            preparing_dir = Path(config.PREPARING_DIR)
            if not preparing_dir.exists():
                self._set_status(AgentStatus.IDLE)
                return

            # Ищем необработанные видео файлы
            video_files = [
                f for f in preparing_dir.iterdir()
                if f.suffix.lower() in {".mp4", ".mov", ".avi", ".mkv", ".webm"}
                   and not f.name.startswith(".")
            ]

            if not video_files:
                self._set_status(AgentStatus.IDLE)
                return

            logger.info("[EDITOR] Найдено %d видео для обработки", len(video_files))
            processed_count = self._process_videos(video_files)
            self._processed += processed_count

            if processed_count:
                self.memory.log_event("EDITOR", "batch_done", {
                    "count": processed_count,
                    "total": self._processed,
                })
                self.report({"last_batch": processed_count, "total": self._processed})
                self._send(f"✂️ [EDITOR] Обработано {processed_count} видео (итого: {self._processed})")

        except Exception as e:
            logger.error("[EDITOR] Ошибка цикла: %s", e)
            self._set_status(AgentStatus.ERROR, str(e))
            raise
        finally:
            if self.status not in (AgentStatus.ERROR,):
                self._set_status(AgentStatus.IDLE)

    # ------------------------------------------------------------------
    # Обработка батча видео
    # ------------------------------------------------------------------

    def _process_videos(self, video_files: List[Path]) -> int:
        """Обрабатывает список видео, возвращает количество успешных."""
        from pipeline import config
        from pipeline.utils import detect_encoder

        vcodec, vcodec_opts = detect_encoder()
        tts_enabled = config.TTS_ENABLED and self._narrator is not None

        # Создаём временную директорию для TTS файлов
        tts_temp = Path(config.TTS_TEMP_DIR)
        tts_temp.mkdir(parents=True, exist_ok=True)

        processed = 0
        for video in video_files:
            try:
                ok = self._process_one(video, vcodec, vcodec_opts, tts_enabled, tts_temp)
                if ok:
                    processed += 1
            except Exception as e:
                logger.error("[EDITOR] Ошибка обработки %s: %s", video.name, e)

        return processed

    def _process_one(
        self,
        video_path: Path,
        vcodec: str,
        vcodec_opts: Dict,
        tts_enabled: bool,
        tts_temp: Path,
    ) -> bool:
        """
        Полный пайплайн одного видео (синхронно с main_processing):
          1. LLM: выбор фона + метаданные (один GPU:LLM захват)
          2. Нарезка stage_slice с теми же metadata_variants (best_segment, VL-резы)
          3. TTS (Narrator / Kokoro)
          4. Постобработка с TTS миксом (ffmpeg, ENCODE)
        """
        from pipeline import config
        from pipeline.main_processing import _cleanup_clip_dir, _default_meta
        from pipeline.slicer import stage_slice
        from pipeline.postprocessor import stage_postprocess
        from pipeline.utils import detect_encoder
        from pipeline.tts_utils import tts_text_for_clip

        logger.info("[EDITOR] Обработка: %s", video_path.name)

        # 1. Единый GPU:LLM блок — фон + метаданные (как в main_processing до нарезки)
        self._set_status(AgentStatus.RUNNING, f"LLM: фон+мета {video_path.name}")
        bg_path, meta_variants = self._get_bg_and_metadata(video_path)

        # Инжектируем visual_filter из account config.json в каждый вариант мета.
        _acc_visual_filter = self._get_account_visual_filter(video_path)
        if _acc_visual_filter and _acc_visual_filter != "none":
            for _mv in meta_variants:
                if not _mv.get("visual_filter"):
                    _mv["visual_filter"] = _acc_visual_filter

        if not meta_variants:
            meta_variants = [_default_meta(video_path.stem)]

        # 2. Нарезка — те же slicer / disputed VL / keyframes, что и в main_processing
        self._set_status(AgentStatus.RUNNING, f"нарезка {video_path.name}")
        clip_dir = Path(config.TEMP_DIR) / video_path.stem
        try:
            clips = stage_slice(
                video_path,
                clip_dir,
                metadata_variants=meta_variants,
            )
        except Exception as e:
            logger.error("[EDITOR] Ошибка нарезки %s: %s", video_path.name, e)
            _cleanup_clip_dir(clip_dir)
            return False

        if not clips:
            logger.warning("[EDITOR] Нарезка не дала клипов: %s", video_path.name)
            _cleanup_clip_dir(clip_dir)
            return False
        logger.info("[EDITOR] Нарезано %d клип(ов)", len(clips))

        ready_clips: List[Path] = []
        try:
            # 3. TTS синтез — один файл на клип (озвучиваем hook_text)
            tts_paths = self._generate_tts_batch(
                clips=clips,
                meta_variants=meta_variants,
                tts_enabled=tts_enabled,
                tts_temp=tts_temp,
            )

            # 4. Постобработка с TTS
            self._set_status(AgentStatus.WAITING, "ожидание GPU (encode)")
            with self._gpu.acquire("EDITOR", GPUPriority.ENCODE):
                self._set_status(AgentStatus.RUNNING, f"постобработка {video_path.name}")
                banner_path = self._pick_banner()
                from pipeline import config as _cfg
                from pipeline.utils import safe_output_folder_name

                folder_key = (
                    safe_output_folder_name(video_path.stem)
                    if getattr(_cfg, "OUTPUT_FOLDER_SHORT", False)
                    else video_path.stem
                )
                _out_dir = _cfg.OUTPUT_DIR / folder_key
                _out_dir.mkdir(parents=True, exist_ok=True)
                ready_clips = stage_postprocess(
                    clips=clips,
                    banner_path=banner_path,
                    vcodec=vcodec,
                    vcodec_opts=vcodec_opts,
                    metadata_variants=meta_variants,
                    bg_path=bg_path,
                    tts_audio_paths=tts_paths,
                    output_dir=_out_dir,
                )

            # Очистка временных TTS файлов
            self._cleanup_tts_temp(tts_paths)
        finally:
            _cleanup_clip_dir(clip_dir)

        if ready_clips:
            # Субтитры (если включены)
            from pipeline import config as _cfg
            if getattr(_cfg, "SUBTITLE_ENABLED", False):
                try:
                    from pipeline.subtitler import add_subtitles
                    subtitled = []
                    for clip in ready_clips:
                        out = add_subtitles(clip)
                        subtitled.append(out)
                    ready_clips = subtitled
                except Exception as _sub_exc:
                    logger.warning("[EDITOR] Ошибка субтитров: %s", _sub_exc)

            logger.info(
                "[EDITOR] Готово: %d/%d клипов (TTS: %s, Sub: %s)",
                len(ready_clips), len(clips),
                "да" if any(tts_paths) else "нет",
                "да" if getattr(_cfg, "SUBTITLE_ENABLED", False) else "нет",
            )
            return True

        logger.warning("[EDITOR] Постобработка не дала результатов для %s", video_path.name)
        return False

    def _get_bg_and_metadata(
        self,
        video_path: Path,
    ) -> Tuple[Optional[Path], List[Dict]]:
        """Выбирает фон и генерирует метаданные в рамках одного GPU:LLM захвата.

        Ранее это были два отдельных GPU acquire (EDITOR_BG + EDITOR_META/VISIONARY),
        что создавало 3 последовательных захвата на видео. Теперь — 2.
        """
        from pipeline import config

        # Сначала пробуем фон из кеша без GPU (быстро)
        bg_path = self._select_background_no_llm(video_path)

        # Если кеш пуст или нужна LLM — захватываем GPU один раз для обеих задач
        needs_llm_bg   = bg_path is None
        needs_llm_meta = True  # мета всегда нужны

        if needs_llm_bg or needs_llm_meta:
            self._set_status(AgentStatus.WAITING, "ожидание GPU (LLM: фон+мета)")
            try:
                with self._gpu.acquire("EDITOR_LLM", GPUPriority.LLM):
                    self._set_status(AgentStatus.RUNNING, f"LLM: фон+мета {video_path.name}")
                    if needs_llm_bg:
                        bg_path = self._choose_bg_with_llm_no_acquire(video_path)
                    meta_variants = self._get_metadata_no_acquire(video_path)
            except Exception as exc:
                logger.warning("[EDITOR] GPU:LLM недоступен: %s — используем fallback", exc)
                if needs_llm_bg:
                    bg_path = self.select_background(video_path.stem)
                meta_variants = [{}]
        else:
            meta_variants = self._get_metadata_no_acquire(video_path)

        motion_topic = _motion_topic_from_meta(video_path, meta_variants)
        if bg_path is None and getattr(config, "ANIMATEDIFF_ENABLED", False):
            try:
                gen = self._run_motion_background(motion_topic)
                if gen is not None:
                    bg_path = gen
                    logger.info("[EDITOR] Фон сгенерирован (AnimateDiff/Ken-Burns): %s", gen.name)
            except Exception as _ad_exc:
                logger.debug("[EDITOR] AnimateDiff/Ken-Burns: %s", _ad_exc)

        return bg_path, meta_variants

    # ------------------------------------------------------------------
    # Генерация метаданных
    # ------------------------------------------------------------------

    def _get_metadata(self, video_path: Path) -> List[Dict]:
        """Генерирует метаданные через Visionary если он подключён."""
        from pipeline import config

        if self._visionary is not None:
            try:
                variants = self._visionary.generate_metadata(
                    video_path,
                    num_variants=getattr(config, "AI_NUM_VARIANTS", 2),
                )
                if variants:
                    logger.info("[EDITOR] Метаданные от VISIONARY: %d вариант(ов)", len(variants))
                    return variants
            except Exception as e:
                logger.warning("[EDITOR] VISIONARY не ответил: %s — используем fallback", e)

        # Fallback: генерируем напрямую
        try:
            from pipeline.ai import generate_video_metadata
            with self._gpu.acquire("EDITOR_META", GPUPriority.LLM):
                variants = generate_video_metadata(
                    video_path,
                    num_variants=getattr(config, "AI_NUM_VARIANTS", 2),
                )
            logger.info("[EDITOR] Метаданные (direct): %d вариант(ов)", len(variants))
            return variants
        except Exception as e:
            logger.warning("[EDITOR] generate_metadata не удался: %s — пустые мета", e)
            return [{}]

    # ------------------------------------------------------------------
    # TTS синтез
    # ------------------------------------------------------------------

    def _get_metadata_no_acquire(self, video_path: Path) -> List[Dict]:
        """Генерирует метаданные БЕЗ захвата GPU (вызывать внутри existing GPU lock).

        Использовать только внутри `with self._gpu.acquire(...)` блока.
        """
        from pipeline import config
        if self._visionary is not None:
            try:
                variants = self._visionary.generate_metadata_no_acquire(
                    video_path,
                    num_variants=getattr(config, "AI_NUM_VARIANTS", 2),
                )
                if variants:
                    logger.info("[EDITOR] Метаданные от VISIONARY (no-acquire): %d вариант(ов)", len(variants))
                    return variants
            except (AttributeError, Exception) as e:
                logger.debug("[EDITOR] VISIONARY.generate_metadata_no_acquire: %s — direct fallback", e)

        try:
            from pipeline.ai import generate_video_metadata
            variants = generate_video_metadata(
                video_path,
                num_variants=getattr(config, "AI_NUM_VARIANTS", 2),
            )
            logger.info("[EDITOR] Метаданные (direct, no-acquire): %d вариант(ов)", len(variants))
            return variants
        except Exception as e:
            logger.warning("[EDITOR] generate_metadata не удался: %s — пустые мета", e)
            return [{}]

    def _select_background_no_llm(self, video_path: Path) -> Optional[Path]:
        """Возвращает фон из кеша или по теме/ротации — БЕЗ вызова LLM.

        Возвращает None если решение требует LLM-вызова.
        """
        try:
            from pipeline import config
            from pipeline.utils import get_unique_bg

            bg_dir: Optional[Path] = None
            for candidate in [
                getattr(config, "BG_VIDEO_DIR", None),
                Path(config.BASE_DIR) / "assets" / "backgrounds",
                Path(config.BASE_DIR) / "assets" / "bg_videos",
            ]:
                if candidate and Path(candidate).exists():
                    bg_dir = Path(candidate)
                    break

            if not bg_dir:
                return None

            bg_files = list(bg_dir.glob("*.mp4")) + list(bg_dir.glob("*.mov"))
            if not bg_files:
                return None

            topic = video_path.stem
            category = self._extract_category(topic)

            # 1. Кеш
            if category in self._bg_cache:
                cached_name = self._bg_cache[category]
                cached_path = bg_dir / cached_name
                if cached_path.exists():
                    logger.debug("[EDITOR] Фон из кеша (no-LLM): %s", cached_name)
                    return cached_path

            # 2. По теме — без LLM
            if topic:
                topic_words = [w for w in topic.lower().split() if len(w) > 2]
                matches = [f for f in bg_files if any(w in f.stem.lower() for w in topic_words)]
                if matches:
                    chosen = random.choice(matches)
                    self._bg_cache[category] = chosen.name
                    logger.info("[EDITOR] Фон по теме (no-LLM): %s", chosen.name)
                    return chosen

            # Нет совпадений и кеша — нужен LLM
            return None

        except Exception:
            return None

    def _choose_bg_with_llm_no_acquire(self, video_path: Path) -> Optional[Path]:
        """LLM-выбор фона БЕЗ захвата GPU (вызывать внутри existing GPU lock).

        Использовать только внутри `with self._gpu.acquire(...)` блока.
        """
        try:
            from pipeline import config
            from pipeline.utils import get_unique_bg

            bg_dir: Optional[Path] = None
            for candidate in [
                getattr(config, "BG_VIDEO_DIR", None),
                Path(config.BASE_DIR) / "assets" / "backgrounds",
                Path(config.BASE_DIR) / "assets" / "bg_videos",
            ]:
                if candidate and Path(candidate).exists():
                    bg_dir = Path(candidate)
                    break

            if not bg_dir:
                return None

            bg_files = list(bg_dir.glob("*.mp4")) + list(bg_dir.glob("*.mov"))
            if not bg_files:
                return None

            topic    = video_path.stem
            category = self._extract_category(topic)
            # Ограничиваем до 20 файлов — промпт не должен содержать 200+ имён
            file_names = [f.name for f in bg_files[:20]]

            visionary_rec  = self.memory.read_recommendation("visionary", "editor")
            strategist_rec = self.memory.read_recommendation("strategist", "editor")
            if not visionary_rec and not strategist_rec:
                # Fallback — ротация без LLM
                try:
                    chosen = get_unique_bg(bg_dir)
                    if chosen:
                        return chosen
                except Exception:
                    pass
                return random.choice(bg_files)

            visionary_hint  = visionary_rec.get("content",  "нет данных") if visionary_rec  else "нет данных"
            strategist_hint = strategist_rec.get("content", "нет данных") if strategist_rec else "нет данных"

            prompt = (
                f"Ты редактор видео. Выбери один фоновый файл для ролика.\n\n"
                f"Тема видео: {topic or category or 'не указана'}\n"
                f"VISIONARY рекомендует стиль: {_sanitize_llm_input(visionary_hint)}\n"
                f"STRATEGIST рекомендует: {_sanitize_llm_input(strategist_hint)}\n\n"
                f"Доступные файлы фонов (показаны первые {len(file_names)}):\n"
                + "\n".join(f"- {name}" for name in file_names)
                + "\n\nВерни ТОЛЬКО имя одного файла из списка выше, без пояснений."
            )

            raw = self._call_ollama_with_fallback(
                prompt=prompt,
                fallback_value=None,
                context_description=f"выбор фона для категории '{category}'",
            )

            if raw is None:
                return None

            chosen_name = self._validate_bg_choice(raw.strip(), file_names)
            if not chosen_name:
                return None

            self._bg_cache[category] = chosen_name
            logger.info("[EDITOR] Фон (LLM, no-acquire): %s", chosen_name)
            for f in bg_files:
                if f.name == chosen_name:
                    return f
            return None

        except Exception as e:
            logger.debug("[EDITOR] _choose_bg_with_llm_no_acquire: %s", e)
            return None

    def _generate_tts_batch(
        self,
        clips: List[Path],
        meta_variants: List[Dict],
        tts_enabled: bool,
        tts_temp: Path,
    ) -> List[Optional[Path]]:
        """
        Генерирует TTS файлы для батча клипов.

        Один TTS файл на клип (на основе метаданных этого клипа).
        Если TTS недоступен или отключён — возвращает список None.
        """
        if not tts_enabled:
            return [None] * len(clips)

        from pipeline.tts_utils import tts_text_for_clip
        from pipeline import config

        tts_paths: List[Optional[Path]] = []
        lang_override = getattr(config, "TTS_DEFAULT_LANG", None)
        force_lang_override = getattr(config, "TTS_FORCE_LANG_OVERRIDE", False)

        for i, clip in enumerate(clips):
            # Выбираем метаданные для этого клипа
            if meta_variants:
                meta = meta_variants[i % len(meta_variants)]
            else:
                meta = {}

            # Инжекция "Часть 2:" если видео относится к серийному контенту
            meta = _apply_serial_hook(meta)

            text, lang = tts_text_for_clip(
                meta,
                lang_override,
                force_lang_override=force_lang_override,
            )

            if not text:
                logger.debug("[EDITOR] Нет текста для TTS клипа %s", clip.name)
                tts_paths.append(None)
                continue

            # Путь для .wav
            wav_path = tts_temp / f"tts_{clip.stem}_{i}.wav"

            tts_path = self._synthesize_tts(text, wav_path, lang)
            tts_paths.append(tts_path)

            if tts_path:
                logger.info("[EDITOR] TTS готов: %s (lang=%s)", wav_path.name, lang)
            else:
                logger.debug("[EDITOR] TTS не удался для %s", clip.name)

        return tts_paths

    def _synthesize_tts(self, text: str, output_path: Path, lang: str) -> Optional[Path]:
        """Синтезирует речь через Narrator агент."""
        if self._narrator is None:
            return None
        try:
            from pipeline import config
            speed = getattr(config, "TTS_SPEED", 1.0)
            return self._narrator.synthesize(text, output_path, lang=lang, speed=speed)
        except Exception as e:
            logger.warning("[EDITOR] TTS синтез не удался: %s", e)
            return None

    def _cleanup_tts_temp(self, tts_paths: List[Optional[Path]]) -> None:
        """Удаляет временные TTS файлы после микширования."""
        for p in tts_paths:
            if p and Path(p).exists():
                try:
                    Path(p).unlink()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Выбор фона и баннера
    # ------------------------------------------------------------------

    def select_background(self, topic: str = "") -> Optional[Path]:
        """
        Умный выбор фона с LLM-контекстом:
          1. Кэш по категории (TTL = 1 цикл)
          2. LLM-решение: VISIONARY стиль + STRATEGIST рекомендация → Ollama выбирает файл
          3. По теме (совпадение в имени файла)
          4. Ротация get_unique_bg (без повторов)
          5. Случайный
          6. Если в каталоге нет .mp4/.mov и включён ANIMATEDIFF — генерация фона
             (внешний скрипт под GPU VIDEO_GEN, затем при необходимости Ken-Burns).
        """
        try:
            from pipeline import config
            from pipeline.utils import get_unique_bg

            bg_dir: Optional[Path] = None
            for candidate in [
                getattr(config, "BG_VIDEO_DIR", None),
                Path(config.BASE_DIR) / "assets" / "backgrounds",
                Path(config.BASE_DIR) / "assets" / "bg_videos",
            ]:
                if candidate and Path(candidate).exists():
                    bg_dir = Path(candidate)
                    break

            if not bg_dir:
                return None

            bg_files = list(bg_dir.glob("*.mp4")) + list(bg_dir.glob("*.mov"))
            if not bg_files:
                if getattr(config, "ANIMATEDIFF_ENABLED", False):
                    gen = self._run_motion_background(topic or "")
                    if gen is not None:
                        logger.info("[EDITOR] Фон сгенерирован (нет видео в каталоге): %s", gen.name)
                        return gen
                return None

            # Определяем категорию для кэша
            category = self._extract_category(topic)

            # 1. Кэш — если для этой категории уже есть решение в этом цикле
            if category in self._bg_cache:
                cached_name = self._bg_cache[category]
                cached_path = bg_dir / cached_name
                if cached_path.exists():
                    logger.debug("[EDITOR] Фон из кэша (категория '%s'): %s", category, cached_name)
                    return cached_path

            # 2. LLM-решение (только если есть хоть одна рекомендация)
            llm_choice = self._choose_bg_with_llm(topic, category, bg_files)
            if llm_choice:
                self._bg_cache[category] = llm_choice.name
                logger.info("[EDITOR] Фон (LLM, категория '%s'): %s", category, llm_choice.name)
                return llm_choice

            # 3. Поиск по теме
            if topic:
                topic_words = [w for w in topic.lower().split() if len(w) > 2]
                matches = [
                    f for f in bg_files
                    if any(w in f.stem.lower() for w in topic_words)
                ]
                if matches:
                    chosen = random.choice(matches)
                    logger.info("[EDITOR] Фон по теме '%s': %s", topic, chosen.name)
                    return chosen

            # 4. Ротация без повторов
            try:
                chosen = get_unique_bg(bg_dir)
                if chosen:
                    logger.info("[EDITOR] Фон (ротация): %s", chosen.name)
                    return chosen
            except Exception:
                pass

            # 5. Случайный
            chosen = random.choice(bg_files)
            logger.info("[EDITOR] Фон (случайный): %s", chosen.name)
            return chosen

        except Exception as e:
            logger.debug("[EDITOR] select_background: %s", e)
            return None

    # ------------------------------------------------------------------
    # LLM-выбор фона
    # ------------------------------------------------------------------

    def _choose_bg_with_llm(
        self,
        topic: str,
        category: str,
        bg_files: List[Path],
    ) -> Optional[Path]:
        """Спрашивает Ollama какой фон выбрать из доступных.

        Читает рекомендации VISIONARY и STRATEGIST, строит промпт,
        Ollama возвращает имя файла. Валидирует что имя из списка.
        """
        visionary_rec  = self.memory.read_recommendation("visionary", "editor")
        strategist_rec = self.memory.read_recommendation("strategist", "editor")

        if not visionary_rec and not strategist_rec:
            # Нет контекста — не тратим GPU на Ollama
            return None

        file_names = [f.name for f in bg_files[:20]]  # не более 20 — промпт не должен содержать 200+ имён
        visionary_hint  = visionary_rec.get("content",  "нет данных") if visionary_rec  else "нет данных"
        strategist_hint = strategist_rec.get("content", "нет данных") if strategist_rec else "нет данных"

        prompt = (
            f"Ты редактор видео. Выбери один фоновый файл для ролика.\n\n"
            f"Тема видео: {topic or category or 'не указана'}\n"
            f"VISIONARY рекомендует стиль: {_sanitize_llm_input(visionary_hint)}\n"
            f"STRATEGIST рекомендует: {_sanitize_llm_input(strategist_hint)}\n\n"
            f"Доступные файлы фонов (показаны первые {len(file_names)}):\n"
            + "\n".join(f"- {name}" for name in file_names) +
            "\n\nВерни ТОЛЬКО имя одного файла из списка выше, без пояснений."
        )

        self._set_status(AgentStatus.WAITING, "ожидание GPU для LLM (фон)")
        try:
            with self._gpu.acquire("EDITOR_BG", GPUPriority.LLM):
                self._set_status(AgentStatus.RUNNING, "LLM выбор фона")
                raw = self._call_ollama_with_fallback(
                    prompt=prompt,
                    fallback_value=None,
                    context_description=f"выбор фона для категории '{category}'",
                )
        except Exception as exc:
            logger.debug("[EDITOR] GPU недоступен для выбора фона: %s", exc)
            return None

        if raw is None:
            return None

        chosen_name = self._validate_bg_choice(raw.strip(), file_names)
        if not chosen_name:
            return None

        # Ищем объект Path по имени файла
        for f in bg_files:
            if f.name == chosen_name:
                return f
        return None

    @staticmethod
    def _validate_bg_choice(raw: str, file_names: List[str]) -> Optional[str]:
        """Проверяет что ответ Ollama является валидным именем файла из списка.

        Защита от галлюцинаций: ищем точное совпадение, затем частичное.
        """
        # Точное совпадение
        cleaned = raw.strip().strip('"\'').split("\n")[0].strip()
        if cleaned in file_names:
            return cleaned

        # Частичное совпадение (Ollama мог вернуть имя без расширения)
        cleaned_lower = cleaned.lower()
        for name in file_names:
            if name.lower() == cleaned_lower:
                return name
            # Имя без расширения
            if Path(name).stem.lower() == cleaned_lower:
                return name

        logger.debug(
            "[EDITOR] LLM выбрал несуществующий фон '%s' — fallback", cleaned
        )
        return None

    @staticmethod
    def _extract_category(topic: str) -> str:
        """Определяет категорию из темы для ключа кэша.

        Берём первое значимое слово (>3 символов) или 'default'.
        """
        if not topic:
            return "default"
        words = [w.lower() for w in topic.split() if len(w) > 3]
        return words[0] if words else "default"

    def _get_account_visual_filter(self, video_path: Path) -> str:
        """
        Читает visual_filter из config.json аккаунта, которому принадлежит видео.

        Orchestrator Zone 2 записывает фильтр в account config.json.
        Editor читает его здесь и передаёт в meta_variants для postprocessor.

        Логика поиска аккаунта: video_path находится в PREPARING_DIR,
        откуда его взял CURATOR из директории аккаунта. Ищем аккаунт,
        чей output/prepared-контент соответствует имени файла.

        Returns:
            Имя фильтра (str) или "" если не найден / не задан.
        """
        from pipeline import config as _cfg
        import json as _json

        try:
            accounts_root = _cfg.SP_ACCOUNTS_DIR
            if not accounts_root.exists():
                return ""

            # Ищем среди всех аккаунтов тот, у кого задан visual_filter
            # Если аккаунт один — берём его. Если несколько — пытаемся сопоставить по пути.
            matching_filter = ""
            for acc_dir in sorted(accounts_root.iterdir()):
                if not acc_dir.is_dir():
                    continue
                cfg_path = acc_dir / "config.json"
                if not cfg_path.exists():
                    continue
                try:
                    acc_cfg = _json.loads(cfg_path.read_text(encoding="utf-8"))
                    vf = acc_cfg.get("visual_filter", "")
                    if vf and vf != "none":
                        # Если видео из директории этого аккаунта — точное совпадение
                        if str(video_path).startswith(str(acc_dir)):
                            return vf
                        # Иначе запоминаем как кандидата
                        matching_filter = vf
                except Exception:
                    continue

            return matching_filter

        except Exception as exc:
            logger.debug("[EDITOR] _get_account_visual_filter ошибка: %s", exc)
            return ""

    def _pick_banner(self) -> Optional[Path]:
        """Случайный баннер из assets/banners/."""
        try:
            from pipeline import config
            from pipeline.utils import get_random_asset
            banner_dir = Path(getattr(config, "BANNER_DIR", "assets/banners"))
            if banner_dir.exists():
                return get_random_asset(banner_dir, (".png", ".jpg", ".jpeg"))
        except Exception:
            pass
        return None

    def _run_motion_background(self, topic: str) -> Optional[Path]:
        """AnimateDiff-скрипт (при наличии) под GPU VIDEO_GEN; Ken-Burns — без удержания слота."""
        try:
            from pipeline import config
            from pipeline.animatediff_bg import generate_motion_background

            script = str(getattr(config, "ANIMATEDIFF_SCRIPT", "") or "").strip()
            acquire = None
            if script:
                acquire = lambda: self._gpu.acquire(
                    "EDITOR_VIDEO_GEN", GPUPriority.VIDEO_GEN
                )
            return generate_motion_background(topic or "", acquire_script_gpu=acquire)
        except Exception as e:
            logger.debug("[EDITOR] _run_motion_background: %s", e)
            return None

    def _generate_bg_ai(self, topic: str) -> Optional[Path]:
        """Точка расширения: делегирует в animatediff_bg с учётом GPU для внешнего скрипта."""
        return self._run_motion_background(topic)
