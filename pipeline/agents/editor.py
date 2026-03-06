"""
pipeline/agents/editor.py — EDITOR: нарезка, постобработка, клонирование + TTS.

Полный цикл обработки одного видео:
  1. VISIONARY   → generate_metadata()   (GPU: LLM)
  2. NARRATOR    → synthesize()          (GPU: TTS)  ← новое в Этапе 3
  3. main_processing.run_processing()    (GPU: ENCODE)
     └── slicer → postprocessor (с TTS audio mix)

Умный выбор фона:
  1. Файл по теме из assets/backgrounds/
  2. Ротация (get_unique_bg) без повторов
  3. AnimateDiff (заглушка — Этап 5)
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
        Полный пайплайн одного видео:
          1. Нарезка через slicer
          2. Генерация метаданных (Visionary / Ollama)
          3. TTS синтез (Narrator / Kokoro)
          4. Постобработка с TTS миксом (ffmpeg)
        """
        from pipeline import config
        from pipeline.slicer import stage_slice
        from pipeline.postprocessor import stage_postprocess
        from pipeline.utils import detect_encoder
        from pipeline.tts_utils import tts_text_for_clip

        logger.info("[EDITOR] Обработка: %s", video_path.name)
        bg_path = self.select_background()

        # 1. Нарезка
        self._set_status(AgentStatus.RUNNING, f"нарезка {video_path.name}")
        clips = stage_slice([video_path], metadata_variants=None)
        if not clips:
            logger.warning("[EDITOR] Нарезка не дала клипов: %s", video_path.name)
            return False

        logger.info("[EDITOR] Нарезано %d клип(ов)", len(clips))

        # 2. Генерация метаданных через Visionary (с GPU lock LLM)
        meta_variants = self._get_metadata(video_path)

        # 3. TTS синтез — один файл на видео (озвучиваем hook_text)
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
            ready_clips = stage_postprocess(
                clips=clips,
                banner_path=banner_path,
                vcodec=vcodec,
                vcodec_opts=vcodec_opts,
                metadata_variants=meta_variants,
                bg_path=bg_path,
                tts_audio_paths=tts_paths,
            )

        # Очистка временных TTS файлов
        self._cleanup_tts_temp(tts_paths)

        if ready_clips:
            logger.info(
                "[EDITOR] Готово: %d/%d клипов (TTS: %s)",
                len(ready_clips), len(clips),
                "да" if any(tts_paths) else "нет",
            )
            return True

        logger.warning("[EDITOR] Постобработка не дала результатов для %s", video_path.name)
        return False

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

        for i, clip in enumerate(clips):
            # Выбираем метаданные для этого клипа
            if meta_variants:
                meta = meta_variants[i % len(meta_variants)]
            else:
                meta = {}

            text, lang = tts_text_for_clip(meta, lang_override)

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
        Умный выбор фона:
          1. По теме (совпадение в имени файла)
          2. Ротация get_unique_bg (без повторов)
          3. AnimateDiff (Этап 5)
        """
        try:
            from pipeline import config
            from pipeline.utils import get_unique_bg

            # Определяем папку с фонами
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

            # Поиск по теме
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

            # Ротация без повторов
            try:
                chosen = get_unique_bg(bg_dir)
                if chosen:
                    logger.info("[EDITOR] Фон (ротация): %s", chosen.name)
                    return chosen
            except Exception:
                pass

            # Случайный
            chosen = random.choice(bg_files)
            logger.info("[EDITOR] Фон (случайный): %s", chosen.name)
            return chosen

        except Exception as e:
            logger.debug("[EDITOR] select_background: %s", e)
            return None

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

    def _generate_bg_ai(self, topic: str) -> Optional[Path]:
        """AnimateDiff генерация — Этап 5."""
        logger.info("[EDITOR] AnimateDiff: TODO — Этап 5")
        return None
