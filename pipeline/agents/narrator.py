"""
pipeline/agents/narrator.py — NARRATOR: локальный TTS через Kokoro-82M.

Kokoro-82M:
  - 82M параметров, ~0.5GB VRAM  
  - MIT лицензия, полностью бесплатный
  - Скорость ~30x realtime на RTX GPU
  - Поддерживает EN, RU и другие языки

Установка (один раз):
  pip install kokoro-onnx soundfile
  # Скачать модели: https://github.com/thewh1teagle/kokoro-onnx/releases
  # kokoro-v1.9.onnx + voices-v1.0.bin → поместить в assets/tts/

GPU-блокировка: GPUPriority.TTS (приоритет 2 — ниже LLM, выше VideoGen).
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agents.gpu_manager import get_gpu_manager, GPUPriority
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

# Маппинг языков на голоса Kokoro
VOICE_MAP: Dict[str, str] = {
    "en":      "af_heart",    # американский английский, женский
    "en-gb":   "bf_emma",     # британский английский
    "ru":      "af_heart",    # русский (через многоязычную модель)
    "default": "af_heart",
}

# Путь к файлам модели
_TTS_DIR    = Path("assets/tts")
_MODEL_FILE = _TTS_DIR / "kokoro-v1.9.onnx"
_VOICES_FILE = _TTS_DIR / "voices-v1.0.bin"


class Narrator(BaseAgent):
    """
    TTS агент. Синтезирует речь из текста локально (без интернета).

    Основной метод:
      synthesize(text, output_path, lang="en") → Optional[Path]

    Используется EDITOR'ом для создания голосовых оверлеев.
    """

    def __init__(
        self,
        memory: Optional[AgentMemory] = None,
        notify: Any = None,
    ) -> None:
        super().__init__("NARRATOR", memory or get_memory(), notify)
        self._gpu   = get_gpu_manager()
        self._model = None
        self._ready = False

    def run(self) -> None:
        logger.info("[NARRATOR] Инициализация TTS модели...")
        self._init_model()
        while not self.should_stop:
            self._set_status(AgentStatus.IDLE)
            self.sleep(60.0)

    # ------------------------------------------------------------------
    # Инициализация модели
    # ------------------------------------------------------------------

    def _init_model(self) -> None:
        """Загружает Kokoro при первом вызове. Тихо деградирует если нет."""
        try:
            import kokoro_onnx  # type: ignore
        except ImportError:
            logger.warning(
                "[NARRATOR] kokoro-onnx не установлен.\n"
                "           Установи: pip install kokoro-onnx soundfile\n"
                "           TTS будет недоступен пока не установишь."
            )
            self._ready = False
            self.memory.set("tts_available", False)
            return

        model_file  = _MODEL_FILE
        voices_file = _VOICES_FILE

        # Ищем файлы модели
        if not model_file.exists() or not voices_file.exists():
            # Пробуем найти рядом с проектом
            for alt_dir in [Path("."), Path("models/tts"), Path("../models")]:
                m = alt_dir / "kokoro-v1.9.onnx"
                v = alt_dir / "voices-v1.0.bin"
                if m.exists() and v.exists():
                    model_file, voices_file = m, v
                    break
            else:
                logger.warning(
                    "[NARRATOR] Файлы модели не найдены.\n"
                    "           Скачай с: https://github.com/thewh1teagle/kokoro-onnx/releases\n"
                    "           Помести в: %s/", _TTS_DIR,
                )
                self._ready = False
                self.memory.set("tts_available", False)
                return

        try:
            self._model = kokoro_onnx.Kokoro(str(model_file), str(voices_file))
            self._ready = True
            self.memory.set("tts_available", True)
            logger.info("[NARRATOR] Kokoro загружен ✓ (модель: %s)", model_file.name)
            self._send("🎙️ [NARRATOR] TTS готов")
        except Exception as e:
            logger.error("[NARRATOR] Ошибка загрузки модели: %s", e)
            self._ready = False
            self.memory.set("tts_available", False)

    # ------------------------------------------------------------------
    # Синтез речи
    # ------------------------------------------------------------------

    def synthesize(
        self,
        text: str,
        output_path: Path,
        lang: str = "en",
        speed: float = 1.0,
    ) -> Optional[Path]:
        """
        Синтезирует речь из текста.

        Порядок приоритетов:
          1. Voice Cloning (если VOICE_CLONE_ENABLED=True) → voice_cloner.clone_voice()
          2. Kokoro ONNX (локальный TTS) — fallback

        Args:
            text:        текст для озвучки (до 500 символов оптимально)
            output_path: путь для сохранения .wav файла
            lang:        язык: "en", "ru", "en-gb", ...
            speed:       скорость речи (0.5 – 2.0)

        Returns:
            Path к .wav файлу или None при ошибке
        """
        # ── 1. Попытка через Voice Cloning ───────────────────────────────
        try:
            from pipeline import config as cfg
            if getattr(cfg, "VOICE_CLONE_ENABLED", False):
                from pipeline.voice_cloner import clone_voice
                result = clone_voice(text, Path(output_path), lang=lang, speed=speed)
                if result is not None:
                    return result
                logger.info("[NARRATOR] Voice Cloning не дал результат — переключаемся на Kokoro")
        except Exception as _vc_exc:
            logger.warning("[NARRATOR] Voice Cloning ошибка: %s — переключаемся на Kokoro", _vc_exc)

        # ── 2. Kokoro ONNX fallback ───────────────────────────────────────
        if not self._ready:
            self._init_model()
        if not self._ready:
            logger.warning("[NARRATOR] Синтез недоступен — модель не загружена")
            return None

        if not text or not text.strip():
            logger.warning("[NARRATOR] Пустой текст — пропускаю")
            return None

        voice = VOICE_MAP.get(lang.lower(), VOICE_MAP["default"])
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        self._set_status(AgentStatus.WAITING, f"ожидание GPU для TTS")
        try:
            import soundfile as sf  # type: ignore

            with self._gpu.acquire("NARRATOR", GPUPriority.TTS):
                self._set_status(AgentStatus.RUNNING, f"TTS {lang}: {text[:30]}...")
                samples, sample_rate = self._model.create(
                    text.strip(),
                    voice=voice,
                    speed=speed,
                    lang=lang,
                )

            sf.write(str(output_path), samples, sample_rate)

            logger.info(
                "[NARRATOR] Синтез OK: %s (lang=%s, speed=%.1f, %d сэмплов)",
                output_path.name, lang, speed, len(samples),
            )
            self.memory.log_event("NARRATOR", "tts_done", {
                "lang": lang, "chars": len(text), "file": output_path.name
            })
            self._set_status(AgentStatus.IDLE)
            return output_path

        except ImportError:
            logger.error("[NARRATOR] soundfile не установлен: pip install soundfile")
            self._set_status(AgentStatus.IDLE)
            return None
        except Exception as e:
            logger.error("[NARRATOR] Ошибка TTS: %s", e)
            self._set_status(AgentStatus.ERROR, str(e))
            # Не сбрасываем в IDLE — SENTINEL должен видеть ERROR
            # EDITOR обрабатывает None как graceful degradation (TTS пропускается)
            return None

    def is_ready(self) -> bool:
        return self._ready
