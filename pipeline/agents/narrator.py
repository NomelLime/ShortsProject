"""
pipeline/agents/narrator.py — NARRATOR: TTS озвучка через Kokoro-82M.

Поддерживает RU, EN и другие языки.
Использует GPU только когда LLM не активен (GPUPriority.TTS).
"""
from __future__ import annotations
import logging
from pathlib import Path
from typing import Any, Dict, Optional
from pipeline.agents.base_agent import BaseAgent, AgentStatus
from pipeline.agents.gpu_manager import get_gpu_manager, GPUPriority
from pipeline.agent_memory import AgentMemory, get_memory

logger = logging.getLogger(__name__)

# Голоса Kokoro по языкам
KOKORO_VOICES: Dict[str, str] = {
    "en": "af_heart",    # английский
    "ru": "af_heart",    # русский (Kokoro поддерживает через многоязычную модель)
    "default": "af_heart",
}

class Narrator(BaseAgent):
    """
    TTS агент. Принимает текст + язык, возвращает Path к .wav файлу.

    Kokoro-82M:
      - 82M параметров, ~0.5GB VRAM
      - MIT лицензия
      - скорость ~30x realtime на RTX GPU
      - установка: pip install kokoro-onnx soundfile
    """

    def __init__(self, memory: Optional[AgentMemory] = None, notify: Any = None) -> None:
        super().__init__("NARRATOR", memory or get_memory(), notify)
        self._gpu   = get_gpu_manager()
        self._model = None
        self._ready = False

    def run(self) -> None:
        logger.info("[NARRATOR] Инициализация TTS...")
        self._init_model()
        while not self.should_stop:
            self._set_status(AgentStatus.IDLE)
            self.sleep(30.0)

    def _init_model(self) -> None:
        """Загружает Kokoro при первом вызове."""
        try:
            import kokoro_onnx  # type: ignore
            self._model = kokoro_onnx.Kokoro("kokoro-v1.9.onnx", "voices-v1.0.bin")
            self._ready = True
            logger.info("[NARRATOR] Kokoro загружен ✓")
        except ImportError:
            logger.warning("[NARRATOR] kokoro-onnx не установлен. TTS недоступен.")
            logger.warning("[NARRATOR] Установи: pip install kokoro-onnx soundfile")
        except FileNotFoundError as e:
            logger.warning("[NARRATOR] Файлы модели не найдены: %s", e)
            logger.warning("[NARRATOR] Скачай: https://github.com/thewh1teagle/kokoro-onnx/releases")

    def synthesize(
        self,
        text: str,
        output_path: Path,
        lang: str = "en",
        speed: float = 1.0,
    ) -> Optional[Path]:
        """
        Синтезирует речь из текста.

        Args:
            text:        текст для озвучки
            output_path: путь для сохранения .wav
            lang:        язык ("en", "ru", ...)
            speed:       скорость речи (0.5–2.0)

        Returns:
            Path к файлу или None при ошибке
        """
        if not self._ready:
            self._init_model()
        if not self._ready:
            logger.error("[NARRATOR] Модель не инициализирована")
            return None

        voice = KOKORO_VOICES.get(lang, KOKORO_VOICES["default"])
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        self._set_status(AgentStatus.RUNNING, f"TTS {lang}")
        try:
            import soundfile as sf  # type: ignore
            with self._gpu.acquire("NARRATOR", GPUPriority.TTS):
                samples, sample_rate = self._model.create(
                    text, voice=voice, speed=speed, lang=lang
                )
            sf.write(str(output_path), samples, sample_rate)
            logger.info("[NARRATOR] Синтез OK: %s (%s, %s)", output_path.name, lang, voice)
            self._set_status(AgentStatus.IDLE)
            return output_path
        except Exception as e:
            logger.error("[NARRATOR] Ошибка TTS: %s", e)
            self._set_status(AgentStatus.ERROR, str(e))
            return None
