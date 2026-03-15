"""
pipeline/voice_cloner.py — Голосовое клонирование (OpenVoice v2 / RVC).

Интеграция:
  - tts_utils.py вызывает `clone_voice(text, output_path)` если
    VOICE_CLONE_ENABLED=True, иначе falls back на Kokoro.
  - reference audio загружается через ContentHub UI → путь в VOICE_CLONE_REF_AUDIO

Поддерживаемые бэкенды:
  openvoice — OpenVoice v2 (melo TTS + tone color converter)
              Установка: pip install openvoice melo-tts
              Не требует обучения — работает от 10-сек reference
  rvc       — RVC (Real-time Voice Cloning)
              Требует предобученную модель (не включена в пакет)

Конфиг (config.py через .env):
  VOICE_CLONE_ENABLED   = 0          — включить/выключить
  VOICE_CLONE_MODEL     = openvoice  — openvoice | rvc
  VOICE_CLONE_REF_AUDIO = ""         — путь к reference audio (загружается через ContentHub)

Если клонирование недоступно/ошибка — возвращает None (fallback в tts_utils.py).
"""
from __future__ import annotations

import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def clone_voice(
    text: str,
    output_path: Path,
    lang: str = "en",
    speed: float = 1.0,
) -> Optional[Path]:
    """
    Клонирует голос из reference audio и синтезирует text.

    Returns:
        Path к .wav файлу или None при ошибке / если отключено.
    """
    from pipeline import config as cfg

    if not getattr(cfg, "VOICE_CLONE_ENABLED", False):
        return None

    model   = str(getattr(cfg, "VOICE_CLONE_MODEL", "openvoice")).lower()
    ref_audio = str(getattr(cfg, "VOICE_CLONE_REF_AUDIO", ""))

    if not ref_audio or not Path(ref_audio).exists():
        logger.warning("[VoiceCloner] VOICE_CLONE_REF_AUDIO не задан или файл не найден: %s", ref_audio)
        return None

    if model == "openvoice":
        return _clone_openvoice(text, output_path, ref_audio, lang, speed)
    elif model == "rvc":
        return _clone_rvc(text, output_path, ref_audio)
    else:
        logger.warning("[VoiceCloner] Неизвестный бэкенд: %s", model)
        return None


# ── OpenVoice v2 ──────────────────────────────────────────────────────────

def _clone_openvoice(
    text: str,
    output_path: Path,
    ref_audio: str,
    lang: str,
    speed: float,
) -> Optional[Path]:
    """
    Голосовое клонирование через OpenVoice v2.

    Этапы:
      1. MeloTTS синтезирует base audio (стандартный TTS)
      2. ToneColorConverter применяет тональность reference audio
    """
    try:
        from melo.api import TTS as MeloTTS  # type: ignore
        from openvoice import se_extractor  # type: ignore
        from openvoice.api import ToneColorConverter  # type: ignore
    except ImportError as exc:
        logger.warning(
            "[VoiceCloner] OpenVoice не установлен: %s\n"
            "  pip install openvoice melo-tts",
            exc,
        )
        return None

    from pipeline.agents.gpu_manager import get_gpu_manager, GPUPriority
    gpu = get_gpu_manager()

    try:
        import torch
        device = "cuda:0" if torch.cuda.is_available() else "cpu"
    except ImportError:
        device = "cpu"

    try:
        with gpu.acquire("VOICE_CLONER", GPUPriority.TTS):
            # Маппинг языков на MeloTTS
            lang_map = {
                "ru": "RU",
                "en": "EN",
                "es": "ES",
                "pt": "PT",
                "zh": "ZH",
                "fr": "FR",
                "ko": "KR",
                "ja": "JP",
            }
            melo_lang = lang_map.get(lang.lower()[:2], "EN")

            # 1. Базовый TTS (MeloTTS)
            with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
                base_path = tmp.name

            tts_model = MeloTTS(language=melo_lang, device=device)
            speaker_ids = tts_model.hps.data.spk2id
            speaker_id  = next(iter(speaker_ids.values()))
            tts_model.tts_to_file(text, speaker_id, base_path, speed=speed)

            # 2. Tone Color Converter
            # Ищем веса конвертера в стандартном месте OpenVoice
            from pipeline import config as cfg
            ckpt_dir = Path(cfg.BASE_DIR) / "assets" / "openvoice"
            converter = ToneColorConverter(
                str(ckpt_dir / "checkpoints_v2" / "config.json"),
                device=device,
            )
            converter.load_ckpt(str(ckpt_dir / "checkpoints_v2" / "checkpoint.pth"))

            # Извлечение SE из reference audio
            target_se, _ = se_extractor.get_se(ref_audio, converter, vad=True)

            # Извлечение SE из base audio
            source_se, _ = se_extractor.get_se(base_path, converter, vad=True)

            # Применяем тональность
            converter.convert(
                audio_src_path  = base_path,
                src_se          = source_se,
                tgt_se          = target_se,
                output_path     = str(output_path),
                message         = "@VoiceCloner",
            )

        import os
        try:
            os.unlink(base_path)
        except OSError:
            pass

        if output_path.exists():
            logger.info("[VoiceCloner] OpenVoice OK: %s", output_path.name)
            return output_path
        else:
            logger.warning("[VoiceCloner] OpenVoice не создал выходной файл")
            return None

    except Exception as exc:
        logger.error("[VoiceCloner] OpenVoice ошибка: %s", exc, exc_info=True)
        return None


# ── RVC ───────────────────────────────────────────────────────────────────

def _clone_rvc(
    text: str,
    output_path: Path,
    ref_audio: str,
) -> Optional[Path]:
    """
    Голосовое клонирование через RVC (Real-time Voice Cloning).
    Требует предобученную модель и rvc пакет.
    Базовый TTS создаётся через edge-tts (CLI).
    """
    try:
        from rvc_python.infer import RVCInference  # type: ignore
    except ImportError:
        logger.warning("[VoiceCloner] rvc_python не установлен: pip install rvc-python")
        return None

    from pipeline import config as cfg

    try:
        # Базовый TTS через edge-tts (без GPU)
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp:
            base_path = tmp.name

        result = subprocess.run(
            ["edge-tts", "--text", text, "--write-media", base_path],
            capture_output=True,
            timeout=30,
        )
        if result.returncode != 0:
            logger.warning("[VoiceCloner] edge-tts ошибка: %s", result.stderr.decode()[:200])
            return None

        # RVC: применяем reference голос
        # Ищем модель по пути ref_audio (заменяем .wav/.mp3 на .pth)
        model_path = Path(ref_audio).with_suffix(".pth")
        if not model_path.exists():
            logger.warning("[VoiceCloner] RVC модель не найдена: %s", model_path)
            return None

        rvc = RVCInference(models_path=str(model_path.parent))
        rvc.load_model(model_path.stem)
        rvc.infer_file(base_path, str(output_path))

        import os
        try:
            os.unlink(base_path)
        except OSError:
            pass

        if output_path.exists():
            logger.info("[VoiceCloner] RVC OK: %s", output_path.name)
            return output_path

    except Exception as exc:
        logger.error("[VoiceCloner] RVC ошибка: %s", exc, exc_info=True)

    return None
