"""
pipeline/tts_utils.py — Утилиты для TTS синтеза.

Вспомогательные функции:
  - detect_language()    → определяет язык текста (ru/en/...)
  - clean_tts_text()     → убирает спецсимволы, обрезает до лимита
  - pick_tts_text()      → выбирает текст из метаданных (hook → title → description)
  - get_voice_for_lang() → возвращает имя голоса Kokoro для языка
"""
from __future__ import annotations

import re
import unicodedata
from typing import Dict, Optional

# Голоса Kokoro по языкам
# Полный список: https://github.com/thewh1teagle/kokoro-onnx
KOKORO_VOICE_MAP: Dict[str, str] = {
    "en":      "af_heart",    # американский английский, женский
    "en-us":   "af_heart",
    "en-gb":   "bf_emma",     # британский английский
    "en-au":   "af_heart",
    "ru":      "af_heart",    # русский (через многоязычную модель)
    "fr":      "ff_siwis",    # французский
    "de":      "af_heart",    # немецкий
    "ja":      "jf_alpha",    # японский
    "ko":      "kf_bella",    # корейский
    "zh":      "zf_xiaobei",  # китайский
    "es":      "ef_dora",     # испанский
    "pt":      "pf_dora",     # португальский
    "it":      "if_sara",     # итальянский
    "hi":      "hf_alpha",    # хинди
    "default": "af_heart",
}

# Максимум символов для TTS (Kokoro хорошо работает до 500)
TTS_MAX_CHARS = 500

# Кириллические символы для определения русского языка
_CYRILLIC_PATTERN = re.compile(r"[а-яёА-ЯЁ]")
_LATIN_PATTERN    = re.compile(r"[a-zA-Z]")


def detect_language(text: str) -> str:
    """
    Простое определение языка по символам текста.
    Возвращает код языка: "ru", "en", ...

    Для более точного определения используй langdetect:
      pip install langdetect
    """
    if not text:
        return "en"

    # Пробуем через langdetect если установлен
    try:
        from langdetect import detect  # type: ignore
        lang = detect(text)
        # Нормализуем коды
        lang_map = {"zh-cn": "zh", "zh-tw": "zh", "pt-br": "pt"}
        return lang_map.get(lang, lang)
    except ImportError:
        pass
    except Exception:
        pass

    # Fallback: по символам
    cyrillic_count = len(_CYRILLIC_PATTERN.findall(text))
    latin_count    = len(_LATIN_PATTERN.findall(text))
    total          = cyrillic_count + latin_count

    if total == 0:
        return "en"

    if cyrillic_count / total > 0.3:
        return "ru"

    return "en"


def get_voice_for_lang(lang: str) -> str:
    """Возвращает имя голоса Kokoro для языка."""
    return KOKORO_VOICE_MAP.get(lang.lower(), KOKORO_VOICE_MAP["default"])


def clean_tts_text(text: str, max_chars: int = TTS_MAX_CHARS) -> str:
    """
    Очищает текст для TTS синтеза:
      - убирает markdown (#, *, _, ~, `)
      - убирает URL
      - убирает множественные пробелы/переносы
      - обрезает до max_chars слов (не резать на полуслове)
    """
    if not text:
        return ""

    # Убираем URL
    text = re.sub(r"https?://\S+", "", text)

    # Убираем markdown
    text = re.sub(r"[#*_~`>]", "", text)

    # Убираем эмодзи и специальные unicode символы
    text = "".join(
        ch for ch in text
        if not unicodedata.category(ch).startswith("So")  # Symbol, Other
    )

    # Нормализуем пробелы и переносы
    text = re.sub(r"\s+", " ", text).strip()

    # Убираем хэштеги
    text = re.sub(r"#\w+", "", text).strip()

    # Обрезаем до лимита (по словам, не символам)
    if len(text) > max_chars:
        words = text.split()
        truncated = ""
        for word in words:
            candidate = (truncated + " " + word).strip()
            if len(candidate) <= max_chars:
                truncated = candidate
            else:
                break
        text = truncated

    return text.strip()


def pick_tts_text(meta: dict) -> Optional[str]:
    """
    Выбирает наиболее подходящий текст из метаданных для озвучки.

    Приоритет:
      1. hook_text  — короткий цепляющий текст (лучший для TTS)
      2. title      — заголовок
      3. description — первые 2 предложения описания
    """
    # 1. hook_text — идеально для голоса
    hook = meta.get("hook_text", "").strip()
    if hook:
        return clean_tts_text(hook)

    # 2. title
    title = meta.get("title", "").strip()
    if title:
        return clean_tts_text(title)

    # 3. Первые 2 предложения description
    desc = meta.get("description", "").strip()
    if desc:
        sentences = re.split(r"[.!?]+", desc)
        short = ". ".join(s.strip() for s in sentences[:2] if s.strip())
        if short:
            return clean_tts_text(short)

    return None


def tts_text_for_clip(
    meta: dict,
    lang_override: Optional[str] = None,
    force_lang_override: bool = False,
) -> tuple[Optional[str], str]:
    """
    Возвращает (текст, язык) для синтеза.
    Язык определяется автоматически если не передан lang_override.
    """
    text = pick_tts_text(meta)
    if not text:
        return None, "en"

    detected = detect_language(text)
    if lang_override:
        ov = (lang_override or "").lower().strip()
        if force_lang_override:
            lang = ov
        # Защита от "ru" для англ. fallback-текста и наоборот.
        elif (ov == "ru" and detected.startswith("en")) or (ov.startswith("en") and detected == "ru"):
            lang = detected
        else:
            lang = ov
    else:
        lang = detected
    return text, lang
