"""
pipeline/niche.py — Автоматическое определение ниши аккаунта.

Варианты определения (в порядке приоритета):

  B. Частотный анализ заголовков и хэштегов из .ai_cache.json в upload_queue.
     Эти файлы генерирует generate_video_metadata() для каждого видео.
     Самое надёжное — видео уже отфильтрованы под аккаунт.

  C. VL-анализ первого попавшегося видео если upload_queue пустой.
     Нужен для нового аккаунта без видео в очереди.

Результат кэшируется в accounts/{name}/config.json["niche"] — при следующем
запуске функция сразу вернёт сохранённое значение.

Экспортирует:
    detect_and_cache_niche(account: dict) -> str
"""

from __future__ import annotations

import json
import logging
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Слова которые не несут нишевой информации
_STOP_WORDS = {
    "the", "and", "for", "you", "this", "that", "with", "are", "was",
    "how", "what", "when", "why", "your", "our", "its", "can", "get",
    "top", "best", "new", "all", "my", "in", "of", "to", "a", "an",
    "is", "it", "be", "do", "on", "or", "at", "by", "up", "as",
    # хэштег-мусор
    "shorts", "viral", "trending", "fyp", "foryou", "reels", "tiktok",
    "youtube", "instagram", "video", "watch", "like", "share",
}


# ─────────────────────────────────────────────────────────────────────────────
# Вариант B — анализ .ai_cache.json из upload_queue
# ─────────────────────────────────────────────────────────────────────────────

def _extract_words_from_cache(cache_path: Path) -> List[str]:
    """Извлекает значимые слова из одного .ai_cache.json файла."""
    try:
        data = json.loads(cache_path.read_text(encoding="utf-8"))
        # ai_cache.json — список вариантов метаданных: [{title, hashtags, ...}, ...]
        words = []
        for variant in data if isinstance(data, list) else [data]:
            title = variant.get("title", "")
            tags = variant.get("hashtags", [])
            if isinstance(tags, str):
                tags = tags.split()
            text = title + " " + " ".join(tags)
            tokens = re.findall(r"[a-zA-ZА-Яа-я]{4,}", text.lower())
            words.extend(tokens)
        return words
    except Exception:
        return []


def _niche_from_upload_queue(account_dir: Path) -> Optional[str]:
    """
    Вариант B: сканирует upload_queue всех платформ на .ai_cache.json файлы,
    делает частотный анализ слов и возвращает топ-нишу.
    Возвращает None если кэш-файлов нет.
    """
    queue_root = account_dir / "upload_queue"
    if not queue_root.exists():
        return None

    all_words: List[str] = []
    for cache_file in queue_root.rglob("*.ai_cache.json"):
        all_words.extend(_extract_words_from_cache(cache_file))

    if not all_words:
        return None

    # Фильтруем стоп-слова
    filtered = [w for w in all_words if w not in _STOP_WORDS and len(w) > 3]
    if not filtered:
        return None

    counter = Counter(filtered)
    # Берём топ-3 слов и собираем короткую фразу-нишу
    top = [word for word, _ in counter.most_common(3)]
    niche = " ".join(top[:2])  # "fitness workout" — достаточно 2 слов
    logger.info("[niche] Вариант B: ниша определена из upload_queue: '%s'", niche)
    return niche


# ─────────────────────────────────────────────────────────────────────────────
# Вариант C — VL-анализ видео (fallback для пустой очереди)
# ─────────────────────────────────────────────────────────────────────────────

def _niche_from_video_vl(account_dir: Path) -> Optional[str]:
    """
    Вариант C: находит любое видео в upload_queue, извлекает кадр,
    отправляет в Qwen2.5-VL и просит определить нишу.
    Возвращает None если видео нет или VL недоступна.
    """
    queue_root = account_dir / "upload_queue"
    video_extensions = {".mp4", ".mov", ".avi", ".mkv", ".webm"}
    video_path: Optional[Path] = None

    if queue_root.exists():
        for p in queue_root.rglob("*"):
            if p.suffix.lower() in video_extensions and p.stat().st_size > 0:
                video_path = p
                break

    if not video_path:
        logger.debug("[niche] Вариант C: видео для VL не найдены в %s", queue_root)
        return None

    try:
        import cv2
        cap = cv2.VideoCapture(str(video_path))
        total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        cap.set(cv2.CAP_PROP_POS_FRAMES, max(0, total // 3))
        ret, frame = cap.read()
        cap.release()
        if not ret:
            return None
        _, buf = cv2.imencode(".jpg", frame)
        frame_bytes = buf.tobytes()
    except Exception as e:
        logger.warning("[niche] Вариант C: ошибка извлечения кадра: %s", e)
        return None

    try:
        from pipeline.config import OLLAMA_MODEL
        from pipeline.ai import ollama_generate_with_timeout
        from pipeline.shared_gpu_lock import acquire_gpu_lock

        prompt = (
            "What is the main topic or niche of this video? "
            "Answer in 2-4 English words only, like: 'fitness workout tips' or 'cooking recipes'. "
            "No explanation, just the topic phrase."
        )
        with acquire_gpu_lock(consumer="VL-NicheDetect", timeout=60):
            response = ollama_generate_with_timeout(
                model=OLLAMA_MODEL,
                prompt=prompt,
                images=[frame_bytes],
                timeout=30,
            )
        raw = (response.get("response", "") if isinstance(response, dict) else str(response)).strip()
        # Чистим лишнее
        niche = re.sub(r"[^a-zA-ZА-Яа-я\s]", "", raw).strip()[:60]
        if niche:
            logger.info("[niche] Вариант C: ниша определена VL из видео: '%s'", niche)
            return niche
    except Exception as e:
        logger.warning("[niche] Вариант C: VL анализ не удался: %s", e)

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Главная функция
# ─────────────────────────────────────────────────────────────────────────────

def detect_and_cache_niche(account: Dict[str, Any]) -> str:
    """
    Определяет нишу аккаунта и кэширует результат в config.json["niche"].

    Алгоритм:
      1. Если config["niche"] уже задан — возвращаем его (ручное или кэшированное)
      2. Вариант B: анализ .ai_cache.json из upload_queue
      3. Вариант C: VL-анализ первого видео (fallback)
      4. Если ничего — "general content"

    Args:
        account: словарь из get_all_accounts() с ключами name, dir, config
    Returns:
        строка ниши (всегда не пустая)
    """
    acc_cfg = account["config"]
    acc_dir = Path(account["dir"])

    # Уже задана (ручно или кэш)
    existing = acc_cfg.get("niche") or acc_cfg.get("topic") or acc_cfg.get("channel_topic")
    if existing:
        return existing

    # Вариант B
    niche = _niche_from_upload_queue(acc_dir)

    # Вариант C (fallback)
    if not niche:
        niche = _niche_from_video_vl(acc_dir)

    # Финальный fallback
    if not niche:
        niche = "general content"
        logger.debug("[niche][%s] Ниша не определена — используем '%s'", account["name"], niche)

    # Кэшируем в config.json
    _save_niche_to_config(acc_dir, acc_cfg, niche)
    return niche


def _save_niche_to_config(acc_dir: Path, acc_cfg: Dict, niche: str) -> None:
    """Записывает определённую нишу в config.json аккаунта."""
    config_path = acc_dir / "config.json"
    try:
        acc_cfg["niche"] = niche
        config_path.write_text(
            json.dumps(acc_cfg, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("[niche] Ниша '%s' сохранена в %s", niche, config_path)
    except Exception as e:
        logger.warning("[niche] Не удалось сохранить нишу в config.json: %s", e)
