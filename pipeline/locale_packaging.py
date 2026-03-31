from __future__ import annotations

import json
import logging
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, Tuple

from pipeline import config
from pipeline.ai import generate_video_metadata
from pipeline.content_locale import (
    FALLBACK_CONTENT_LOCALE,
    locale_language_code,
    resolve_content_locale_for_account,
)
from pipeline.subtitler import add_subtitles_for_lang
from pipeline.tts_utils import tts_text_for_clip

logger = logging.getLogger(__name__)


def _safe_locale_tag(locale: str) -> str:
    return (locale or FALLBACK_CONTENT_LOCALE).replace("/", "_").replace("\\", "_")


def _meta_cache_path(video_path: Path, platform: str, locale: str) -> Path:
    tag = _safe_locale_tag(locale)
    return video_path.with_suffix(f".{platform}.{tag}.jit_meta.json")


def _video_cache_path(video_path: Path, platform: str, locale: str) -> Path:
    tag = _safe_locale_tag(locale)
    return video_path.with_stem(f"{video_path.stem}.{platform}.{tag}.jit")


def _localize_meta_for_account(
    video_path: Path,
    base_meta: Dict,
    account_cfg: Dict,
    platform: str,
) -> Dict:
    locale = resolve_content_locale_for_account(account_cfg or {})
    cache_path = _meta_cache_path(video_path, platform, locale)
    if cache_path.exists():
        try:
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except Exception:
            pass

    localized: Dict = dict(base_meta or {})
    try:
        variants = generate_video_metadata(
            video_path=video_path,
            num_variants=1,
            content_locale=locale,
            account_cfg=account_cfg,
            target_platform=platform,
        )
        if variants and isinstance(variants[0], dict):
            localized = dict(variants[0])
    except Exception as exc:
        logger.warning("[LocalePack] metadata JIT fallback for %s: %s", video_path.name, exc)

    localized["content_locale"] = locale
    try:
        cache_path.write_text(
            json.dumps(localized, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass
    return localized


def _mix_tts_audio(base_video: Path, text: str, lang: str, out_video: Path) -> Path:
    wav_tmp: Path | None = None
    try:
        from pipeline.voice_cloner import clone_voice

        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
            wav_tmp = Path(f.name)

        tts_wav = clone_voice(text, wav_tmp, lang=lang, speed=float(getattr(config, "TTS_SPEED", 1.0)))
        if not tts_wav or not Path(tts_wav).exists():
            return base_video

        mix = float(getattr(config, "TTS_VOICE_OVER_MIX", 0.85))
        vol = float(getattr(config, "TTS_VOLUME", 1.0))
        orig_vol = max(0.0, min(1.0, 1.0 - mix))
        voice_vol = max(0.0, min(3.0, mix * vol))
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            str(base_video),
            "-i",
            str(tts_wav),
            "-filter_complex",
            (
                f"[0:a]volume={orig_vol:.3f}[a0];"
                f"[1:a]volume={voice_vol:.3f}[a1];"
                "[a0][a1]amix=inputs=2:duration=first:normalize=0[aout]"
            ),
            "-map",
            "0:v",
            "-map",
            "[aout]",
            "-c:v",
            "copy",
            "-c:a",
            "aac",
            "-b:a",
            str(getattr(config, "AUDIO_BITRATE", "192k")),
            str(out_video),
        ]
        res = subprocess.run(cmd, capture_output=True, timeout=180)
        if res.returncode == 0 and out_video.exists():
            return out_video
        return base_video
    except Exception as exc:
        logger.warning("[LocalePack] TTS mix skipped for %s: %s", base_video.name, exc)
        return base_video
    finally:
        if wav_tmp and wav_tmp.exists():
            try:
                wav_tmp.unlink()
            except OSError:
                pass


def prepare_locale_pack_for_upload(
    video_path: Path,
    base_meta: Dict,
    account_cfg: Dict,
    platform: str,
) -> Tuple[Path, Dict]:
    """
    JIT-языковая упаковка на этапе реального слота загрузки.
    Возвращает (видео_под_locale, meta_под_locale).
    """
    localized_meta = _localize_meta_for_account(video_path, base_meta, account_cfg, platform)
    locale = str(localized_meta.get("content_locale") or resolve_content_locale_for_account(account_cfg or {}))
    lang = locale_language_code(locale)

    out_video = _video_cache_path(video_path, platform, locale)
    if out_video.exists():
        return out_video, localized_meta

    current_video = video_path

    # 1) JIT TTS под язык локали (если включено)
    if getattr(config, "TTS_ENABLED", False):
        text, _det = tts_text_for_clip(localized_meta, lang_override=lang, force_lang_override=True)
        if text:
            tts_out = out_video.with_stem(f"{out_video.stem}.tts")
            current_video = _mix_tts_audio(current_video, text, lang, tts_out)

    # 2) JIT субтитры под целевой язык аккаунта (если включено)
    if getattr(config, "SUBTITLE_ENABLED", False):
        current_video = add_subtitles_for_lang(current_video, target_lang=lang, source_lang="auto")

    if current_video != video_path and current_video.exists():
        if current_video != out_video:
            try:
                out_video.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(current_video, out_video)
            except Exception:
                return current_video, localized_meta
        return out_video, localized_meta

    return video_path, localized_meta
