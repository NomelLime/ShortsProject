"""
scripts/setup_voice_clone.py — загрузка чекпоинтов OpenVoice v2 для voice_cloner.py.

Ожидаемая структура:
  assets/openvoice/checkpoints_v2/config.json
  assets/openvoice/checkpoints_v2/checkpoint.pth

Запуск:  python scripts/setup_voice_clone.py
Опции:   --skip-download  только инструкции;  --url <zip>

Дополнительно: pip install openvoice melo-tts torch
"""
from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent
CKPT_ROOT = ROOT / "assets" / "openvoice" / "checkpoints_v2"
DEFAULT_ZIP_URL = (
    "https://myshell-public-repo-host.s3.amazonaws.com/openvoice/checkpoints_v2_0417.zip"
)


def _have_checkpoints() -> bool:
    return (CKPT_ROOT / "config.json").is_file() and (CKPT_ROOT / "checkpoint.pth").is_file()


def _find_pair(root: Path) -> Optional[Tuple[Path, Path]]:
    for cfg in root.rglob("config.json"):
        if not cfg.is_file():
            continue
        parent = cfg.parent
        ckpt = parent / "checkpoint.pth"
        if ckpt.is_file():
            return cfg, ckpt
    return None


def download_and_extract(url: str) -> bool:
    CKPT_ROOT.parent.mkdir(parents=True, exist_ok=True)
    print(f"\n→ Скачивание: {url}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "ShortsProject-setup/1.0"})
        with urllib.request.urlopen(req, timeout=300) as resp:
            data = resp.read()
    except Exception as e:
        print(f"❌ Ошибка скачивания: {e}")
        return False

    with tempfile.TemporaryDirectory() as td:
        zpath = Path(td) / "ckpt.zip"
        zpath.write_bytes(data)
        extract_to = Path(td) / "out"
        extract_to.mkdir()
        try:
            with zipfile.ZipFile(zpath, "r") as zf:
                zf.extractall(extract_to)
        except zipfile.BadZipFile:
            print("❌ Не zip-архив или битый файл")
            return False

        pair = _find_pair(extract_to)
        if not pair:
            print("❌ В архиве не найдены config.json + checkpoint.pth рядом")
            return False
        cfg, ckpt = pair
        CKPT_ROOT.mkdir(parents=True, exist_ok=True)
        shutil.copy2(cfg, CKPT_ROOT / "config.json")
        shutil.copy2(ckpt, CKPT_ROOT / "checkpoint.pth")
    print(f"✅ Установлено в {CKPT_ROOT}")
    return True


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--skip-download", action="store_true")
    p.add_argument("--url", default=DEFAULT_ZIP_URL)
    args = p.parse_args()

    print("=== OpenVoice v2 checkpoints (ShortsProject) ===")
    if _have_checkpoints():
        print(f"Уже есть: {CKPT_ROOT}")
        return 0
    if args.skip_download:
        print("Скачайте вручную и положите config.json + checkpoint.pth в:")
        print(f"  {CKPT_ROOT}")
        print(f"URL (пример): {args.url}")
        return 0
    return 0 if download_and_extract(args.url) else 1


if __name__ == "__main__":
    sys.exit(main())
