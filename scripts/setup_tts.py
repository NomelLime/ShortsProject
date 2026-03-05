"""
scripts/setup_tts.py — Установка Kokoro-82M TTS.

Запуск:
    python scripts/setup_tts.py

Что делает:
  1. Устанавливает kokoro-onnx и soundfile (pip)
  2. Создаёт папку assets/tts/
  3. Скачивает kokoro-v1.9.onnx и voices-v1.0.bin с GitHub releases
  4. Проверяет что модель работает (тест синтеза)

Опции:
    --skip-download    только pip install, без скачивания моделей
    --test-only        только тест, без установки
    --lang ru          язык для теста (по умолчанию: en)
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import urllib.request
from pathlib import Path

# Добавляем корень проекта в PYTHONPATH
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

# Константы
TTS_DIR    = ROOT / "assets" / "tts"
MODEL_URL  = "https://github.com/thewh1teagle/kokoro-onnx/releases/download/model-files-v1.9/kokoro-v1.9.onnx"
VOICES_URL = "https://github.com/thewh1teagle/kokoro-onnx/releases/download/model-files-v1.9/voices-v1.0.bin"
MODEL_FILE  = TTS_DIR / "kokoro-v1.9.onnx"
VOICES_FILE = TTS_DIR / "voices-v1.0.bin"

# Ожидаемые размеры файлов (приблизительно)
MODEL_SIZE_MB  = 310   # ~310 MB
VOICES_SIZE_MB = 220   # ~220 MB


def print_step(text: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {text}")
    print(f"{'='*60}")


def install_packages() -> bool:
    """Устанавливает зависимости через pip."""
    print_step("Установка пакетов")
    packages = ["kokoro-onnx", "soundfile", "langdetect"]
    for pkg in packages:
        print(f"  → pip install {pkg}...")
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", pkg, "--quiet"],
            capture_output=False,
        )
        if result.returncode != 0:
            print(f"  ❌ Ошибка установки {pkg}")
            return False
        print(f"  ✅ {pkg}")
    return True


def download_models() -> bool:
    """Скачивает файлы моделей с GitHub."""
    print_step("Скачивание файлов модели Kokoro-82M")
    TTS_DIR.mkdir(parents=True, exist_ok=True)

    files = [
        (MODEL_URL, MODEL_FILE, MODEL_SIZE_MB, "kokoro-v1.9.onnx"),
        (VOICES_URL, VOICES_FILE, VOICES_SIZE_MB, "voices-v1.0.bin"),
    ]

    for url, dest, expected_mb, name in files:
        if dest.exists():
            size_mb = dest.stat().st_size / 1e6
            print(f"  ⏭️  {name} уже есть ({size_mb:.0f}MB) — пропускаю")
            continue

        print(f"  ⬇️  Скачиваю {name} (~{expected_mb}MB)...")
        try:
            def _progress(count, block_size, total_size):
                if total_size > 0:
                    pct = min(count * block_size * 100 // total_size, 100)
                    print(f"\r     {pct}%", end="", flush=True)

            urllib.request.urlretrieve(url, dest, _progress)
            print(f"\r  ✅ {name} ({dest.stat().st_size / 1e6:.0f}MB)")
        except Exception as e:
            print(f"\n  ❌ Ошибка скачивания {name}: {e}")
            print(f"     Скачай вручную: {url}")
            print(f"     Помести в: {TTS_DIR}/")
            return False

    return True


def run_test(lang: str = "en") -> bool:
    """Тестирует синтез речи."""
    print_step(f"Тест синтеза (lang={lang})")

    test_texts = {
        "en": "Hello! This is a test of the Kokoro text to speech system.",
        "ru": "Привет! Это тест системы синтеза речи Kokoro.",
    }
    text = test_texts.get(lang, test_texts["en"])

    try:
        import kokoro_onnx  # type: ignore
        import soundfile as sf  # type: ignore
        import numpy as np
    except ImportError as e:
        print(f"  ❌ Импорт не удался: {e}")
        print("  Установи пакеты: python scripts/setup_tts.py --skip-download")
        return False

    if not MODEL_FILE.exists() or not VOICES_FILE.exists():
        print(f"  ❌ Файлы модели не найдены в {TTS_DIR}/")
        print("  Скачай модели: python scripts/setup_tts.py --skip-install")
        return False

    print(f"  Текст: «{text[:60]}»")
    print(f"  Загружаю модель... ", end="", flush=True)

    try:
        model = kokoro_onnx.Kokoro(str(MODEL_FILE), str(VOICES_FILE))
        print("OK")

        print(f"  Синтезирую... ", end="", flush=True)
        from pipeline.tts_utils import get_voice_for_lang
        voice = get_voice_for_lang(lang)
        samples, sample_rate = model.create(text, voice=voice, speed=1.0, lang=lang)
        print(f"OK ({len(samples)/sample_rate:.1f}с аудио)")

        # Сохраняем тест
        test_out = TTS_DIR / f"test_output_{lang}.wav"
        sf.write(str(test_out), samples, sample_rate)
        print(f"  💾 Файл сохранён: {test_out}")
        print(f"  ✅ TTS работает корректно!")
        return True

    except Exception as e:
        print(f"\n  ❌ Ошибка синтеза: {e}")
        return False


def check_current_state() -> None:
    """Показывает текущее состояние TTS."""
    print_step("Текущее состояние")

    # Пакеты
    for pkg in ["kokoro_onnx", "soundfile", "langdetect"]:
        try:
            __import__(pkg)
            print(f"  ✅ {pkg}")
        except ImportError:
            print(f"  ❌ {pkg} — не установлен")

    # Файлы моделей
    for f, name in [(MODEL_FILE, "kokoro-v1.9.onnx"), (VOICES_FILE, "voices-v1.0.bin")]:
        if f.exists():
            print(f"  ✅ {name} ({f.stat().st_size/1e6:.0f}MB)")
        else:
            print(f"  ❌ {name} — не найден")


def main() -> None:
    parser = argparse.ArgumentParser(description="Установка Kokoro TTS для ShortsProject")
    parser.add_argument("--skip-download",  action="store_true", help="Только pip install")
    parser.add_argument("--skip-install",   action="store_true", help="Только скачивание моделей")
    parser.add_argument("--test-only",      action="store_true", help="Только тест")
    parser.add_argument("--check",          action="store_true", help="Проверить текущее состояние")
    parser.add_argument("--lang",           default="en", help="Язык теста (en/ru)")
    args = parser.parse_args()

    print("\n🎙️  ShortsProject — Установка Kokoro-82M TTS\n")

    if args.check:
        check_current_state()
        return

    if args.test_only:
        ok = run_test(args.lang)
        sys.exit(0 if ok else 1)

    success = True

    if not args.skip_install:
        success = install_packages()
        if not success:
            print("\n❌ Установка пакетов не удалась")
            sys.exit(1)

    if not args.skip_download:
        success = download_models()
        if not success:
            print("\n⚠️  Скачивание не удалось. Скачай вручную:")
            print(f"   {MODEL_URL}")
            print(f"   {VOICES_URL}")
            print(f"   → помести в {TTS_DIR}/")
            print("\nПосле скачивания запусти: python scripts/setup_tts.py --test-only")

    # Тест
    if success:
        run_test(args.lang)
        print("\n" + "="*60)
        print("  ✅ Kokoro TTS готов к работе!")
        print("  TTS автоматически включится в следующем run_crew.py")
        print("="*60)
    else:
        print("\n⚠️  Установка завершена с ошибками")


if __name__ == "__main__":
    main()
