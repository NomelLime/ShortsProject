"""
run_crew.py — Запуск ShortsProject с агентной системой.

Режимы:
  python run_crew.py           → запуск + интерактивный CLI
  python run_crew.py --daemon  → фоновый режим (только Telegram)
  python run_crew.py --cmd "статус"  → одна команда и выход

Telegram команды (если настроен бот):
  /status   → статус всех агентов
  /help     → справка
  /start    → запустить агентов
  /stop     → остановить агентов
  <текст>   → произвольная команда через COMMANDER
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path

# Добавляем корень проекта в PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent))

from pipeline.logging_setup import setup_logging
from pipeline.crew import ShortsProjectCrew

logger = logging.getLogger("run_crew")


# ──────────────────────────────────────────────────────────────────────────────
# Telegram polling (упрощённый, без библиотеки)
# ──────────────────────────────────────────────────────────────────────────────

def make_telegram_notify(token: str, chat_id: str):
    """Создаёт функцию отправки сообщений в Telegram."""
    import requests

    def notify(msg: str) -> None:
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
                timeout=10,
            )
        except Exception as e:
            logger.debug("Telegram send error: %s", e)

    return notify


def telegram_polling_loop(crew: ShortsProjectCrew, token: str, chat_id: str) -> None:
    """Фоновый поток: читает входящие сообщения и передаёт в COMMANDER."""
    import requests

    offset = 0
    logger.info("Telegram polling запущен (chat_id=%s)", chat_id)

    while True:
        try:
            resp = requests.get(
                f"https://api.telegram.org/bot{token}/getUpdates",
                params={"offset": offset, "timeout": 30},
                timeout=40,
            )
            updates = resp.json().get("result", [])
            for upd in updates:
                offset = upd["update_id"] + 1
                msg = upd.get("message", {})
                # Только наш chat_id
                if str(msg.get("chat", {}).get("id")) != str(chat_id):
                    continue
                text = msg.get("text", "").strip()
                if not text:
                    continue
                logger.info("Telegram команда: %s", text)
                result = crew.command(text)
                crew._notify(result)
        except Exception as e:
            logger.debug("Telegram polling error: %s", e)
            time.sleep(5)


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def interactive_cli(crew: ShortsProjectCrew) -> None:
    """Интерактивный режим: вводи команды в терминале."""
    print("\n" + "="*60)
    print("  ShortsProject COMMANDER  |  введи 'помощь' или 'выход'")
    print("="*60 + "\n")

    while True:
        try:
            text = input(">>> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nВыход...")
            break

        if not text:
            continue
        if text.lower() in ("выход", "exit", "quit", "/exit"):
            break

        result = crew.command(text)
        print(f"\n{result}\n")


# ──────────────────────────────────────────────────────────────────────────────
# main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="ShortsProject Crew Runner")
    parser.add_argument("--daemon",  action="store_true", help="Фоновый режим")
    parser.add_argument("--cmd",     type=str, default="", help="Одна команда и выход")
    parser.add_argument("--no-telegram", action="store_true", help="Без Telegram polling")
    args = parser.parse_args()

    setup_logging()

    # Telegram интеграция
    tg_token   = os.getenv("TELEGRAM_BOT_TOKEN", "")
    tg_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    notify     = None

    if tg_token and tg_chat_id and not args.no_telegram:
        notify = make_telegram_notify(tg_token, tg_chat_id)
        logger.info("Telegram уведомления включены")
    else:
        logger.info("Telegram не настроен — уведомления только в лог")

    # Запуск системы
    crew = ShortsProjectCrew(notify=notify)

    # Graceful shutdown
    def _shutdown(sig, frame):
        logger.info("Получен сигнал %s — останавливаю...", sig)
        crew.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    crew.start()

    # Режим одной команды
    if args.cmd:
        result = crew.command(args.cmd)
        print(result)
        crew.stop()
        return

    # Telegram polling поток
    if tg_token and tg_chat_id and not args.no_telegram:
        t = threading.Thread(
            target=telegram_polling_loop,
            args=(crew, tg_token, tg_chat_id),
            daemon=True,
        )
        t.start()

    # CLI или daemon
    if args.daemon:
        logger.info("Daemon режим — нажми Ctrl+C для остановки")
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            pass
    else:
        interactive_cli(crew)

    crew.stop()


if __name__ == "__main__":
    main()
