"""
start_shorts_agents.py — один процесс: все агенты ShortsProject (crew), без PreLend.

Эквивалентно запуску с фоновым режимом:

  python run_crew.py --daemon

Дополнительные аргументы передаются в run_crew (например --cmd, --no-telegram).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))


def main() -> None:
    import run_crew

    argv = sys.argv[1:]
    if "--cmd" not in argv and "--daemon" not in argv:
        sys.argv = [sys.argv[0], "--daemon"] + argv
    run_crew.main()


if __name__ == "__main__":
    main()
