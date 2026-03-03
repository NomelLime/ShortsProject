"""
tests/conftest.py
Общие фикстуры и настройка окружения для тестов.
"""

import sys
from pathlib import Path

# Добавляем корень проекта в sys.path чтобы импорты pipeline.* работали
ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
