import logging
from logging.handlers import RotatingFileHandler
from pipeline import config


def setup_logger(name: str = None) -> logging.Logger:
    """Настраивает корневой логгер (один раз) и возвращает логгер с именем name."""
    root_logger = logging.getLogger()

    # Проверяем корневой логгер, а не именованный —
    # иначе при первом вызове с именем 'foo' хэндлеры добавились бы к root,
    # при следующем вызове с именем 'bar' — снова добавились бы дубликаты.
    if not root_logger.handlers:
        config.LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        file_handler = RotatingFileHandler(
            config.LOG_FILE, maxBytes=10 * 1024 * 1024,
            backupCount=5, encoding="utf-8",
        )
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)

    return logging.getLogger(name) if name else root_logger
