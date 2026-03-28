"""Опциональный JSON-лог (LOG_FORMAT=json)."""

from __future__ import annotations

import logging

from pythonjsonlogger import jsonlogger


def setup_json_logging(service_name: str) -> None:
    handler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        rename_fields={"asctime": "timestamp", "levelname": "level"},
    )
    handler.setFormatter(formatter)
    logging.root.handlers = [handler]
    logging.root.setLevel(logging.INFO)
    old_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.service = service_name
        return record

    logging.setLogRecordFactory(record_factory)
