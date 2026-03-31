from __future__ import annotations

import logging
import os
from pathlib import Path

LOGGER_NAME = "quant_lab"
_FILE_HANDLER_TAG = "_quant_lab_file_handler"
_STREAM_HANDLER_TAG = "_quant_lab_stream_handler"


def configure_logging(*, project_root: Path | None = None, level: str | None = None) -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(_normalize_level(level))
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _ensure_stream_handler(logger, formatter)
    _ensure_file_handler(logger, formatter, project_root=project_root)
    return logger


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def _ensure_stream_handler(logger: logging.Logger, formatter: logging.Formatter) -> None:
    for handler in logger.handlers:
        if getattr(handler, _STREAM_HANDLER_TAG, False):
            handler.setFormatter(formatter)
            return

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    setattr(handler, _STREAM_HANDLER_TAG, True)
    logger.addHandler(handler)


def _ensure_file_handler(
    logger: logging.Logger,
    formatter: logging.Formatter,
    *,
    project_root: Path | None,
) -> None:
    if project_root is None:
        return

    file_path = (project_root.resolve() / "data" / "logs" / "quant_lab.log").resolve()
    file_path.parent.mkdir(parents=True, exist_ok=True)

    for handler in list(logger.handlers):
        if not getattr(handler, _FILE_HANDLER_TAG, False):
            continue
        if Path(getattr(handler, "baseFilename", "")).resolve() == file_path:
            handler.setFormatter(formatter)
            return
        logger.removeHandler(handler)
        handler.close()

    handler = logging.FileHandler(file_path, encoding="utf-8")
    handler.setFormatter(formatter)
    setattr(handler, _FILE_HANDLER_TAG, True)
    logger.addHandler(handler)


def _normalize_level(raw_level: str | None) -> int:
    value = str(raw_level or os.getenv("QUANT_LAB_LOG_LEVEL") or "INFO").strip().upper()
    return getattr(logging, value, logging.INFO)
