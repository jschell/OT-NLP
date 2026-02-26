# pipeline/modules/logger.py
"""Structured logging setup for the Psalms NLP pipeline.

Outputs JSON-formatted log lines to stdout and, optionally, to a file.
Each line contains: ts, level, name, msg plus any extra fields passed via
the `extra` kwarg (stage, data, duration_s, run_id, rows_written).
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path


class JsonFormatter(logging.Formatter):
    """Format log records as a single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        """Serialise *record* to a JSON string."""
        log_obj: dict[str, object] = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "name": record.name,
            "msg": record.getMessage(),
        }
        for key in ("stage", "data", "duration_s", "run_id", "rows_written"):
            if hasattr(record, key):
                log_obj[key] = getattr(record, key)
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_obj)


def setup_logger(
    log_path: str = "/data/outputs/pipeline.log",
    name: str = "psalms_nlp",
) -> logging.Logger:
    """Configure a named logger with JSON output to stdout and *log_path*.

    Parameters
    ----------
    log_path:
        Filesystem path for the persistent log file.  Parent directory is
        created if absent.  On permission errors the file handler is skipped
        and stdout logging continues uninterrupted.
    name:
        Logger name; defaults to ``"psalms_nlp"``.

    Returns
    -------
    logging.Logger
        The configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = JsonFormatter()

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(fmt)
    logger.addHandler(stdout_handler)

    try:
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except (PermissionError, OSError):
        pass  # Non-fatal — stdout logging continues

    return logger
