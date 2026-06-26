"""Structured logging — JSON formatter, request-ID context propagation.

Call `configure_logging()` once at process boot. Every log record gets:
  - `@timestamp`, `level`, `logger`, `message`
  - `request_id` (contextvar; empty outside a request)
  - `event` (the logger message template)
  - exception info inlined as `exc_type`, `exc_message`, `exc_stack`

The JSON shape is compatible with standard log shippers (vector, fluentd,
Datadog, Loki). Toggle via `ENGRAM_LOG_FORMAT=json|plain`.
"""

from __future__ import annotations

import contextvars
import json
import logging
import os
import sys
import time
import traceback
import uuid
from typing import Any

_request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "engram_request_id", default=""
)


def new_request_id() -> str:
    return f"req-{uuid.uuid4().hex[:16]}"


def set_request_id(rid: str) -> None:
    _request_id_var.set(rid)


def get_request_id() -> str:
    return _request_id_var.get()


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "@timestamp": time.strftime(
                "%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)
            )
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": _request_id_var.get(),
            "thread": record.threadName,
        }
        # Include extra fields the caller passed via logger.extra=
        for k, v in record.__dict__.items():
            if k in {
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "created", "msecs", "relativeCreated",
                "thread", "threadName", "processName", "process", "message",
                "taskName",
            }:
                continue
            if k.startswith("_"):
                continue
            try:
                json.dumps(v)
                payload[k] = v
            except TypeError:
                payload[k] = repr(v)
        if record.exc_info:
            exc_type, exc, tb = record.exc_info
            payload["exc_type"] = exc_type.__name__ if exc_type else None
            payload["exc_message"] = str(exc) if exc else None
            payload["exc_stack"] = "".join(traceback.format_tb(tb))[-4000:]
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(level: int | str | None = None) -> None:
    """Idempotent logging setup. Safe to call multiple times."""
    if level is None:
        level = os.environ.get("ENGRAM_LOG_LEVEL", "INFO")
    fmt = os.environ.get("ENGRAM_LOG_FORMAT", "json").lower()
    root = logging.getLogger()
    # Remove existing handlers so re-configuration is clean.
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.StreamHandler(sys.stderr)
    if fmt == "plain":
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s %(name)s [%(threadName)s] %(message)s"
            )
        )
    else:
        handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(level)
    # Noisy third-party loggers
    for name in ("urllib3", "httpx", "httpcore", "neo4j.notifications"):
        logging.getLogger(name).setLevel(max(logging.WARNING, logging.getLogger().level))
