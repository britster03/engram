"""Structured logging test — JSON formatter + request-ID contextvar."""

from __future__ import annotations

import io
import json
import logging

from engram.logging_setup import (
    JsonFormatter,
    configure_logging,
    get_request_id,
    new_request_id,
    set_request_id,
)


def test_json_formatter_emits_request_id():
    set_request_id("req-abc")
    rec = logging.LogRecord(
        name="engram.test",
        level=logging.INFO,
        pathname="x.py",
        lineno=1,
        msg="hello %s",
        args=("world",),
        exc_info=None,
    )
    rec.threadName = "t"
    out = JsonFormatter().format(rec)
    payload = json.loads(out)
    assert payload["message"] == "hello world"
    assert payload["request_id"] == "req-abc"
    assert payload["level"] == "INFO"
    set_request_id("")


def test_configure_logging_installs_json_handler():
    configure_logging(level=logging.INFO)
    # Inject a handler we can capture
    buf = io.StringIO()
    h = logging.StreamHandler(buf)
    h.setFormatter(JsonFormatter())
    logging.getLogger().addHandler(h)
    try:
        logging.getLogger("engram.test").info("meow", extra={"foo": 1})
        line = buf.getvalue().strip().splitlines()[-1]
        payload = json.loads(line)
        assert payload["foo"] == 1
        assert payload["message"] == "meow"
    finally:
        logging.getLogger().removeHandler(h)


def test_request_id_helpers():
    rid = new_request_id()
    assert rid.startswith("req-")
    set_request_id(rid)
    assert get_request_id() == rid
    set_request_id("")
