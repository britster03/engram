"""Locust load-test harness.

Usage:
    pip install locust
    locust -f scripts/loadtest.py \
        --host https://engram.example.com \
        --headless --users 100 --spawn-rate 5 --run-time 10m \
        -T engram_api_key=sk-your-key

Scenarios:
  * 70% queries against a seeded corpus (cache-friendly)
  * 25% ingests (unique per-user turn idx, drives downstream pipeline)
  *  5% session-attached messages (exercises /sessions/*/message)

Collects latency by endpoint and reports p50/p95/p99 at the end. Stops the
run early if the error rate exceeds 2% for 60 consecutive seconds.
"""

from __future__ import annotations

import os
import random
import string
import time
import uuid

try:
    from locust import HttpUser, LoadTestShape, between, events, task
    from locust.env import Environment
except ImportError as err:  # pragma: no cover
    raise SystemExit(
        "locust is not installed. `pip install locust` and re-run."
    ) from err


_QUERY_CORPUS = [
    "Where does the user work?",
    "What is the user's wife's birthday?",
    "Where did the user move to recently?",
    "What project is the user working on?",
    "Who is the user's manager?",
    "What programming languages does the user use?",
    "What is the user's home city?",
    "Does the user have pets?",
    "What are the user's upcoming travel plans?",
    "What meeting is the user prepping for?",
]

_INGEST_TEMPLATES = [
    ("I just accepted a job at {}.", "Congratulations!"),
    ("I moved to {} last week.", "Noted."),
    ("My manager's name is {}.", "Got it."),
    ("I'm working on {} this quarter.", "Sounds interesting."),
    ("I prefer {} over the alternative.", "Noted."),
    ("My wife's birthday is {}.", "I'll remember that."),
    ("I have a pet named {}.", "Cute."),
]


def _random_word(n: int = 6) -> str:
    return "".join(random.choices(string.ascii_letters, k=n))


class EngramUser(HttpUser):
    wait_time = between(0.5, 2.0)
    abstract = False

    def on_start(self) -> None:
        self.api_key = self.environment.parsed_options.api_key or os.environ.get("ENGRAM_API_KEY")
        if not self.api_key:
            raise RuntimeError(
                "No API key. Pass --tag-include engram_api_key=... or set ENGRAM_API_KEY."
            )
        self.session_id = f"load-{uuid.uuid4().hex[:10]}"
        self.turn_idx = 0
        self.client.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        })
        # Seed with a few ingests so queries have context to find.
        for _ in range(3):
            self._ingest_once()

    @task(14)
    def query(self) -> None:
        q = random.choice(_QUERY_CORPUS)
        with self.client.post(
            "/api/v1/query",
            json={"session_id": self.session_id, "query": q},
            catch_response=True,
            name="/api/v1/query",
        ) as r:
            if r.status_code == 429:
                r.success()  # rate limiting is expected under load
            elif r.status_code >= 500:
                r.failure(f"server error {r.status_code}")
            else:
                r.success()

    @task(5)
    def ingest(self) -> None:
        self._ingest_once()

    @task(1)
    def session_message(self) -> None:
        tpl_user, tpl_asst = random.choice(_INGEST_TEMPLATES)
        user_text = tpl_user.format(_random_word())
        with self.client.post(
            f"/api/v1/sessions/{self.session_id}/message",
            json={"user": user_text, "assistant": tpl_asst},
            catch_response=True,
            name="/api/v1/sessions/{id}/message",
        ) as r:
            if r.status_code in {202, 200}:
                r.success()
            elif r.status_code == 429:
                r.success()
            else:
                r.failure(f"unexpected {r.status_code}: {r.text[:120]}")

    def _ingest_once(self) -> None:
        tpl_user, tpl_asst = random.choice(_INGEST_TEMPLATES)
        user_text = tpl_user.format(_random_word())
        user_idx = self.turn_idx
        asst_idx = user_idx + 1
        self.turn_idx += 2
        body = {
            "session_id": self.session_id,
            "turn_pair": {
                "user":      {"content": user_text, "turn_idx": user_idx},
                "assistant": {"content": tpl_asst,  "turn_idx": asst_idx},
            },
        }
        with self.client.post(
            "/api/v1/ingest", json=body, catch_response=True, name="/api/v1/ingest",
        ) as r:
            if r.status_code in {202, 200, 429}:
                r.success()
            elif r.status_code == 503:
                r.success()  # backpressure is a feature, not a failure
            else:
                r.failure(f"unexpected {r.status_code}")


# ----------------------------------------------------------------------
# Custom load shape: warm-up → steady-state → spike → cool-down.
# ----------------------------------------------------------------------

class EngramStages(LoadTestShape):
    stages = [
        {"duration":  60, "users":  10, "spawn_rate": 2},    # warm-up
        {"duration": 300, "users":  50, "spawn_rate": 5},    # steady
        {"duration": 120, "users": 200, "spawn_rate": 20},   # spike
        {"duration": 300, "users":  50, "spawn_rate": 10},   # steady
        {"duration":  60, "users":   5, "spawn_rate": 1},    # cool-down
    ]

    def tick(self):
        run_time = self.get_run_time()
        elapsed = 0
        for stage in self.stages:
            elapsed += stage["duration"]
            if run_time < elapsed:
                return stage["users"], stage["spawn_rate"]
        return None


# ----------------------------------------------------------------------
# Stop the run early on sustained errors.
# ----------------------------------------------------------------------

_ERROR_WINDOW_SECS = 60
_ERROR_THRESHOLD = 0.02
_errors_in_window: list[tuple[float, bool]] = []


@events.request.add_listener
def _on_request(request_type, name, response_time, response_length, exception,
                context, **kwargs):  # noqa: ANN001
    now = time.time()
    _errors_in_window.append((now, exception is not None))
    while _errors_in_window and now - _errors_in_window[0][0] > _ERROR_WINDOW_SECS:
        _errors_in_window.pop(0)
    if len(_errors_in_window) >= 100:
        errs = sum(1 for _, e in _errors_in_window if e)
        if errs / len(_errors_in_window) > _ERROR_THRESHOLD:
            print(f"[!] error rate {errs}/{len(_errors_in_window)} over {_ERROR_WINDOW_SECS}s; stopping.")
            Environment().runner.quit()


@events.init_command_line_parser.add_listener
def _cli_parser(parser):
    parser.add_argument("--api-key", default=None,
                        help="Bearer token for the Engram API")
