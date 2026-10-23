import json

from benchmarks.lifecycle.ledger import Progress, append


def test_close_reconciles_attempts_finished_between_heartbeats(tmp_path):
    progress = Progress(tmp_path, phase="evaluation")
    journal = tmp_path / "provider_attempts.test.jsonl"
    append(journal, {"attempt_id": "one", "state": "STARTED"})
    progress.refresh_usage()
    assert progress.state["in_flight_calls"] == 1
    append(journal, {"attempt_id": "one", "state": "FINISHED", "input_tokens": 12,
                     "output_tokens": 10, "token_count_source": "provider"})
    progress.close("COMPLETE")
    final = json.loads((tmp_path / "progress.json").read_text())
    assert final["status"] == "COMPLETE"
    assert final["known_lifecycle_tokens"] == 22
    assert final["in_flight_calls"] == final["unknown_usage_calls"] == 0
