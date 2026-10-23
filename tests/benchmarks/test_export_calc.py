import csv
import json

import pytest

from benchmarks.lifecycle.export_calc import COLUMN, MAX_CELL_CHARS, export


def test_export_keeps_full_context_in_details_and_bounded_csv(tmp_path):
    source = tmp_path / "source.csv"
    destination = tmp_path / "calc.csv"
    details = tmp_path / "details.json"
    context = "model input " * 5000
    data = {
        "postgresql": [{
            "stage": "L2", "store": "postgres_conversations",
            "operation": "search_session_events", "read_count": 1,
            "returned_count": 1, "distinct_result_count": 1,
            "items": [{"content": "database text " * 5000}],
        }],
        "neo4j": [], "redis": [],
        "frontier_calls": [{"phase": "generation", "model": "frontier",
                            "submitted": True, "attempt_statuses": ["RETURNED"],
                            "context": context}],
        "trace_id": "question-1", "trace_archive": "trace.xz",
    }
    with source.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["Conversation", "Question", COLUMN])
        writer.writeheader()
        writer.writerow({"Conversation": "conv-1", "Question": "When?", COLUMN: json.dumps(data)})

    assert export(source, destination, details) == 1
    with destination.open(newline="") as stream:
        row = next(csv.DictReader(stream))
    assert len(row[COLUMN]) <= MAX_CELL_CHARS
    compact = json.loads(row[COLUMN])
    assert compact["postgresql"][0]["operation"] == "search_session_events"
    assert compact["frontier_calls"][0]["context_chars"] == len(context)
    assert compact["full_details_file"] == details.name
    assert json.loads(details.read_text())[0]["retrieval_context"]["frontier_calls"][0]["context"] == context
    with pytest.raises(FileExistsError):
        export(source, destination, details)
