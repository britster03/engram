import csv
import json

from benchmarks.lifecycle.observer import Trace
from benchmarks.lifecycle.report import csv_write
from benchmarks.lifecycle.retrieval_report import question_retrieval


def test_question_retrieval_keeps_source_results_separate_from_exact_model_input(tmp_path):
    trace = Trace("canonical:q1")
    trace.stage = "L2"
    trace.emit("store", store="postgres_conversations", function="search_session_events",
               result=[{"event_id": "event-1", "score": 0.8,
                        "payload": {"turn_pair": {"user": {"content": "I moved in June", "timestamp": "2023-06-01"}}},
                        "api_key": "secret"}])
    trace.stage = "L3_VECTOR"
    trace.emit("store", store="neo4j", function="vector_search",
               result=[{"memory_id": "memory-1", "l0_abstract": "Moved in June", "score": 0.9}])
    trace.emit("store", store="postgres_canonical", function="get_current_state",
               result={"current_version": {"body": "Moved in June"},
                       "claims": [{"id": "claim-1", "predicate": "MOVED_IN", "object_value": "June"}]})
    trace.emit("model_context", phase="answerability", operation_id="first", text="candidate context")
    trace.emit("model_context", phase="generation", operation_id="last", text="final context\nwith a second line")
    trace.calls.extend([{"operation_id": "first", "model": "muse-spark-1.3-contributor", "status": "RETURNED"},
                        {"operation_id": "last", "model": "muse-spark-1.3-contributor", "status": "RETURNED"}])

    result = question_retrieval(trace, answer_mode="canonical-model")
    assert result["postgresql"][0]["items"][0]["payload"]["turn_pair"]["user"]["content"] == "I moved in June"
    assert result["postgresql"][1]["items"][0]["claims"][0]["object_value"] == "June"
    assert result["neo4j"][0]["items"][0]["memory_id"] == "memory-1"
    assert result["neo4j"][0]["items"][0]["score"] == 0.9
    assert "secret" not in json.dumps(result)
    assert [call["context"] for call in result["frontier_calls"]] == [
        "candidate context", "final context\nwith a second line"
    ]
    assert result["frontier_calls"][-1]["phase"] == "generation"
    csv_write(tmp_path / "result.csv", [{"Retrieved Data / Frontier Context": result}])
    with (tmp_path / "result.csv").open(newline="") as stream:
        stored = json.loads(next(csv.DictReader(stream))["Retrieved Data / Frontier Context"])
    assert stored == result


def test_question_retrieval_records_no_model_call_and_failed_store_read():
    trace = Trace("canonical:q2")
    trace.emit("store", store="neo4j", function="run_template",
               error={"type": "ServiceUnavailable"})
    result = question_retrieval(trace, answer_mode="evidence-terminal")
    assert result["frontier_calls"] == []
    assert result["postgresql"] == []
    assert result["neo4j"][0]["returned_count"] is None
    assert result["neo4j"][0]["error"]["type"] == "ServiceUnavailable"


def test_repeated_large_store_result_has_bounded_preview_and_raw_trace_digest():
    trace = Trace("canonical:q3")
    state = {"node": {"id": "memory-1"},
             "claims": [{"id": f"claim-{i}", "object_value": f"value-{i}"} for i in range(100)]}
    for _ in range(40):
        trace.emit("store", store="postgres_canonical", function="get_current_state", result=state)
    result = question_retrieval(trace)
    assert len(result["postgresql"]) == 1
    read = result["postgresql"][0]
    assert read["read_count"] == 40
    assert read["returned_count"] == 40
    assert read["distinct_result_count"] == 1
    assert read["items"][0]["claims"]["total_count"] == 100
    assert len(read["items"][0]["claims"]["preview"]) == 10
    assert len(read["sample_result_sha256"]) == 64
    assert len(json.dumps(result)) < 2000


def test_distinct_reads_of_same_operation_share_one_group():
    trace = Trace("canonical:q4")
    for memory_id in ("a", "b"):
        trace.emit("store", store="neo4j", function="vector_search",
                   result=[{"memory_id": memory_id, "score": 0.7}])
    group = question_retrieval(trace)["neo4j"][0]
    assert group["read_count"] == 2
    assert group["returned_count"] == 2
    assert group["distinct_result_count"] == 2
    assert [item["memory_id"] for item in group["items"]] == ["a", "b"]
