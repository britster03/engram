from google.protobuf.timestamp_pb2 import Timestamp
from temporalio.api.history.v1 import History,HistoryEvent

from benchmarks.lifecycle.activities import activity_rows


def history(attempt=1,complete=True):
    events=[HistoryEvent(event_id=1,event_time=Timestamp(seconds=1),activity_task_scheduled_event_attributes={"activity_id":"a","activity_type":{"name":"engram.canonical_extract"}}),HistoryEvent(event_id=2,event_time=Timestamp(seconds=3),activity_task_started_event_attributes={"scheduled_event_id":1,"attempt":attempt})]
    if complete:
        events.append(HistoryEvent(event_id=3,event_time=Timestamp(seconds=5),activity_task_completed_event_attributes={"scheduled_event_id":1,"started_event_id":2}))
    return History(events=events)


def test_native_history_separates_execution_from_queue_time():
    row=activity_rows(history(),"private")[0]
    assert row["schedule_to_terminal_ms"]==4000
    assert row["last_attempt_latency_ms"]==2000 and row["queue_latency_ms"]==2000
    assert row["retry_count"]==0 and row["attempt_timing_status"]=="EXACT"


def test_folded_retries_do_not_invent_first_attempt_duration():
    row=activity_rows(history(attempt=3),"private")[0]
    assert row["retry_count"]==2 and row["queue_latency_ms"] is None
    assert row["attempt_timing_status"]=="LAST_ATTEMPT_ONLY_SERVER_FOLDED_RETRIES"


def test_unfinished_activity_has_no_successful_timing():
    row=activity_rows(history(complete=False),"private")[0]
    assert row["status"]=="UNFINISHED" and row["last_attempt_latency_ms"] is None
    assert row["schedule_to_terminal_ms"] is None and row["attempt_timing_status"]=="INCOMPLETE"
