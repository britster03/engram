"""Read-only Temporal history accounting for native ingestion activities."""
from __future__ import annotations

import asyncio
from collections import defaultdict
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from .ledger import append, read


def milliseconds(timestamp):
    return timestamp.seconds*1000+timestamp.nanos/1_000_000


def activity_rows(history, workflow_id):
    scheduled,started,terminal={},{},{}
    outcomes={"completed":"COMPLETED","failed":"FAILED","timed_out":"TIMED_OUT","canceled":"CANCELED"}
    for event in history.events:
        if event.HasField("activity_task_scheduled_event_attributes"):
            attributes=event.activity_task_scheduled_event_attributes
            scheduled[event.event_id]={"activity_id":attributes.activity_id,"activity_type":attributes.activity_type.name,"scheduled_ms":milliseconds(event.event_time)}
        elif event.HasField("activity_task_started_event_attributes"):
            attributes=event.activity_task_started_event_attributes
            started[attributes.scheduled_event_id]={"started_ms":milliseconds(event.event_time),"attempt":attributes.attempt}
        else:
            for field,status in outcomes.items():
                name=f"activity_task_{field}_event_attributes"
                if event.HasField(name):
                    attributes=getattr(event,name)
                    terminal[attributes.scheduled_event_id]={"finished_ms":milliseconds(event.event_time),"status":status}
                    break
    rows=[]
    for identity,schedule in scheduled.items():
        start=started.get(identity,{})
        end=terminal.get(identity,{})
        attempt=start.get("attempt")
        rows.append({"activity_trace_id":f"{workflow_id}:{identity}","workflow_id":workflow_id,**schedule,**start,**end,"status":end.get("status","UNFINISHED"),"schedule_to_terminal_ms":end["finished_ms"]-schedule["scheduled_ms"] if end else None,"last_attempt_latency_ms":end["finished_ms"]-start["started_ms"] if end and start else None,"queue_latency_ms":start["started_ms"]-schedule["scheduled_ms"] if start and attempt==1 else None,"retry_count":max(0,attempt-1) if attempt is not None else None,"attempt_timing_status":"LAST_ATTEMPT_ONLY_SERVER_FOLDED_RETRIES" if attempt and attempt>1 else "EXACT" if start and end else "INCOMPLETE","timing_source":"TEMPORAL_HISTORY"})
    return rows


async def fetch(config, dispatches):
    from temporalio.client import Client
    client=await asyncio.wait_for(Client.connect(config.address,namespace=config.namespace),30)
    slots=asyncio.Semaphore(4)
    async def one(dispatch):
        async with slots:
            handle=client.get_workflow_handle(dispatch["workflow_id"])
            description=await asyncio.wait_for(handle.describe(),30)
            history=await asyncio.wait_for(handle.fetch_history(),30)
            rows=activity_rows(history,dispatch["workflow_id"])
            return {**dispatch,"workflow_run_id":description.run_id,"workflow_status":description.status.name,"activities":rows}
    return await asyncio.gather(*(one(d) for d in dispatches))


def effective_workflow_ids(dispatches):
    """Latest non-projection generation; every projection remains required."""
    newest = {}
    required = set()
    for row in dispatches:
        if row["workflow_type"] == "PROJECTION":
            required.add(row["workflow_id"])
        else:
            key = (row.get("tenant_id"), row["workflow_type"], row["aggregate_id"])
            if key not in newest or row.get("generation", 1) > newest[key].get("generation", 1):
                newest[key] = row
    return required | {row["workflow_id"] for row in newest.values()}


def collect(state, tenant_id, directory):
    """Cache completed workflow histories externally, without native writes."""
    path=Path(directory)/"temporal_history_trace.jsonl"
    cached={r["workflow_id"]:r for r in read(path)}
    with psycopg.connect(state.cfg.event_ledger.dsn,row_factory=dict_row) as conn:
        dispatches=conn.execute("SELECT tenant_id,workflow_id,workflow_type,aggregate_id,generation FROM workflow_dispatches WHERE tenant_id=%s AND workflow_id IS NOT NULL ORDER BY workflow_id",(tenant_id,)).fetchall()
    terminal = {"COMPLETED", "FAILED", "TERMINATED", "CANCELED", "TIMED_OUT"}
    pending=[d for d in dispatches if cached.get(d["workflow_id"],{}).get("workflow_status") not in terminal]
    for record in asyncio.run(fetch(state.cfg.temporal,pending)) if pending else []:
        if record["workflow_status"] in terminal:
            append(path,record)
        cached[record["workflow_id"]]=record
    statuses={d["workflow_id"]:cached[d["workflow_id"]]["workflow_status"] for d in dispatches}
    counts=defaultdict(int)
    for status in statuses.values():
        counts[status]+=1
    required = effective_workflow_ids(dispatches)
    effective = {identity: status for identity, status in statuses.items() if identity in required}
    return {"workflow_statuses":dict(counts),
            "superseded_workflow_ids":sorted(set(statuses)-required),
            "ready":all(s=="COMPLETED" for s in effective.values()),
            "fatal":any(s in {"FAILED","TERMINATED","CANCELED","TIMED_OUT"} for s in effective.values()),
            "unfinished_workflows":[identity for identity,status in effective.items() if status!="COMPLETED"]}
