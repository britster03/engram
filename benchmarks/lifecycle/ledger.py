"""Durable append-only records and atomic monitoring state."""
from __future__ import annotations

import dataclasses
import hashlib
import gzip
import lzma
import json
import os
import threading
import time
import uuid
from datetime import date, datetime
from pathlib import Path

_LOCK = threading.RLock()
PRIVATE_KEYS = {"api_key", "admin_key", "authorization", "password", "reader_password", "writer_password", "dsn", "headers", "extra_headers", "api_key_hashes", "key_hash"}


def safe(value):
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    if dataclasses.is_dataclass(value):
        value = dataclasses.asdict(value)
    if isinstance(value, dict):
        return {str(k): safe(v) for k, v in value.items() if str(k).lower() not in PRIVATE_KEYS and not str(k).lower().endswith("_api_key")}
    if isinstance(value, (list, tuple, set)):
        return [safe(v) for v in value]
    if isinstance(value, (datetime, date, uuid.UUID, Path)):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return f"<{type(value).__name__}>"


def digest(value):
    return hashlib.sha256(json.dumps(safe(value), sort_keys=True, ensure_ascii=False, default=str).encode()).hexdigest()


def atomic(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{os.getpid()}.{uuid.uuid4().hex}.tmp")
    with temporary.open("w") as stream:
        json.dump(safe(value), stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def append(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(safe(value), ensure_ascii=False, allow_nan=False) + "\n"
    with _LOCK, path.open("a") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def append_trace(directory, trace):
    """Losslessly archive large trace details; keep an append-only index."""
    directory=Path(directory)
    value=safe(trace.to_dict())
    encoded=json.dumps(value,ensure_ascii=False,allow_nan=False).encode()
    identity=hashlib.sha256(encoded).hexdigest()
    relative=Path("trace_payloads")/(identity+".json.xz")
    path=directory/relative
    path.parent.mkdir(parents=True,exist_ok=True)
    if not path.exists():
        temporary=path.with_name(path.name+"."+uuid.uuid4().hex+".tmp")
        with temporary.open("wb") as stream:
            with lzma.LZMAFile(stream,mode="wb",filters=[{"id":lzma.FILTER_LZMA2,"preset":1,"dict_size":8*1024*1024}]) as compressed:
                compressed.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    index={**{k:v for k,v in value.items() if k!="events"},"events":[e for e in value["events"] if e["kind"] in {"embedding","embedding_cache"}],"event_count":len(value["events"]),"trace_archive":str(relative),"trace_archive_sha256":identity,"trace_archive_uncompressed_bytes":len(encoded),"trace_archive_bytes":path.stat().st_size,"trace_storage":"LOSSLESS_XZ_LINKED"}
    append(directory/"trace.jsonl",index)
    return index


def expand_trace(directory, index):
    """Read legacy inline traces or verify and expand an archived trace."""
    if not index.get("trace_archive"):
        return index
    root=Path(directory).resolve()
    path=(root/index["trace_archive"]).resolve()
    if root not in path.parents:
        raise ValueError("Trace archive is outside the run directory")
    opener=lzma.open if path.suffix==".xz" else gzip.open
    with opener(path,"rb") as stream:
        encoded=stream.read()
    if hashlib.sha256(encoded).hexdigest()!=index["trace_archive_sha256"]:
        raise ValueError("Trace archive checksum mismatch")
    value=json.loads(encoded)
    if value["trace_id"]!=index["trace_id"]:
        raise ValueError("Trace archive identity mismatch")
    return value


def iter_read(path,*,live=False):
    path = Path(path)
    if not path.exists():
        return
    with path.open() as stream:
        for line in stream:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                if live and not line.endswith("\n") and not stream.read(1):
                    return
                raise ValueError(f"Interrupted or corrupt JSONL record requires recovery: {path}")


def read(path):
    return list(iter_read(path))


def usage_record(row):
    result={k:v for k,v in row.items() if k not in {"request","response_text","raw_usage"}}
    raw=row.get("raw_usage") or {}
    for direction,legacy in [("input","prompt"),("output","completion")]:
        value=raw.get(direction+"_tokens",raw.get(legacy+"_tokens"))
        if value is not None:
            result.setdefault("provider_"+direction+"_tokens",value)
    details=raw.get("input_tokens_details",raw.get("prompt_tokens_details")) or {}
    if "cache_write_tokens" in details:
        result.setdefault("cache_write_input_tokens",details["cache_write_tokens"])
    return result


def known_provider_tokens(row):
    return sum(known_provider_count(row,direction) for direction in ["input","output"])


def known_provider_count(row,direction):
    key=direction+"_tokens"
    return (row.get("provider_"+key) if "provider_"+key in row else row.get(key) if row.get("token_count_source")=="provider" else 0) or 0


def recover_offline(directory):
    """Preserve torn tails and terminally label attempts after writers stop."""
    directory=Path(directory)
    recovered=[]
    for path in sorted(directory.glob("*.jsonl")):
        with path.open("r+b") as stream:
            stream.seek(0,2)
            size=stream.tell()
            if not size:
                continue
            stream.seek(size-1)
            if stream.read(1)==b"\n":
                continue
            position=size
            tail=b""
            while position>0 and b"\n" not in tail:
                previous=max(0,position-65536)
                stream.seek(previous)
                tail=stream.read(position-previous)+tail
                position=previous
            offset=position+tail.rfind(b"\n")+1
            tail=tail[tail.rfind(b"\n")+1:]
            try:
                json.loads(tail)
            except (json.JSONDecodeError,UnicodeDecodeError):
                suffix=hashlib.sha256(tail).hexdigest()
                preserved=path.with_name(path.name+".interrupted-"+suffix+".tail")
                preserved.write_bytes(tail)
                stream.truncate(offset)
                recovered.append({"file":path.name,"offset":offset,"preserved_tail":preserved.name,"sha256":suffix})
            else:
                stream.seek(0,2)
                stream.write(b"\n")
            stream.flush()
            os.fsync(stream.fileno())
    starts,finished={},set()
    for path in directory.glob("provider_attempts.*.jsonl"):
        for row in iter_read(path):
            if row["state"]=="STARTED":
                starts[row["attempt_id"]]=(path,usage_record(row))
            else:
                finished.add(row["attempt_id"])
    for identity in set(starts)-finished:
        path,row=starts[identity]
        append(path,{**row,"state":"FINISHED","status":"INTERRUPTED","input_tokens":None,"output_tokens":None,"reasoning_tokens":None,"cached_input_tokens":None,"token_count_source":"unavailable","latency_ms":None,"timing_status":"UNKNOWN_INTERRUPTED","recovered_after_writers_stopped":True})
        recovered.append({"attempt_id":identity,"status":"INTERRUPTED","usage":"UNKNOWN"})
    local_path=directory/"local_scoring_attempts.jsonl"
    local_starts,local_finished={},set()
    for row in iter_read(local_path):
        if row["state"]=="STARTED":
            local_starts[row["attempt_id"]]=row
        else:
            local_finished.add(row["attempt_id"])
    for identity in set(local_starts)-local_finished:
        append(local_path,{**local_starts[identity],"state":"FINISHED","status":"INTERRUPTED","latency_ms":None,"timing_status":"UNKNOWN_INTERRUPTED","recovered_after_writers_stopped":True})
        recovered.append({"attempt_id":identity,"status":"INTERRUPTED","component":"local_scoring","latency":"UNKNOWN"})
    for record in recovered:
        append(directory/"journal_recovery.jsonl",record)
    return recovered


class Progress:
    def __init__(self, directory, **initial):
        self.directory = Path(directory)
        previous=self.directory/"progress.json"
        self.state=json.loads(previous.read_text()) if previous.exists() else {}
        self.elapsed_offset=self.state.get("elapsed_seconds",0)
        self.state.update(status="RUNNING",**initial)
        self.state.setdefault("completed",0)
        self.state.setdefault("errors",0)
        self.state.setdefault("known_tokens",0)
        self.started = time.monotonic()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._heartbeat, daemon=True)
        self.update()
        self._thread.start()

    def update(self, **updates):
        with _LOCK:
            self.state.update(updates)
            self.state["elapsed_seconds"] = round(self.elapsed_offset+time.monotonic()-self.started, 2)
            self.state["process_elapsed_seconds"]=round(time.monotonic()-self.started,2)
            if updates.get("phase")=="ingestion":
                self.state["accepted_ingestion_turns"]=updates.get("completed",self.state.get("accepted_ingestion_turns",0))
                self.state["current_question"]=None
            if updates.get("phase")=="evaluation":
                self.state["completed_evaluations"]=updates.get("completed",self.state.get("completed_evaluations",0))
            self.state["updated_at"] = datetime.now().astimezone().isoformat()
            atomic(self.directory / "progress.json", self.state)

    def refresh_usage(self):
        starts, finished = set(), {}
        for path in self.directory.glob("provider_attempts.*.jsonl"):
            for row in iter_read(path,live=True):
                if row["state"] == "STARTED":
                    starts.add(row["attempt_id"])
                else:
                    finished[row["attempt_id"]] = usage_record(row)
        self.update(known_lifecycle_tokens=sum(known_provider_tokens(r) for r in finished.values()), unknown_usage_calls=sum(r.get("input_tokens") is None or r.get("output_tokens") is None for r in finished.values()),estimated_usage_calls=sum(r.get("token_count_source")=="tokenizer_estimate" for r in finished.values()), in_flight_calls=len(starts-set(finished)))

    def _heartbeat(self):
        while not self._stop.wait(30):
            self.refresh_usage()
            public = {k: self.state.get(k) for k in ["phase", "conversation", "checkpoint", "completed", "expected", "errors", "known_tokens", "known_lifecycle_tokens", "unknown_usage_calls", "in_flight_calls", "elapsed_seconds"]}
            print("BENCHMARK " + json.dumps(public), flush=True)

    def close(self, status):
        self._stop.set()
        self._thread.join(timeout=1)
        # Capture attempts completed since the last 30-second heartbeat.
        # A terminal report must not show stale in-flight work or token totals.
        self.refresh_usage()
        terminal=self.state.get("status")
        self.update(status=terminal if terminal in {"COMPLETE","COMPLETE_WITH_ERRORS","INCOMPLETE"} else status)
