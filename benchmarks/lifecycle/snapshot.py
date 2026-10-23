"""Tenant snapshots in the private benchmark stack; never production backups."""
from __future__ import annotations

import gzip
import base64
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .infrastructure import assert_isolated
from .ledger import atomic

EXCLUDED = {"tenants", "api_keys", "audit_log"}


def encode(value):
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"__binary_base64__":base64.b64encode(bytes(value)).decode()}
    if hasattr(value, "iso_format"):
        return value.iso_format()
    return str(value)


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=encode, allow_nan=False)


def checksum(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def tables(conn):
    rows = conn.execute("SELECT table_name,column_name,data_type,is_generated FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name,ordinal_position").fetchall()
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["table_name"]].append(row)
    return {name: columns for name, columns in grouped.items() if name not in EXCLUDED and any(c["column_name"] == "tenant_id" for c in columns)}


def collect(state, tenant_id):
    cfg = assert_isolated()
    with psycopg.connect(cfg.event_ledger.dsn, row_factory=dict_row) as conn:
        with conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            schema = tables(conn)
            pg={}
            for name,columns in schema.items():
                json_columns=[c["column_name"] for c in columns if c["data_type"] in {"json","jsonb"}]
                pairs=[]
                for column in json_columns:
                    pairs.extend([sql.Literal(column),sql.SQL("{} IS NULL").format(sql.Identifier(column))])
                nulls=sql.SQL("jsonb_build_object({}) AS __sql_nulls__").format(sql.SQL(",").join(pairs)) if pairs else sql.SQL("'{}'::jsonb AS __sql_nulls__")
                query=sql.SQL("SELECT *,{} FROM {} WHERE tenant_id=%s").format(nulls,sql.Identifier(name))
                pg[name]=sorted(conn.execute(query,(tenant_id,)).fetchall(),key=canonical)
    with state.neo4j.reader().session() as session:
        nodes = [dict(r) for r in session.run("MATCH (n {tenant_id:$tenant}) RETURN labels(n) AS labels, properties(n) AS properties ORDER BY n.memory_id", tenant=tenant_id)]
        edges = [dict(r) for r in session.run("MATCH (a {tenant_id:$tenant})-[r]->(b {tenant_id:$tenant}) RETURN a.memory_id AS start,b.memory_id AS end,type(r) AS type,properties(r) AS properties", tenant=tenant_id)]
    return json.loads(canonical({"tenant_id": tenant_id, "schema": schema, "postgres": pg, "neo4j": {"nodes": sorted(nodes, key=canonical), "edges": sorted(edges, key=canonical)}}))


def fingerprint(state, tenant_id):
    data = collect(state, tenant_id)
    return {"postgres": checksum(data["postgres"]), "neo4j": checksum(data["neo4j"]), "schema": checksum(data["schema"])}


def save(state, tenant_id, path, **metadata):
    data = collect(state, tenant_id)
    data["metadata"] = metadata
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".tmp")
    with gzip.open(temp, "wt") as stream:
        stream.write(canonical(data))
    temp.replace(path)
    atomic(path.with_suffix(".manifest.json"), {**metadata, "tenant_id": tenant_id, "file_sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "fingerprints": {k: checksum(data[k]) for k in ["postgres", "neo4j", "schema"]}, "counts": {name: len(rows) for name, rows in data["postgres"].items()}})
    return data


def restore(state, path):
    cfg = assert_isolated()
    path = Path(path)
    manifest = json.loads(path.with_suffix(".manifest.json").read_text())
    if hashlib.sha256(path.read_bytes()).hexdigest() != manifest["file_sha256"]:
        raise RuntimeError("Checkpoint file checksum mismatch")
    with gzip.open(path, "rt") as stream:
        data = json.load(stream)
    tid = data["tenant_id"]
    if not tid.startswith("locomo-"):
        raise RuntimeError("Unsafe checkpoint tenant")
    with psycopg.connect(cfg.event_ledger.dsn, row_factory=dict_row) as conn:
        schema = tables(conn)
        if checksum(schema) != checksum(data["schema"]):
            raise RuntimeError("Checkpoint schema mismatch")
        with conn.transaction():
            # Isolated bench superuser only, transaction-local; no schema changes.
            conn.execute("SET LOCAL session_replication_role=replica")
            for name in schema:
                conn.execute(sql.SQL("DELETE FROM {} WHERE tenant_id=%s").format(sql.Identifier(name)), (tid,))
            for name, columns in schema.items():
                writable = [c for c in columns if c["is_generated"] == "NEVER"]
                names = [c["column_name"] for c in writable]
                statement = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(sql.Identifier(name), sql.SQL(",").join(map(sql.Identifier, names)), sql.SQL(",").join(sql.Placeholder() for _ in names))
                def value(row, column):
                    raw = row[column["column_name"]]
                    if column["data_type"] == "bytea" and isinstance(raw,dict):
                        return base64.b64decode(raw["__binary_base64__"],validate=True)
                    if column["data_type"] in {"json","jsonb"}:
                        if "__sql_nulls__" in row:
                            return None if row["__sql_nulls__"].get(column["column_name"]) else Jsonb(raw)
                        # Legacy checkpoints from this harness predate explicit
                        # null flags. These native insert contracts are known:
                        # nullable source spans use SQL NULL, and entity claims
                        # require SQL NULL for their absent scalar object.
                        if raw is None and (column["column_name"]=="source_span" or (name=="memory_claims" and column["column_name"]=="object_value" and row.get("object_entity_id"))):
                            return None
                        return Jsonb(raw)
                    return raw
                rows = [[value(row,c) for c in writable] for row in data["postgres"][name]]
                if rows:
                    with conn.cursor() as cursor:
                        cursor.executemany(statement, rows)
    with state.neo4j.writer().session() as session:
        session.run("MATCH (n {tenant_id:$tenant}) DETACH DELETE n", tenant=tid).consume()
        groups = defaultdict(list)
        for node in data["neo4j"]["nodes"]:
            if not node["properties"].get("memory_id") or node["properties"].get("tenant_id") != tid:
                raise RuntimeError("Invalid graph checkpoint identity")
            groups[tuple(sorted(node["labels"]))].append(node["properties"])
        for labels, nodes in groups.items():
            if any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", label) for label in labels):
                raise RuntimeError("Invalid graph label")
            session.run("UNWIND $rows AS props CREATE (n:" + ":".join(labels) + ") SET n=props", rows=nodes).consume()
        groups.clear()
        for edge in data["neo4j"]["edges"]:
            groups[edge["type"]].append(edge)
        for kind, edges in groups.items():
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", kind):
                raise RuntimeError("Invalid graph relationship type")
            session.run("UNWIND $rows AS row MATCH (a {tenant_id:$tenant,memory_id:row.start}),(b {tenant_id:$tenant,memory_id:row.end}) CREATE (a)-[r:" + kind + "]->(b) SET r=row.properties", rows=edges, tenant=tid).consume()
        session.run("CALL db.awaitIndexes(300)").consume()
    import redis
    redis.Redis.from_url(cfg.session_cache.redis_url).flushdb()
    actual_data=collect(state,tid)
    legacy=any("__sql_nulls__" not in row for rows in data["postgres"].values() for row in rows)
    if legacy:
        for name,rows in actual_data["postgres"].items():
            for row in rows:
                row.pop("__sql_nulls__",None)
            actual_data["postgres"][name]=sorted(rows,key=canonical)
    actual={"postgres":checksum(actual_data["postgres"]),"neo4j":checksum(actual_data["neo4j"]),"schema":checksum(actual_data["schema"])}
    if actual != manifest["fingerprints"]:
        raise RuntimeError("Restored checkpoint fingerprint mismatch")
    return data
