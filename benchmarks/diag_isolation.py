"""Diagnostic: does a minted tenant key actually scope ingest to its tenant?

Confirms or refutes the finding that LoCoMo data landed in `_default` instead
of its per-conversation tenant. Talks to the server over HTTP (so it is
working-directory independent for the API calls) and then reads PostgreSQL
to see how each ingest was tagged.

It ingests TWO pairs:
  * one with a freshly-minted tenant key  -> should be tagged <that tenant>
  * one with the legacy/default key        -> should be tagged _default

Set ENGRAM_DATABASE_URL to the same PostgreSQL database used by the server
before running this diagnostic.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import httpx
import psycopg

BASE = os.environ.get("ENGRAM_BASE_URL", "http://127.0.0.1:8000")


def load_env_keys() -> dict[str, str]:
    """Pull keys from environment, falling back to a nearby .env file."""
    keys = {
        k: os.environ[k]
        for k in (
            "ENGRAM_ADMIN_KEY",
            "ENGRAM_API_KEY",
            "ENGRAM_DATABASE_URL",
        )
        if k in os.environ
    }
    if all(
        name in keys
        for name in (
            "ENGRAM_ADMIN_KEY",
            "ENGRAM_API_KEY",
            "ENGRAM_DATABASE_URL",
        )
    ):
        return keys
    for candidate in (".env", "../.env", "../../.env"):
        p = Path(candidate)
        if not p.exists():
            continue
        for line in p.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                name, val = line.split("=", 1)
                keys.setdefault(name.strip(), val.strip().strip('"').strip("'"))
        break
    return keys


def ingest_one(key: str, source: str) -> int:
    r = httpx.post(
        f"{BASE}/api/v1/ingest",
        headers={"Authorization": f"Bearer {key}"},
        json={
            "session_id": "diag",
            "turn_pair": {
                "user": {"content": "diag user", "turn_idx": 0},
                "assistant": {"content": "diag asst", "turn_idx": 1},
            },
            "source": source,
        },
    )
    return r.status_code


def tenant_for_source(database_url: str, source: str) -> str | None:
    with psycopg.connect(database_url) as connection:
        row = connection.execute(
            "SELECT tenant_id FROM events WHERE source = %s ORDER BY created_at DESC LIMIT 1",
            (source,),
        ).fetchone()
    return row[0] if row else None


def main() -> int:
    keys = load_env_keys()
    admin = keys.get("ENGRAM_ADMIN_KEY")
    default_key = keys.get("ENGRAM_API_KEY")
    database_url = keys.get("ENGRAM_DATABASE_URL")
    if not admin:
        print("ERROR: ENGRAM_ADMIN_KEY not found in env or .env")
        return 2

    if not database_url:
        print("ERROR: ENGRAM_DATABASE_URL not found in env or .env")
        return 2
    print("using PostgreSQL control plane\n")

    # 1) mint a fresh tenant + key
    tid = "diag-" + time.strftime("%H%M%S")
    r = httpx.post(
        f"{BASE}/api/v1/admin/tenants",
        headers={"Authorization": f"Bearer {admin}"},
        json={"tenant_id": tid, "display_name": "diag"},
    )
    if r.status_code not in (200, 201):
        print(f"ERROR: create-tenant returned {r.status_code}: {r.text[:300]}")
        return 1
    minted = r.json()["api_key"]
    print(f"minted tenant: {tid}")
    print(f"minted key == default key? {minted == default_key}\n")

    # 2) ingest with minted key (unique source) and with the default key
    src_minted = f"diag-minted-{tid}"
    src_default = f"diag-default-{tid}"
    print("ingest with minted key ->", ingest_one(minted, src_minted))
    if default_key:
        print("ingest with default key ->", ingest_one(default_key, src_default))
    time.sleep(1.5)

    # 3) read back how each was tagged
    got_minted = tenant_for_source(database_url, src_minted)
    got_default = tenant_for_source(database_url, src_default) if default_key else "(skipped)"

    print("\n--- VERDICT ---")
    print(f"minted-key ingest : expected={tid:<14} got={got_minted}")
    print(f"default-key ingest: expected={'_default':<14} got={got_default}")
    print()
    if got_minted == tid:
        print("PASS: tenant isolation works on the server. If real runs still")
        print("      land in _default, the bug is in the harness key handling.")
    elif got_minted == "_default":
        print("FAIL: a valid minted key is being resolved to _default on the")
        print("      server side. Tenant context is not reaching ingest.")
    else:
        print(f"UNEXPECTED: got {got_minted!r} — investigate manually.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
