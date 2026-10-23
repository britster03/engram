"""Durable, tenant-scoped source generations and reprocessing requests."""

INGESTION_POLICY_SCHEMA = (
    """CREATE TABLE IF NOT EXISTS ingest_reprocessings (
        tenant_id TEXT NOT NULL, root_event_id TEXT NOT NULL, event_id TEXT NOT NULL,
        previous_event_id TEXT, request_key TEXT, policy TEXT NOT NULL, reason TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('PENDING', 'ACTIVE', 'SUPERSEDED', 'FAILED')),
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (tenant_id, event_id),
        UNIQUE (tenant_id, root_event_id, request_key),
        FOREIGN KEY (tenant_id, root_event_id) REFERENCES events(tenant_id, event_id),
        FOREIGN KEY (tenant_id, event_id) REFERENCES events(tenant_id, event_id),
        FOREIGN KEY (tenant_id, previous_event_id) REFERENCES events(tenant_id, event_id)
    )""",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_ingest_generation_active ON ingest_reprocessings(tenant_id, root_event_id) WHERE state = 'ACTIVE'",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_ingest_generation_pending ON ingest_reprocessings(tenant_id, root_event_id) WHERE state = 'PENDING'",
    "CREATE INDEX IF NOT EXISTS idx_ingest_generation_root ON ingest_reprocessings(tenant_id, root_event_id, created_at)",
    "DROP INDEX IF EXISTS idx_memory_claims_logical_fingerprint",
    """CREATE UNIQUE INDEX idx_memory_claims_logical_fingerprint
        ON memory_claims(tenant_id, subject_id, predicate, normalized_object_hash,
            COALESCE(valid_from, '-infinity'::timestamptz), COALESCE(valid_until, 'infinity'::timestamptz))
        WHERE status NOT IN ('RETRACTED', 'SUPERSEDED', 'HISTORICAL')""",
    "CREATE INDEX IF NOT EXISTS idx_dispatch_started_reconciliation ON workflow_dispatches(updated_at, dispatch_id) WHERE status = 'STARTED'",
)
