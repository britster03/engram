"""Shared immutable PostgreSQL index definitions for conversation retrieval."""

SESSION_STATE_SQL = """
CREATE TABLE IF NOT EXISTS session_states (
    tenant_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, session_id)
)
"""

SESSION_SEARCH_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_events_session_history "
    "ON events(tenant_id, session_id, created_at DESC, event_id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_events_conversation_text ON events USING GIN "
    "(to_tsvector('simple', "
    "COALESCE(payload::jsonb #>> '{turn_pair,user,content}', '') || ' ' || "
    "COALESCE(payload::jsonb #>> '{turn_pair,assistant,content}', ''))) "
    "WHERE event_type IN ('INGEST', 'QUERY')",
    "CREATE INDEX IF NOT EXISTS idx_memory_versions_body_search "
    "ON memory_versions USING GIN (to_tsvector('simple', body))",
)
