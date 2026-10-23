"""Accept user messages before generation and track independent response runs."""

from alembic import op

revision = "0007_durable_chat"
down_revision = "0006_overview_lineage"
branch_labels = None
depends_on = None

SCHEMA = "\nCREATE TABLE IF NOT EXISTS chat_runs (\n    tenant_id TEXT NOT NULL, run_id TEXT NOT NULL, session_id TEXT NOT NULL,\n    client_message_id TEXT NOT NULL, input_event_id TEXT NOT NULL REFERENCES events(event_id),\n    input_hash TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'RUNNING'\n      CHECK (status IN ('RUNNING','COMPLETED','FAILED','CANCELLED')),\n    response JSONB, error_code TEXT,\n    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,\n    deadline TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP + INTERVAL '5 minutes',\n    completed_at TIMESTAMPTZ,\n    PRIMARY KEY (tenant_id, run_id), UNIQUE (tenant_id, client_message_id),\n    FOREIGN KEY (tenant_id, session_id) REFERENCES session_states(tenant_id, session_id) ON DELETE CASCADE\n);\nCREATE UNIQUE INDEX IF NOT EXISTS idx_chat_runs_active_session\n    ON chat_runs(tenant_id, session_id) WHERE status = 'RUNNING';\nCREATE INDEX IF NOT EXISTS idx_chat_runs_deadline ON chat_runs(deadline) WHERE status = 'RUNNING';\n"


def upgrade() -> None:
    for statement in SCHEMA.split(";"):
        if statement.strip():
            op.execute(statement)
    op.execute(
        """DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_app') THEN GRANT SELECT, INSERT, UPDATE, DELETE ON chat_runs TO engram_app; END IF; END $$;"""
    )


def downgrade() -> None:
    raise RuntimeError("Durable chat migrations are forward-only")
