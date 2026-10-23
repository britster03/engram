"""Source generations and policy-aware ingestion recovery."""

from alembic import op

from engram.storage.ingestion_policy_schema import INGESTION_POLICY_SCHEMA

revision = "0009_ingestion_policy"
down_revision = "0008_overview_policy"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in INGESTION_POLICY_SCHEMA:
        op.execute(statement)
    op.execute("ALTER TABLE ingest_reprocessings ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY ingest_reprocessings_tenant_isolation ON ingest_reprocessings "
        "USING (tenant_id = current_setting('app.tenant_id', true)) "
        "WITH CHECK (tenant_id = current_setting('app.tenant_id', true))"
    )
    op.execute("""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_app') THEN
            GRANT SELECT, INSERT, UPDATE ON ingest_reprocessings TO engram_app;
        END IF;
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_dispatcher') THEN
            GRANT SELECT, UPDATE ON ingest_reprocessings, events, consolidation_tasks TO engram_dispatcher;
        END IF;
    END $$""")


def downgrade() -> None:
    raise RuntimeError("Ingestion policy migrations are forward-only")
