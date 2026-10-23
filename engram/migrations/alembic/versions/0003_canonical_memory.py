"""Create tenant-scoped PostgreSQL canonical memory state.

Revision 0002 owns the Temporal dispatch lease columns on
``workflow_dispatches``.  Canonical writes use that existing table as the
single projection handoff; this revision deliberately does not create a
second projection outbox.
"""

from __future__ import annotations

from alembic import op

from engram.storage.canonical_schema import schema_statements

revision = "0003_canonical_memory"
down_revision = "0002_temporal_dispatch_leases"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in schema_statements():
        op.execute(statement)

    canonical_tables = (
        "canonical_mutations",
        "memory_nodes",
        "memory_versions",
        "memory_claims",
        "memory_evidence",
        "entity_aliases",
        "memory_hierarchy",
        "memory_overviews",
        "memory_uri_aliases",
        "ingest_artifacts",
    )
    for table in canonical_tables:
        policy = f"{table}_tenant_isolation"
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(
            f"""DO $canonical$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_policy
                    WHERE polname = '{policy}'
                      AND polrelid = '{table}'::regclass
                ) THEN
                    EXECUTE 'CREATE POLICY {policy} ON {table}
                        USING (tenant_id = current_setting(''app.tenant_id'', true))
                        WITH CHECK (tenant_id = current_setting(''app.tenant_id'', true))';
                END IF;
            END
            $canonical$;"""
        )

    # Install least-privilege grants only when the deployment has created the
    # corresponding roles.  Local/test databases normally have neither role.
    op.execute(
        """DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_app') THEN
                GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE
                    canonical_mutations, memory_nodes, memory_versions,
                    memory_claims, memory_evidence, entity_aliases,
                    memory_hierarchy, memory_overviews, memory_uri_aliases,
                    ingest_artifacts TO engram_app;
            END IF;
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_dispatcher') THEN
                GRANT SELECT, INSERT, UPDATE ON TABLE workflow_dispatches
                    TO engram_dispatcher;
            END IF;
        END
        $$;"""
    )


def downgrade() -> None:
    raise RuntimeError("Canonical memory migrations are forward-only")
