"""Invalidate derived overviews transactionally and retain input lineage."""

from alembic import op

revision = "0006_overview_lineage"
down_revision = "0005_ordered_retrieval"
branch_labels = None
depends_on = None

STATEMENTS = (
    "ALTER TABLE consolidation_tasks ADD COLUMN IF NOT EXISTS requested_generation BIGINT NOT NULL DEFAULT 1",
    "ALTER TABLE consolidation_tasks ADD COLUMN IF NOT EXISTS processing_generation BIGINT",
    "CREATE TABLE IF NOT EXISTS memory_overview_dependencies (\n        tenant_id TEXT NOT NULL, overview_id UUID NOT NULL, memory_id UUID NOT NULL,\n        input_revision BIGINT NOT NULL, version_id UUID,\n        PRIMARY KEY (tenant_id, overview_id, memory_id),\n        FOREIGN KEY (tenant_id, overview_id) REFERENCES memory_overviews(tenant_id, id) ON DELETE CASCADE,\n        FOREIGN KEY (tenant_id, memory_id) REFERENCES memory_nodes(tenant_id, id) ON DELETE CASCADE\n    )",
    "CREATE INDEX IF NOT EXISTS idx_overview_dependencies_source ON memory_overview_dependencies(tenant_id, memory_id)",
    "ALTER TABLE memory_overview_dependencies ENABLE ROW LEVEL SECURITY",
    "DROP POLICY IF EXISTS overview_dependency_tenant ON memory_overview_dependencies",
    "CREATE POLICY overview_dependency_tenant ON memory_overview_dependencies USING (tenant_id = current_setting('app.tenant_id', true)) WITH CHECK (tenant_id = current_setting('app.tenant_id', true))",
    "UPDATE memory_nodes n SET revision = revision + 1 WHERE EXISTS (\n        SELECT 1 FROM memory_overviews o WHERE o.tenant_id = n.tenant_id AND o.scope_id = n.id\n        AND o.input_revision = n.revision AND o.model_metadata->>'schema' IS DISTINCT FROM 'canonical-overview-v3')",
    "CREATE OR REPLACE FUNCTION engram_request_overview(t TEXT, scope UUID) RETURNS VOID\n    LANGUAGE plpgsql AS $$\n    DECLARE proposed TEXT := 'task-' || replace(gen_random_uuid()::text, '-', '');\n            actual TEXT; due TIMESTAMPTZ; queue TEXT;\n    BEGIN\n      INSERT INTO consolidation_tasks(task_id, tenant_id, node_id, task_type, priority, scheduled_at)\n      VALUES(proposed, t, scope::text, 'CONSOLIDATE_OVERVIEW', 6, CURRENT_TIMESTAMP +\n        COALESCE(NULLIF(current_setting('app.overview_debounce_seconds', true), '')::integer, 30) * INTERVAL '1 second')\n      ON CONFLICT (tenant_id, node_id, task_type) WHERE status IN ('PENDING','PROCESSING')\n      DO UPDATE SET requested_generation = consolidation_tasks.requested_generation + 1\n      RETURNING task_id, scheduled_at INTO actual, due;\n      IF actual = proposed THEN\n        queue := COALESCE(NULLIF(current_setting('app.consolidation_task_queue', true), ''), 'engram-consolidation');\n        INSERT INTO workflow_dispatches(dispatch_id, workflow_type, aggregate_id, tenant_id,\n          task_queue, workflow_id, available_at)\n        VALUES ('dsp-' || replace(gen_random_uuid()::text, '-', ''), 'CONSOLIDATION', actual, t,\n          queue, 'consolidation:' || actual || ':1', due);\n      END IF;\n    END $$",
    "CREATE OR REPLACE FUNCTION engram_invalidate_overviews() RETURNS TRIGGER\n    LANGUAGE plpgsql AS $$\n    DECLARE current_data JSONB; previous_data JSONB; t TEXT; source UUID; affected RECORD;\n    BEGIN\n      IF pg_trigger_depth() > 1 THEN\n        IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;\n      END IF;\n      IF TG_OP <> 'DELETE' THEN current_data := to_jsonb(NEW); END IF;\n      IF TG_OP <> 'INSERT' THEN previous_data := to_jsonb(OLD); END IF;\n      t := COALESCE(current_data->>'tenant_id', previous_data->>'tenant_id');\n      FOR source IN SELECT DISTINCT value FROM unnest(ARRAY[\n        (current_data->>TG_ARGV[0])::uuid, (previous_data->>TG_ARGV[0])::uuid\n      ]) AS roots(value) WHERE value IS NOT NULL LOOP\n        FOR affected IN\n          WITH RECURSIVE ancestors(id) AS (\n            SELECT source UNION\n            SELECT h.parent_id FROM memory_hierarchy h JOIN ancestors a ON h.child_id = a.id\n            WHERE h.tenant_id = t\n          ) SELECT n.id, n.memory_type, n.status FROM memory_nodes n JOIN ancestors a ON a.id = n.id\n            WHERE n.tenant_id = t ORDER BY n.id\n        LOOP\n          IF NOT (TG_TABLE_NAME = 'memory_nodes' AND affected.id = source) THEN\n            UPDATE memory_nodes SET revision = revision + 1, updated_at = CURRENT_TIMESTAMP\n              WHERE tenant_id = t AND id = affected.id;\n          END IF;\n          IF affected.memory_type IN ('COLLECTION','PROJECT') AND affected.status = 'ACTIVE'\n            AND NOT (TG_TABLE_NAME = 'memory_nodes' AND TG_OP = 'DELETE' AND affected.id = source) THEN\n            PERFORM engram_request_overview(t, affected.id);\n          END IF;\n        END LOOP;\n      END LOOP;\n      IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;\n    END $$",
    "CREATE OR REPLACE FUNCTION engram_bump_overview_source_revision() RETURNS TRIGGER\n    LANGUAGE plpgsql AS $$ BEGIN\n      IF NEW.revision = OLD.revision AND (NEW.status, NEW.current_version_id, NEW.canonical_name, NEW.metadata)\n        IS DISTINCT FROM (OLD.status, OLD.current_version_id, OLD.canonical_name, OLD.metadata) THEN\n        NEW.revision := OLD.revision + 1;\n      END IF;\n      RETURN NEW;\n    END $$",
    "DROP TRIGGER IF EXISTS bump_overview_source_revision ON memory_nodes",
    "CREATE TRIGGER bump_overview_source_revision BEFORE UPDATE ON memory_nodes FOR EACH ROW EXECUTE FUNCTION engram_bump_overview_source_revision()",
    "DROP TRIGGER IF EXISTS invalidate_node_overviews ON memory_nodes",
    "CREATE TRIGGER invalidate_node_overviews AFTER UPDATE OF revision, status, current_version_id, canonical_name, metadata ON memory_nodes FOR EACH ROW EXECUTE FUNCTION engram_invalidate_overviews('id')",
    "DROP TRIGGER IF EXISTS invalidate_deleted_node_overviews ON memory_nodes",
    "CREATE TRIGGER invalidate_deleted_node_overviews BEFORE DELETE ON memory_nodes FOR EACH ROW EXECUTE FUNCTION engram_invalidate_overviews('id')",
    "DROP TRIGGER IF EXISTS invalidate_source_overviews ON memory_hierarchy",
    "CREATE TRIGGER invalidate_source_overviews AFTER INSERT OR UPDATE OR DELETE ON memory_hierarchy FOR EACH ROW EXECUTE FUNCTION engram_invalidate_overviews('parent_id')",
    "DROP TRIGGER IF EXISTS invalidate_source_overviews ON memory_claims",
    "CREATE TRIGGER invalidate_source_overviews AFTER INSERT OR UPDATE OR DELETE ON memory_claims FOR EACH ROW EXECUTE FUNCTION engram_invalidate_overviews('subject_id')",
    "DROP TRIGGER IF EXISTS invalidate_source_overviews ON memory_versions",
    "CREATE TRIGGER invalidate_source_overviews AFTER INSERT OR UPDATE OR DELETE ON memory_versions FOR EACH ROW EXECUTE FUNCTION engram_invalidate_overviews('memory_id')",
    "DROP TRIGGER IF EXISTS invalidate_source_overviews ON memory_evidence",
    "CREATE TRIGGER invalidate_source_overviews AFTER INSERT OR UPDATE OR DELETE ON memory_evidence FOR EACH ROW EXECUTE FUNCTION engram_invalidate_overviews('memory_id')",
    "DROP TRIGGER IF EXISTS invalidate_source_overviews ON entity_aliases",
    "CREATE TRIGGER invalidate_source_overviews AFTER INSERT OR UPDATE OR DELETE ON entity_aliases FOR EACH ROW EXECUTE FUNCTION engram_invalidate_overviews('entity_id')",
)


def upgrade() -> None:
    for statement in STATEMENTS:
        op.get_bind().exec_driver_sql(statement)
    op.execute(
        """DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_app') THEN GRANT SELECT, INSERT, UPDATE, DELETE ON memory_overview_dependencies TO engram_app; END IF; END $$;"""
    )


def downgrade() -> None:
    raise RuntimeError("Overview lineage migrations are forward-only")
