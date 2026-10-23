"""Derived-view invalidation schema. Migration 0006 freezes its initial SQL."""

OVERVIEW_SCHEMA = (
    "ALTER TABLE consolidation_tasks ADD COLUMN IF NOT EXISTS requested_generation BIGINT NOT NULL DEFAULT 1",
    "ALTER TABLE consolidation_tasks ADD COLUMN IF NOT EXISTS processing_generation BIGINT",
    """CREATE TABLE IF NOT EXISTS memory_overview_dependencies (
        tenant_id TEXT NOT NULL, overview_id UUID NOT NULL, memory_id UUID NOT NULL,
        input_revision BIGINT NOT NULL, version_id UUID,
        PRIMARY KEY (tenant_id, overview_id, memory_id),
        FOREIGN KEY (tenant_id, overview_id) REFERENCES memory_overviews(tenant_id, id) ON DELETE CASCADE,
        FOREIGN KEY (tenant_id, memory_id) REFERENCES memory_nodes(tenant_id, id) ON DELETE CASCADE
    )""",
    "CREATE INDEX IF NOT EXISTS idx_overview_dependencies_source ON memory_overview_dependencies(tenant_id, memory_id)",
    "ALTER TABLE memory_overview_dependencies ENABLE ROW LEVEL SECURITY",
    "DROP POLICY IF EXISTS overview_dependency_tenant ON memory_overview_dependencies",
    "CREATE POLICY overview_dependency_tenant ON memory_overview_dependencies USING (tenant_id = current_setting('app.tenant_id', true)) WITH CHECK (tenant_id = current_setting('app.tenant_id', true))",
    # Retain immutable legacy artifacts, but reserve a fresh revision for v3.
    """UPDATE memory_nodes n SET revision = revision + 1 WHERE EXISTS (
        SELECT 1 FROM memory_overviews o WHERE o.tenant_id = n.tenant_id AND o.scope_id = n.id
        AND o.input_revision = n.revision AND o.model_metadata->>'schema' IS DISTINCT FROM 'canonical-overview-v3')""",
    """CREATE OR REPLACE FUNCTION engram_request_overview(t TEXT, scope UUID) RETURNS VOID
    LANGUAGE plpgsql AS $$
    DECLARE proposed TEXT := 'task-' || replace(gen_random_uuid()::text, '-', '');
            actual TEXT; due TIMESTAMPTZ; queue TEXT;
    BEGIN
      INSERT INTO consolidation_tasks(task_id, tenant_id, node_id, task_type, priority, scheduled_at)
      VALUES(proposed, t, scope::text, 'CONSOLIDATE_OVERVIEW', 6, CURRENT_TIMESTAMP +
        COALESCE(NULLIF(current_setting('app.overview_debounce_seconds', true), '')::integer, 30) * INTERVAL '1 second')
      ON CONFLICT (tenant_id, node_id, task_type) WHERE status IN ('PENDING','PROCESSING')
      DO UPDATE SET requested_generation = consolidation_tasks.requested_generation + 1
      RETURNING task_id, scheduled_at INTO actual, due;
      IF actual = proposed THEN
        queue := COALESCE(NULLIF(current_setting('app.consolidation_task_queue', true), ''), 'engram-consolidation');
        INSERT INTO workflow_dispatches(dispatch_id, workflow_type, aggregate_id, tenant_id,
          task_queue, workflow_id, available_at)
        VALUES ('dsp-' || replace(gen_random_uuid()::text, '-', ''), 'CONSOLIDATION', actual, t,
          queue, 'consolidation:' || actual || ':1', due);
      END IF;
    END $$""",
    """CREATE OR REPLACE FUNCTION engram_invalidate_overviews() RETURNS TRIGGER
    LANGUAGE plpgsql AS $$
    DECLARE current_data JSONB; previous_data JSONB; t TEXT; source UUID; affected RECORD;
    BEGIN
      IF pg_trigger_depth() > 1 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
      END IF;
      IF TG_OP <> 'DELETE' THEN current_data := to_jsonb(NEW); END IF;
      IF TG_OP <> 'INSERT' THEN previous_data := to_jsonb(OLD); END IF;
      t := COALESCE(current_data->>'tenant_id', previous_data->>'tenant_id');
      FOR source IN SELECT DISTINCT value FROM unnest(ARRAY[
        (current_data->>TG_ARGV[0])::uuid, (previous_data->>TG_ARGV[0])::uuid
      ]) AS roots(value) WHERE value IS NOT NULL LOOP
        FOR affected IN
          WITH RECURSIVE ancestors(id) AS (
            SELECT source UNION
            SELECT h.parent_id FROM memory_hierarchy h JOIN ancestors a ON h.child_id = a.id
            WHERE h.tenant_id = t
          ) SELECT n.id, n.memory_type, n.status FROM memory_nodes n JOIN ancestors a ON a.id = n.id
            WHERE n.tenant_id = t ORDER BY n.id
        LOOP
          IF NOT (TG_TABLE_NAME = 'memory_nodes' AND affected.id = source) THEN
            UPDATE memory_nodes SET revision = revision + 1, updated_at = CURRENT_TIMESTAMP
              WHERE tenant_id = t AND id = affected.id;
          END IF;
          IF affected.memory_type IN ('COLLECTION','PROJECT','EPISODE') AND affected.status = 'ACTIVE'
            AND NOT (TG_TABLE_NAME = 'memory_nodes' AND TG_OP = 'DELETE' AND affected.id = source) THEN
            PERFORM engram_request_overview(t, affected.id);
          END IF;
        END LOOP;
      END LOOP;
      IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
    END $$""",
    """CREATE OR REPLACE FUNCTION engram_bump_overview_source_revision() RETURNS TRIGGER
    LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.revision = OLD.revision AND (NEW.status, NEW.current_version_id, NEW.canonical_name, NEW.metadata)
        IS DISTINCT FROM (OLD.status, OLD.current_version_id, OLD.canonical_name, OLD.metadata) THEN
        NEW.revision := OLD.revision + 1;
      END IF;
      RETURN NEW;
    END $$""",
    "DROP TRIGGER IF EXISTS bump_overview_source_revision ON memory_nodes",
    "CREATE TRIGGER bump_overview_source_revision BEFORE UPDATE ON memory_nodes FOR EACH ROW EXECUTE FUNCTION engram_bump_overview_source_revision()",
    "DROP TRIGGER IF EXISTS invalidate_node_overviews ON memory_nodes",
    "CREATE TRIGGER invalidate_node_overviews AFTER UPDATE OF revision, status, current_version_id, canonical_name, metadata ON memory_nodes FOR EACH ROW EXECUTE FUNCTION engram_invalidate_overviews('id')",
    "DROP TRIGGER IF EXISTS invalidate_deleted_node_overviews ON memory_nodes",
    "CREATE TRIGGER invalidate_deleted_node_overviews BEFORE DELETE ON memory_nodes FOR EACH ROW EXECUTE FUNCTION engram_invalidate_overviews('id')",
    *tuple(
        statement
        for table, field in (
            ("memory_hierarchy", "parent_id"),
            ("memory_claims", "subject_id"),
            ("memory_versions", "memory_id"),
            ("memory_evidence", "memory_id"),
            ("entity_aliases", "entity_id"),
        )
        for statement in (
            f"DROP TRIGGER IF EXISTS invalidate_source_overviews ON {table}",
            f"CREATE TRIGGER invalidate_source_overviews AFTER INSERT OR UPDATE OR DELETE ON {table} "
            f"FOR EACH ROW EXECUTE FUNCTION engram_invalidate_overviews('{field}')",
        )
    ),
)
