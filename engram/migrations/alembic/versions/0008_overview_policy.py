"""Allow immutable overview regeneration after a generator-policy change."""

from alembic import op

revision = "0008_overview_policy"
down_revision = "0007_durable_chat"
branch_labels = None
depends_on = None


INVALIDATION_SQL = """CREATE OR REPLACE FUNCTION engram_invalidate_overviews() RETURNS TRIGGER
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
    END $$"""


def upgrade() -> None:
    for statement in (
        "ALTER TABLE memory_overviews ADD COLUMN IF NOT EXISTS generation_policy TEXT NOT NULL DEFAULT 'legacy'",
        "ALTER TABLE memory_overviews DROP CONSTRAINT IF EXISTS memory_overviews_tenant_id_scope_id_input_revision_key",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_overviews_generation_policy "
        "ON memory_overviews(tenant_id, scope_id, input_revision, generation_policy)",
    ):
        op.execute(statement)
    op.execute(INVALIDATION_SQL)


def downgrade() -> None:
    raise RuntimeError("Overview policy migrations are forward-only")
