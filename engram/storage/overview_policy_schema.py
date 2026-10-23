"""Immutable overview artifacts are identified by sources and generator policy."""

OVERVIEW_POLICY_SCHEMA = (
    "ALTER TABLE memory_overviews ADD COLUMN IF NOT EXISTS generation_policy TEXT NOT NULL DEFAULT 'legacy'",
    "ALTER TABLE memory_overviews DROP CONSTRAINT IF EXISTS memory_overviews_tenant_id_scope_id_input_revision_key",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_overviews_generation_policy "
    "ON memory_overviews(tenant_id, scope_id, input_revision, generation_policy)",
)
