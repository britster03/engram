"""Add payload, revision, and fenced leases to Temporal dispatch rows.

The first control-plane migration predates the production dispatcher and
created a minimal ``workflow_dispatches`` table.  This table is the sole
transactional outbox: canonical mutations carry their stable IDs and revision
in its payload/envelope, so no second projection-outbox table is required.
"""

from __future__ import annotations

from alembic import op

revision = "0002_temporal_dispatch_leases"
down_revision = "0001_control_plane"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Every statement is idempotent so an operator can safely run this after a
    # deployment that bootstrapped the newer SCHEMA_SQL definition directly.
    op.execute("ALTER TABLE workflow_dispatches ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ")
    op.execute("ALTER TABLE workflow_dispatches ADD COLUMN IF NOT EXISTS claim_token TEXT")
    op.execute("ALTER TABLE workflow_dispatches ADD COLUMN IF NOT EXISTS lease_owner TEXT")
    op.execute("ALTER TABLE workflow_dispatches ADD COLUMN IF NOT EXISTS claimed_until TIMESTAMPTZ")
    op.execute(
        "ALTER TABLE workflow_dispatches "
        "ADD COLUMN IF NOT EXISTS failure_count INTEGER NOT NULL DEFAULT 0"
    )
    op.execute("ALTER TABLE workflow_dispatches ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ")
    op.execute("ALTER TABLE workflow_dispatches ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ")
    op.execute("ALTER TABLE workflow_dispatches ADD COLUMN IF NOT EXISTS failure_class TEXT")
    op.execute(
        "ALTER TABLE workflow_dispatches "
        "ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb"
    )
    op.execute("ALTER TABLE workflow_dispatches ADD COLUMN IF NOT EXISTS aggregate_revision BIGINT")
    op.execute(
        "UPDATE workflow_dispatches SET available_at = COALESCE(available_at, created_at) "
        "WHERE available_at IS NULL"
    )
    op.execute(
        "ALTER TABLE workflow_dispatches ALTER COLUMN available_at SET DEFAULT CURRENT_TIMESTAMP"
    )
    op.execute("ALTER TABLE workflow_dispatches ALTER COLUMN available_at SET NOT NULL")
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_workflow_dispatches_due "
        "ON workflow_dispatches(status, available_at, created_at) "
        "WHERE status = 'PENDING'"
    )


def downgrade() -> None:
    # Dispatch state is operationally important and the control-plane
    # migrations are intentionally forward-only. Restore a verified backup if
    # these columns ever need to be removed.
    raise RuntimeError("Temporal dispatch migrations are forward-only")
