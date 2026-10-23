"""Create the Postgres control-plane schema and Temporal dispatch outbox."""

from __future__ import annotations

from alembic import op

from engram.storage.postgres import SCHEMA_SQL

revision = "0001_control_plane"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in SCHEMA_SQL.split(";"):
        if statement.strip():
            op.execute(statement)


def downgrade() -> None:
    # Control-plane removal is intentionally not automated. Restore from a
    # verified backup rather than destroying an event ledger by accident.
    raise RuntimeError("Postgres control-plane migrations are forward-only")
