"""Make logical claim identity lifecycle-independent.

Unknown predicates are multi-valued and coexist.  Earlier V2 builds marked
later values CONFLICTING and only deduplicated ACTIVE rows, which let repeated
evidence create duplicate non-active claims.  This forward migration merges
their evidence, restores coexistence, and enforces one logical claim across
all lifecycle states.
"""

from __future__ import annotations

from alembic import op

revision = "0004_claim_identity_policy"
down_revision = "0003_canonical_memory"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TEMP TABLE claim_identity_duplicates ON COMMIT DROP AS
        WITH ranked AS (
            SELECT
                tenant_id,
                id AS duplicate_id,
                first_value(id) OVER identity AS survivor_id,
                row_number() OVER identity AS position
            FROM memory_claims
            WINDOW identity AS (
                PARTITION BY tenant_id, subject_id, predicate, normalized_object_hash,
                    COALESCE(valid_from, '-infinity'::timestamptz),
                    COALESCE(valid_until, 'infinity'::timestamptz)
                ORDER BY (status = 'ACTIVE') DESC, created_at, id
            )
        )
        SELECT tenant_id, duplicate_id, survivor_id
        FROM ranked
        WHERE position > 1
        """
    )
    op.execute(
        """
        UPDATE memory_evidence AS evidence
        SET claim_id = duplicate.survivor_id
        FROM claim_identity_duplicates AS duplicate
        WHERE evidence.tenant_id = duplicate.tenant_id
          AND evidence.claim_id = duplicate.duplicate_id
        """
    )
    op.execute(
        """
        UPDATE memory_claims AS claim
        SET supersedes_claim_id = CASE
                WHEN claim.id = duplicate.survivor_id THEN NULL
                ELSE duplicate.survivor_id
            END
        FROM claim_identity_duplicates AS duplicate
        WHERE claim.tenant_id = duplicate.tenant_id
          AND claim.supersedes_claim_id = duplicate.duplicate_id
        """
    )
    op.execute(
        """
        UPDATE memory_claims AS claim
        SET superseded_by_claim_id = CASE
                WHEN claim.id = duplicate.survivor_id THEN NULL
                ELSE duplicate.survivor_id
            END
        FROM claim_identity_duplicates AS duplicate
        WHERE claim.tenant_id = duplicate.tenant_id
          AND claim.superseded_by_claim_id = duplicate.duplicate_id
        """
    )
    op.execute(
        """
        DELETE FROM memory_claims AS claim
        USING claim_identity_duplicates AS duplicate
        WHERE claim.tenant_id = duplicate.tenant_id
          AND claim.id = duplicate.duplicate_id
        """
    )
    op.execute(
        """
        UPDATE memory_claims
        SET status = 'ACTIVE', updated_at = CURRENT_TIMESTAMP
        WHERE status = 'CONFLICTING'
          AND predicate <> 'HAS_BIRTH_DATE'
        """
    )
    op.execute("DROP INDEX IF EXISTS idx_memory_claims_active_fingerprint")
    op.execute("ALTER TABLE memory_claims ALTER COLUMN normalized_object_hash SET NOT NULL")
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_claims_logical_fingerprint
        ON memory_claims(
            tenant_id, subject_id, predicate, normalized_object_hash,
            COALESCE(valid_from, '-infinity'::timestamptz),
            COALESCE(valid_until, 'infinity'::timestamptz)
        )
        """
    )


def downgrade() -> None:
    raise RuntimeError("Canonical memory migrations are forward-only")
