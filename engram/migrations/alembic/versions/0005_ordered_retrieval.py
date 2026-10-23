"""Index current and past conversation retrieval."""

from alembic import op

from engram.storage.session_search import SESSION_SEARCH_INDEXES, SESSION_STATE_SQL

revision = "0005_ordered_retrieval"
down_revision = "0004_claim_identity_policy"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(SESSION_STATE_SQL)
    op.execute("""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_app') THEN
            GRANT SELECT, INSERT, UPDATE, DELETE ON session_states TO engram_app;
        END IF;
    END $$;""")
    # Concurrent construction avoids blocking ingestion on existing deployments.
    with op.get_context().autocommit_block():
        for statement in SESSION_SEARCH_INDEXES:
            op.execute(statement.replace("INDEX IF NOT EXISTS", "INDEX CONCURRENTLY IF NOT EXISTS"))


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for name in (
            "idx_memory_versions_body_search",
            "idx_events_conversation_text",
            "idx_events_session_history",
        ):
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
    op.drop_table("session_states")
