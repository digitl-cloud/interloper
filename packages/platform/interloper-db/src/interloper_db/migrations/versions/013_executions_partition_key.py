"""Stamp each execution with its run's partition key.

Coverage reads executions by partition. Deriving the key by joining every
execution to its run made the read grow with the organisation's whole
history: about 1.2 s at 600k executions. With the key on the row, a window
reads only its own partitions, and each asset's first and last attempted
partition is one index probe apiece.

``stamp_execution_partition_key``, a ``BEFORE INSERT`` trigger on
``executions``, copies the key from the run as the fold creates the row. A
run's partition key is set when the run is created and never changes, so
the copy cannot drift. Keeping it a trigger of its own leaves the fold of
migration 012 as it was.

The upgrade runs in one transaction: adding the column locks
``executions``, so the fold's concurrent upserts wait for the backfill and
the indexes, then meet the new trigger.

Revision ID: 013
Revises: 012
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "013"
down_revision: str | None = "012"
branch_labels: str | None = None
depends_on: str | None = None

_STAMP = """CREATE OR REPLACE FUNCTION stamp_execution_partition_key() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.partition_key := (SELECT partition_key FROM runs WHERE id = NEW.run_id);
    RETURN NEW;
END
$$"""


def upgrade() -> None:
    op.execute("ALTER TABLE executions ADD COLUMN partition_key varchar")
    op.execute(_STAMP)
    op.execute(
        "CREATE TRIGGER trg_executions_stamp_partition_key BEFORE INSERT ON executions "
        "FOR EACH ROW EXECUTE FUNCTION stamp_execution_partition_key()"
    )
    op.execute("UPDATE executions e SET partition_key = r.partition_key FROM runs r WHERE r.id = e.run_id")
    op.execute(
        "CREATE INDEX ix_executions_partition_key ON executions (org_id, partition_key) "
        "INCLUDE (component_id, status, run_id)"
    )
    op.execute("CREATE INDEX ix_executions_component_partition_key ON executions (org_id, component_id, partition_key)")


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_executions_stamp_partition_key ON executions")
    op.execute("DROP FUNCTION IF EXISTS stamp_execution_partition_key()")
    op.execute("ALTER TABLE executions DROP COLUMN IF EXISTS partition_key")
