"""Persist what each asset's partitions came to, instead of grouping executions on every coverage read.

Coverage asks, per asset and partition key, whether any execution succeeded
and which run failed. It answered by grouping the window's executions on
every read, and found each asset's first and last attempted partition by
aggregating the asset's whole history, so a read grew with the runs behind
the calendar rather than with the calendar: about 290 ms of database time for
an organisation of a thousand assets run three times a day, 2.8 s for one ten
times larger.

``partitions`` holds the answer, one row per organisation, asset and
partition key ever attempted. ``fold_partition``, a trigger on ``executions``, merges each
execution in as it is created and again when it ends in success or failure:
``succeeded`` is an OR, ``failed_run_id`` a maximum. Both are commutative and
idempotent, so executions ending in any order, or in two transactions at
once, leave the row the grouping computed. Nothing is ever taken back out of
a row: an execution that ended in success or failure stays there, since the
runner reports ``operation_failed`` for the final failure only. The primary
key leads with the organisation every read is scoped to and carries both
outcomes, so a read never visits the table.

On a fresh or existing database ``create_all`` has already created the table
from the model; the statement here creates it in the model's shape for a
database upgraded without ``create_all``. The upgrade runs in one transaction:
creating the triggers locks ``executions``, so the event fold's concurrent
upserts wait for the backfill, then meet the triggers. The two indexes
migration 013 built for the grouping served nothing else and go.

Downgrading restores the indexes and empties the table, which the next
upgrade fills again.

Revision ID: 025
Revises: 024
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "025"
down_revision: str | None = "024"
branch_labels: str | None = None
depends_on: str | None = None

_TABLE = """CREATE TABLE IF NOT EXISTS partitions (
    org_id uuid NOT NULL,
    component_id uuid NOT NULL,
    partition_key varchar NOT NULL,
    succeeded boolean NOT NULL,
    failed_run_id uuid,
    PRIMARY KEY (org_id, component_id, partition_key) INCLUDE (succeeded, failed_run_id)
)"""

_FOLD = """CREATE OR REPLACE FUNCTION fold_partition() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO partitions AS p (org_id, component_id, partition_key, succeeded, failed_run_id)
    VALUES (
        NEW.org_id, NEW.component_id, NEW.partition_key, NEW.status = 'success',
        CASE WHEN NEW.status = 'failed' THEN NEW.run_id END
    )
    ON CONFLICT (org_id, component_id, partition_key) DO UPDATE SET
        succeeded = p.succeeded OR EXCLUDED.succeeded,
        failed_run_id = GREATEST(p.failed_run_id, EXCLUDED.failed_run_id)
    WHERE (EXCLUDED.succeeded AND NOT p.succeeded)
        OR p.failed_run_id IS DISTINCT FROM GREATEST(p.failed_run_id, EXCLUDED.failed_run_id);
    RETURN NULL;
END
$$"""

# text orders a uuid as GREATEST does, and Postgres has no max(uuid).
_BACKFILL = """INSERT INTO partitions (org_id, component_id, partition_key, succeeded, failed_run_id)
SELECT org_id, component_id, partition_key, bool_or(status = 'success'),
    (max(run_id::text) FILTER (WHERE status = 'failed'))::uuid
FROM executions
WHERE partition_key IS NOT NULL
GROUP BY org_id, component_id, partition_key"""


def upgrade() -> None:
    op.execute(_TABLE)
    op.execute(_FOLD)
    op.execute(
        "CREATE TRIGGER trg_executions_fold_partition_insert AFTER INSERT ON executions FOR EACH ROW "
        "WHEN (NEW.partition_key IS NOT NULL) EXECUTE FUNCTION fold_partition()"
    )
    op.execute(
        "CREATE TRIGGER trg_executions_fold_partition_update AFTER UPDATE OF status ON executions FOR EACH ROW "
        "WHEN (NEW.partition_key IS NOT NULL AND NEW.status IN ('success', 'failed') "
        "AND OLD.status IS DISTINCT FROM NEW.status) EXECUTE FUNCTION fold_partition()"
    )
    op.execute(_BACKFILL)
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_executions_partition_key")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_executions_component_partition_key")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_executions_partition_key ON executions "
            "(org_id, partition_key) INCLUDE (component_id, status, run_id)"
        )
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_executions_component_partition_key ON executions "
            "(org_id, component_id, partition_key)"
        )
    op.execute("DROP TRIGGER IF EXISTS trg_executions_fold_partition_update ON executions")
    op.execute("DROP TRIGGER IF EXISTS trg_executions_fold_partition_insert ON executions")
    op.execute("DROP FUNCTION IF EXISTS fold_partition()")
    op.execute("TRUNCATE partitions")
