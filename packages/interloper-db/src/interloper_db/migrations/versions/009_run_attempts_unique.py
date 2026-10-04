"""Hold each attempt of a run stack in exactly one run.

A stack is meant to be a linear chain of attempts, but nothing in the schema
said so: two concurrent retries of the same head both succeeded, and releases
before the head-only retry fix let an earlier attempt be retried a second
time. Both leave two runs under one ``(root_run_id, attempt)``, which every
"latest attempt of the stack" read then counts twice. A unique index makes
the database refuse the branch.

The index cannot be built over a table that already holds a branched stack,
and a failed ``CREATE UNIQUE INDEX CONCURRENTLY`` leaves an invalid index
behind that ``IF NOT EXISTS`` would then skip, so the upgrade checks first
and refuses with the offending stacks named. Resolving them is a data
decision, not a schema one, so the migration leaves it to the operator. The
index is built ``CONCURRENTLY`` so ``runs`` keeps taking writes, and
``IF NOT EXISTS`` because ``create_all`` already builds it on a fresh
database.

Revision ID: 009
Revises: 008
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "009"
down_revision: str | None = "008"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    branched = (
        op.get_bind()
        .execute(text("SELECT DISTINCT root_run_id FROM runs GROUP BY root_run_id, attempt HAVING count(*) > 1"))
        .scalars()
        .all()
    )
    if branched:
        raise RuntimeError(
            f"{len(branched)} run stack(s) hold more than one run for the same attempt, so attempts cannot be made "
            f"unique; resolve them before upgrading (root_run_id: {', '.join(str(root) for root in branched)})"
        )
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ix_runs_root_run_id_attempt ON runs (root_run_id, attempt)"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_runs_root_run_id_attempt")
