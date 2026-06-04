"""Add retry lineage columns to runs.

A failed run can be retried by creating a new run that points back to its
predecessor via ``retry_of`` and carries an incremented ``attempt`` count.
``retry_scope`` records whether the retry re-runs the whole DAG (``"all"``)
or only the previously failed assets (``"failed"``).

Revision ID: 006
Revises: 005
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "006"
down_revision: str | None = "005"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Add retry_of, attempt, and retry_scope columns to the runs table."""
    op.execute('ALTER TABLE runs ADD COLUMN IF NOT EXISTS retry_of UUID REFERENCES runs(id) ON DELETE SET NULL;')
    op.execute('ALTER TABLE runs ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 1;')
    op.execute('ALTER TABLE runs ADD COLUMN IF NOT EXISTS retry_scope VARCHAR;')
    op.execute('CREATE INDEX IF NOT EXISTS ix_runs_retry_of ON runs (retry_of);')


def downgrade() -> None:
    """Remove the retry lineage columns from the runs table."""
    op.execute('DROP INDEX IF EXISTS ix_runs_retry_of;')
    op.execute('ALTER TABLE runs DROP COLUMN IF EXISTS retry_scope;')
    op.execute('ALTER TABLE runs DROP COLUMN IF EXISTS attempt;')
    op.execute('ALTER TABLE runs DROP COLUMN IF EXISTS retry_of;')
