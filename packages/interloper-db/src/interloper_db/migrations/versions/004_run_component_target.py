"""Runs and backfills target any component: rename job_id → component_id.

A run's target generalizes from "a job" to "any runnable component" (job,
source, or asset — ad-hoc materialization without a throwaway job). This is
an in-place rename: run and backfill history is preserved, and the FK still
points at ``components(id) ON DELETE SET NULL``.

Guarded so it is a no-op on freshly provisioned databases, where
``create_all()`` already produced the new column names.

Revision ID: 004
Revises: 003
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "004"
down_revision: str | None = "003"
branch_labels: str | None = None
depends_on: str | None = None

_RENAMES = [
    # (table, old constraint fkey, old index)
    ("runs", "runs_job_id_fkey", "ix_runs_job_id"),
    ("backfills", "backfills_job_id_fkey", "ix_backfills_job_id"),
]


def upgrade() -> None:
    """Rename the target column, its FK constraint, and its index."""
    for table, fkey, index in _RENAMES:
        op.execute(
            f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = '{table}' AND column_name = 'job_id'
                ) THEN
                    ALTER TABLE {table} RENAME COLUMN job_id TO component_id;
                    ALTER TABLE {table} RENAME CONSTRAINT {fkey} TO {table}_component_id_fkey;
                    ALTER INDEX {index} RENAME TO ix_{table}_component_id;
                END IF;
            END $$
            """
        )


def downgrade() -> None:
    """Rename back to job_id."""
    for table, _fkey, _index in _RENAMES:
        op.execute(
            f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = '{table}' AND column_name = 'component_id'
                ) THEN
                    ALTER TABLE {table} RENAME COLUMN component_id TO job_id;
                    ALTER TABLE {table} RENAME CONSTRAINT {table}_component_id_fkey TO {table}_job_id_fkey;
                    ALTER INDEX ix_{table}_component_id RENAME TO {"ix_" + table + "_job_id"};
                END IF;
            END $$
            """
        )
