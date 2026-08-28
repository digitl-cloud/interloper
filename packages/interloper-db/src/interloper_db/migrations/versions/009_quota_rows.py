"""Quota limits become one row per quota key.

The ``quotas`` table changes from one column per limit to
``(org_id, key, limit)`` so new quotas need no schema change. Existing
per-column overrides are unpivoted into rows. Idempotent: a no-op on
fresh databases (``create_all`` provisions the new shape) and on re-runs.

Revision ID: 009
Revises: 008
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "009"
down_revision: str | None = "008"
branch_labels: str | None = None
depends_on: str | None = None

_LEGACY_COLUMNS = ("max_sources", "max_assets_per_source", "max_successful_runs_per_month")


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not inspector.has_table("quotas"):
        return
    columns = {column["name"] for column in inspector.get_columns("quotas")}
    if "key" in columns:
        return

    op.create_table(
        "quotas_new",
        sa.Column("org_id", sa.Uuid(), primary_key=True),
        sa.Column("key", sa.String(), primary_key=True),
        sa.Column("limit", sa.Integer(), nullable=True),
    )
    for key in _LEGACY_COLUMNS:
        op.execute(
            sa.text(
                # Interpolated, not bound: the keys are module constants, never user input.
                f'INSERT INTO quotas_new (org_id, key, "limit") '
                f"SELECT org_id, '{key}', {key} FROM quotas WHERE {key} IS NOT NULL"
            )
        )
    op.drop_table("quotas")
    op.rename_table("quotas_new", "quotas")


def downgrade() -> None:
    op.create_table(
        "quotas_old",
        sa.Column("org_id", sa.Uuid(), primary_key=True),
        sa.Column("max_sources", sa.Integer(), nullable=True),
        sa.Column("max_assets_per_source", sa.Integer(), nullable=True),
        sa.Column("max_successful_runs_per_month", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.execute(
        sa.text(
            "INSERT INTO quotas_old (org_id, max_sources, max_assets_per_source, max_successful_runs_per_month) "
            "SELECT org_id, "
            "MAX(CASE WHEN key = 'max_sources' THEN \"limit\" END), "
            "MAX(CASE WHEN key = 'max_assets_per_source' THEN \"limit\" END), "
            "MAX(CASE WHEN key = 'max_successful_runs_per_month' THEN \"limit\" END) "
            "FROM quotas GROUP BY org_id"
        )
    )
    op.drop_table("quotas")
    op.rename_table("quotas_old", "quotas")
