"""Quota limits, the usage ledger, and the run reservation marker.

``create_all()`` provisions the two new tables on fresh databases; this
migration covers upgrade-only paths (tables) and existing databases (the
``runs.quota_reserved_at`` column, which ``create_all`` never adds to an
already-existing table). Idempotent: every step checks before acting.

Revision ID: 007
Revises: 006
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "007"
down_revision: str | None = "006"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table("quotas"):
        op.create_table(
            "quotas",
            sa.Column("org_id", sa.Uuid(), primary_key=True),
            sa.Column("max_sources", sa.Integer(), nullable=True),
            sa.Column("max_assets_per_source", sa.Integer(), nullable=True),
            sa.Column("max_successful_runs_per_month", sa.Integer(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP")),
        )

    if not inspector.has_table("usage"):
        op.create_table(
            "usage",
            sa.Column("org_id", sa.Uuid(), primary_key=True),
            sa.Column("metric", sa.String(), primary_key=True),
            sa.Column("period_start", sa.Date(), primary_key=True),
            sa.Column("used", sa.Integer(), nullable=False),
            sa.Column("reserved", sa.Integer(), nullable=False),
        )

    # Seed the ledger from run history so reconciliation is meaningful from
    # day one — without this, months containing runs that succeeded before
    # metering existed under-count forever. ON CONFLICT keeps any rows live
    # charging already wrote (re-runs and mixed-version upgrades).
    if bind.dialect.name == "postgresql":
        op.execute(
            """
            INSERT INTO usage (org_id, metric, period_start, used, reserved)
            SELECT org_id, 'successful_runs',
                   (date_trunc('month', completed_at AT TIME ZONE 'UTC'))::date,
                   count(*), 0
            FROM runs
            WHERE status = 'success' AND completed_at IS NOT NULL
            GROUP BY org_id, 3
            ON CONFLICT (org_id, metric, period_start) DO NOTHING
            """
        )

    run_columns = {column["name"] for column in inspector.get_columns("runs")}
    if "quota_reserved_at" not in run_columns:
        op.add_column("runs", sa.Column("quota_reserved_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    # The usage table is the billing ledger and is deliberately kept.
    op.drop_column("runs", "quota_reserved_at")
    op.drop_table("quotas")
