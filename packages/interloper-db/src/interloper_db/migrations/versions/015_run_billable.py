"""Billability recorded on runs.

``runs.billable`` is stamped at creation from the target operation's
declaration, so quota decisions survive the component's deletion
(``component_id`` nulls, the run stays as history). Existing rows all
predate non-billable operations, so the ``true`` default is also the
correct backfill.

The column is normally provisioned by ``create_all()`` (it is a plain
SQLModel field); this migration exists so upgrade-only paths get it too.
Idempotent: a no-op when the column already exists.

Revision ID: 015
Revises: 014
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "015"
down_revision: str | None = "014"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    bind = op.get_bind()
    columns = {col["name"] for col in sa.inspect(bind).get_columns("runs")}
    if "billable" in columns:
        return

    op.add_column("runs", sa.Column("billable", sa.Boolean(), nullable=False, server_default=sa.text("true")))


def downgrade() -> None:
    op.drop_column("runs", "billable")
