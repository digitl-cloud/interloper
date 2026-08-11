"""Soft-delete marker for organisations.

``create_all()`` provisions the column on fresh databases; this migration
adds it to existing ones (``create_all`` never alters a table). Idempotent.

Revision ID: 008
Revises: 007
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "008"
down_revision: str | None = "007"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    bind = op.get_bind()
    columns = {column["name"] for column in sa.inspect(bind).get_columns("organisations")}
    if "deleted_at" not in columns:
        op.add_column("organisations", sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("organisations", "deleted_at")
