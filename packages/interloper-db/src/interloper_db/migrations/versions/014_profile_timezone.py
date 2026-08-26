"""User-set display timezone on profiles.

The column is normally provisioned by ``create_all()`` (it is a plain
SQLModel field); this migration exists so upgrade-only paths get it too.
Idempotent: a no-op when the column already exists.

Revision ID: 014
Revises: 013
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "014"
down_revision: str | None = "013"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    bind = op.get_bind()
    columns = {col["name"] for col in sa.inspect(bind).get_columns("profiles")}
    if "timezone" in columns:
        return

    op.add_column("profiles", sa.Column("timezone", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("profiles", "timezone")
