"""An organisation's slug is its key: ``organisations.slug`` becomes ``organisations.key``.

The column keeps its values, so every address stays where it was. Its unique
index is recreated under the new column's name rather than renamed, which
SQLite, the unit tests' database, cannot do.

On a fresh database ``create_all`` has already created ``key`` with its index,
and migration 016 then added an empty ``slug`` beside it, so the upgrade drops
that one instead.

Revision ID: 019
Revises: 018
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "019"
down_revision: str | None = "018"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("organisations")}
    if "slug" not in columns:
        return
    op.execute("DROP INDEX IF EXISTS ix_organisations_slug")
    if "key" in columns:
        op.execute("ALTER TABLE organisations DROP COLUMN slug")
        return
    op.execute("ALTER TABLE organisations RENAME COLUMN slug TO key")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS ix_organisations_key ON organisations (key)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_organisations_key")
    op.execute("ALTER TABLE organisations RENAME COLUMN key TO slug")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS ix_organisations_slug ON organisations (slug)")
