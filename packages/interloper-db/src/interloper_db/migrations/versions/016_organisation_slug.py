"""Organisations carry a slug: their name in the app's address.

Every existing organisation takes the slug derived from its name by
:func:`interloper_db.organisation_keys.unique_key`, the oldest organisation of a name
getting the plain one and later ones a number, so a migrated database holds
what ``OrganisationStore.create`` would have given it. Deleted organisations
get one too: the unique index spans every row. The derivation is imported
rather than copied, so it stays the store's.

On a fresh database ``create_all`` has already created ``organisations`` with
the column and its index, and there are no rows to fill, so every statement is
idempotent.

Revision ID: 016
Revises: 015
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from interloper_db.organisation_keys import unique_key

# revision identifiers, used by Alembic.
revision: str = "016"
down_revision: str | None = "015"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE organisations ADD COLUMN IF NOT EXISTS slug varchar")
    connection = op.get_bind()
    taken = set(connection.execute(sa.text("SELECT slug FROM organisations WHERE slug IS NOT NULL")).scalars())
    pending = connection.execute(
        sa.text("SELECT id, name FROM organisations WHERE slug IS NULL ORDER BY created_at, id")
    ).all()
    for org_id, name in pending:
        slug = unique_key(name, taken)
        taken.add(slug)
        connection.execute(sa.text("UPDATE organisations SET slug = :s WHERE id = :id"), {"s": slug, "id": org_id})
    op.execute("ALTER TABLE organisations ALTER COLUMN slug SET NOT NULL")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS ix_organisations_slug ON organisations (slug)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_organisations_slug")
    op.execute("ALTER TABLE organisations DROP COLUMN IF EXISTS slug")
