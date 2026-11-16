"""``profiles.google_id`` goes: a profile's Google sign-in is its identity issued by Google.

Migration 022 copied every ``google_id`` into ``identities`` and left the
column, nullable, for the pods of the release before it. A first sign-in one
of those pods served during that rollout wrote a ``google_id`` and no identity,
so the upgrade copies any value still missing before it drops the column and
its unique index. On a fresh database ``create_all`` made ``profiles`` without
the column, and the upgrade does nothing.

The downgrade adds the column back as 022 left it, nullable and unique, filled
from each profile's Google identity.

Revision ID: 024
Revises: 023
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "024"
down_revision: str | None = "023"
branch_labels: str | None = None
depends_on: str | None = None

_GOOGLE_ISSUER = "https://accounts.google.com"


def upgrade() -> None:
    if "google_id" not in {column["name"] for column in sa.inspect(op.get_bind()).get_columns("profiles")}:
        return
    op.execute(
        "INSERT INTO identities (issuer, subject, profile_id) "
        f"SELECT '{_GOOGLE_ISSUER}', google_id, id FROM profiles WHERE google_id IS NOT NULL "
        "ON CONFLICT DO NOTHING"
    )
    op.execute("DROP INDEX IF EXISTS ix_profiles_google_id")
    op.execute("ALTER TABLE profiles DROP COLUMN google_id")


def downgrade() -> None:
    if "google_id" not in {column["name"] for column in sa.inspect(op.get_bind()).get_columns("profiles")}:
        op.execute("ALTER TABLE profiles ADD COLUMN google_id varchar")
    op.execute(
        "UPDATE profiles SET google_id = identities.subject FROM identities "
        f"WHERE identities.profile_id = profiles.id AND identities.issuer = '{_GOOGLE_ISSUER}'"
    )
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS ix_profiles_google_id ON profiles (google_id)")
