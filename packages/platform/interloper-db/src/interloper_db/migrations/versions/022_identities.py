"""A profile signs in through identities: ``identities`` takes over from ``profiles.google_id``.

An identity provider names a person by its issuer and a subject unique within
it, so ``identities`` holds one row per pair, pointing at the profile it signs
in to; another provider than Google can then sign a profile in. Every existing
profile signed in with Google, whose ID token ``sub`` its ``google_id`` holds,
so each becomes an identity issued by ``https://accounts.google.com``.

``profiles.google_id`` stays one more release, no longer required: pods of
the previous release keep reading and writing it while this one rolls out,
and a profile created by this release has none. The release after drops it.

On a fresh database ``create_all`` has already created ``identities``, and a
``profiles`` table without ``google_id``, so the upgrade creates the table only
for a database upgraded without ``create_all`` and copies only where the column
exists.

The downgrade writes each Google identity back into ``google_id`` and drops the
table. A profile with no Google identity, which nothing before this revision
creates, blocks it: the column is required again.

Revision ID: 022
Revises: 021
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "022"
down_revision: str | None = "021"
branch_labels: str | None = None
depends_on: str | None = None

_GOOGLE_ISSUER = "https://accounts.google.com"

_IDENTITIES = """CREATE TABLE IF NOT EXISTS identities (
    issuer varchar NOT NULL,
    subject varchar NOT NULL,
    profile_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT identities_pkey PRIMARY KEY (issuer, subject),
    CONSTRAINT identities_profile_id_fkey FOREIGN KEY (profile_id) REFERENCES profiles (id)
)"""


def upgrade() -> None:
    op.execute(_IDENTITIES)
    op.execute("CREATE INDEX IF NOT EXISTS ix_identities_profile_id ON identities (profile_id)")
    if "google_id" in {column["name"] for column in sa.inspect(op.get_bind()).get_columns("profiles")}:
        op.execute(
            "INSERT INTO identities (issuer, subject, profile_id) "
            f"SELECT '{_GOOGLE_ISSUER}', google_id, id FROM profiles WHERE google_id IS NOT NULL "
            "ON CONFLICT DO NOTHING"
        )
        op.execute("ALTER TABLE profiles ALTER COLUMN google_id DROP NOT NULL")


def downgrade() -> None:
    if "google_id" not in {column["name"] for column in sa.inspect(op.get_bind()).get_columns("profiles")}:
        op.execute("ALTER TABLE profiles ADD COLUMN google_id varchar")
    op.execute(
        "UPDATE profiles SET google_id = identities.subject FROM identities "
        f"WHERE identities.profile_id = profiles.id AND identities.issuer = '{_GOOGLE_ISSUER}'"
    )
    op.execute("ALTER TABLE profiles ALTER COLUMN google_id SET NOT NULL")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS ix_profiles_google_id ON profiles (google_id)")
    op.execute("DROP TABLE identities")
