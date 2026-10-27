"""A personal access token stands for its user: ``personal_access_tokens`` becomes ``tokens``.

A token was scoped to one organisation; it now reaches every organisation its
holder belongs to, with the holder's live role in each, and the caller names
the organisation it acts in. ``tokens`` is the one table for every bearer
secret a user holds: ``kind`` says what issued it (``personal`` for every
existing row), ``client_id`` and ``resource`` name the OAuth client and the
surface an access token is issued for, and ``name`` is a personal token's
label only. The table is renamed and altered in place, so every existing
token keeps its hash and goes on working.

``tokens`` is this migration's (``create_all`` skips it): on an existing
database ``create_all`` runs first and would otherwise create an empty
``tokens`` in the way of the rename. A fresh database never had
``personal_access_tokens``, so the upgrade creates that table in its last
shape first and both paths take the same rename.

Downgrading keeps the personal tokens and scopes each to its holder's last
organisation, else their oldest membership; a token whose holder belongs to
no organisation, and every token of another kind, is deleted.

Revision ID: 015
Revises: 014
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "015"
down_revision: str | None = "014"
branch_labels: str | None = None
depends_on: str | None = None

_PERSONAL_ACCESS_TOKENS = """CREATE TABLE IF NOT EXISTS personal_access_tokens (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    name varchar NOT NULL,
    token_prefix varchar NOT NULL,
    token_hash varchar NOT NULL,
    expires_at timestamp with time zone,
    last_used_at timestamp with time zone,
    revoked_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT personal_access_tokens_pkey PRIMARY KEY (id),
    CONSTRAINT personal_access_tokens_user_id_fkey FOREIGN KEY (user_id) REFERENCES profiles (id)
)"""

# Names the old table's objects carry, and the names the model gives them on ``tokens``.
_RENAMED_CONSTRAINTS = (
    ("personal_access_tokens_pkey", "tokens_pkey"),
    ("personal_access_tokens_user_id_fkey", "tokens_user_id_fkey"),
)
_RENAMED_INDEXES = (
    ("ix_personal_access_tokens_user_id", "ix_tokens_user_id"),
    ("ix_personal_access_tokens_token_hash", "ix_tokens_token_hash"),
)


def upgrade() -> None:
    op.execute(_PERSONAL_ACCESS_TOKENS)
    op.execute("CREATE INDEX IF NOT EXISTS ix_personal_access_tokens_user_id ON personal_access_tokens (user_id)")
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_personal_access_tokens_token_hash ON personal_access_tokens (token_hash)"
    )
    op.execute("ALTER TABLE personal_access_tokens RENAME TO tokens")
    for old, new in _RENAMED_CONSTRAINTS:
        op.execute(f"ALTER TABLE tokens RENAME CONSTRAINT {old} TO {new}")
    for old, new in _RENAMED_INDEXES:
        op.execute(f"ALTER INDEX {old} RENAME TO {new}")
    op.execute("ALTER TABLE tokens DROP COLUMN IF EXISTS organisation_id")
    op.execute("ALTER TABLE tokens ADD COLUMN kind varchar NOT NULL DEFAULT 'personal'")
    op.execute("ALTER TABLE tokens ADD COLUMN client_id varchar")
    op.execute("ALTER TABLE tokens ADD COLUMN resource varchar")
    op.execute("ALTER TABLE tokens ALTER COLUMN name DROP NOT NULL")


def downgrade() -> None:
    op.execute("DELETE FROM tokens WHERE kind <> 'personal'")
    op.execute(
        "ALTER TABLE tokens ADD COLUMN organisation_id uuid "
        "CONSTRAINT personal_access_tokens_organisation_id_fkey REFERENCES organisations (id)"
    )
    op.execute(
        """UPDATE tokens SET organisation_id = COALESCE(
            (
                SELECT m.organisation_id FROM user_organisations m
                JOIN profiles p ON p.id = m.user_id AND p.last_organisation_id = m.organisation_id
                WHERE m.user_id = tokens.user_id
            ),
            (
                SELECT m.organisation_id FROM user_organisations m
                WHERE m.user_id = tokens.user_id
                ORDER BY m.created_at, m.organisation_id
                LIMIT 1
            )
        )"""
    )
    op.execute("DELETE FROM tokens WHERE organisation_id IS NULL")
    op.execute("ALTER TABLE tokens ALTER COLUMN organisation_id SET NOT NULL")
    op.execute("ALTER TABLE tokens ALTER COLUMN name SET NOT NULL")
    op.execute("ALTER TABLE tokens DROP COLUMN kind, DROP COLUMN client_id, DROP COLUMN resource")
    for old, new in _RENAMED_INDEXES:
        op.execute(f"ALTER INDEX {new} RENAME TO {old}")
    for old, new in _RENAMED_CONSTRAINTS:
        op.execute(f"ALTER TABLE tokens RENAME CONSTRAINT {new} TO {old}")
    op.execute("ALTER TABLE tokens RENAME TO personal_access_tokens")
    op.execute("CREATE INDEX ix_personal_access_tokens_organisation_id ON personal_access_tokens (organisation_id)")
