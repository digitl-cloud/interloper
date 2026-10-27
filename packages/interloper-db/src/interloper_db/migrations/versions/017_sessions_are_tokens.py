"""A login session is a token: ``sessions`` folds into ``tokens`` as kind ``session``.

Every live session moves with its hash and expiry, so nobody is logged out.
Its organisation is dropped: the request names the organisation it acts in,
and ``profiles.last_organisation_id`` already remembers the last one used.
Expired sessions are left behind with the table.

A fresh database never had ``sessions`` (``create_all`` no longer knows the
model), so the upgrade creates it in its last shape first and both paths
take the same move.

Downgrading moves the live session tokens back into a recreated
``sessions``, without an organisation, and deletes every session token.

Revision ID: 017
Revises: 016
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "017"
down_revision: str | None = "016"
branch_labels: str | None = None
depends_on: str | None = None

# The default is parenthesised so the DDL also parses on SQLite, which the unit tests run it on.
_SESSIONS = """CREATE TABLE IF NOT EXISTS sessions (
    id uuid DEFAULT (gen_random_uuid()) NOT NULL,
    user_id uuid NOT NULL,
    organisation_id uuid,
    token_hash varchar NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT sessions_pkey PRIMARY KEY (id),
    CONSTRAINT sessions_user_id_fkey FOREIGN KEY (user_id) REFERENCES profiles (id),
    CONSTRAINT sessions_organisation_id_fkey FOREIGN KEY (organisation_id) REFERENCES organisations (id)
)"""


def upgrade() -> None:
    op.execute(_SESSIONS)
    op.execute(
        "INSERT INTO tokens (id, user_id, kind, token_prefix, token_hash, expires_at, created_at) "
        "SELECT id, user_id, 'session', '', token_hash, expires_at, created_at FROM sessions "
        "WHERE expires_at > CURRENT_TIMESTAMP"
    )
    op.execute("DROP TABLE sessions")


def downgrade() -> None:
    op.execute(_SESSIONS)
    op.execute("CREATE INDEX IF NOT EXISTS ix_sessions_user_id ON sessions (user_id)")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS ix_sessions_token_hash ON sessions (token_hash)")
    op.execute(
        "INSERT INTO sessions (id, user_id, token_hash, expires_at, created_at) "
        "SELECT id, user_id, token_hash, expires_at, created_at FROM tokens "
        "WHERE kind = 'session' AND revoked_at IS NULL AND expires_at > CURRENT_TIMESTAMP"
    )
    op.execute("DELETE FROM tokens WHERE kind = 'session'")
