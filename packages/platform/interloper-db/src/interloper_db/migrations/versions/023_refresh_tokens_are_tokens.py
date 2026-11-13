"""A refresh token is a token: ``oauth_refresh_tokens`` folds into ``tokens`` as kind ``refresh``.

``tokens`` holds every bearer secret that stands for a user, one row per
secret, and a refresh token was the one kept apart. ``tokens`` gains the two
columns only a refresh token fills, ``family_id`` (indexed: a family is
revoked and purged whole) and ``rotated_at``, and every refresh token moves
with its hash, family and state, so no client has to be approved again.
Rotated and revoked tokens move too, since replaying one must still revoke
its family.

``tokens`` is this migration's (``create_all`` skips it), so the columns are
added here; the unit tests create it from the model, with them already, so
the upgrade adds only what the table lacks.

The table is dropped in the same step rather than one release later: a pod
still on the previous release fails a refresh during the rollout without
rotating the token, and the client's next attempt reaches an upgraded pod.

Downgrading recreates ``oauth_refresh_tokens`` in its 018 shape, moves the
refresh tokens back (those whose client still exists, as its foreign key
requires), deletes them from ``tokens`` and drops the two columns.

Revision ID: 023
Revises: 022
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "023"
down_revision: str | None = "022"
branch_labels: str | None = None
depends_on: str | None = None

_REFRESH_TOKENS = """CREATE TABLE IF NOT EXISTS oauth_refresh_tokens (
    token_hash varchar NOT NULL,
    family_id uuid NOT NULL,
    client_id varchar NOT NULL,
    user_id uuid NOT NULL,
    resource varchar NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    rotated_at timestamp with time zone,
    revoked_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT oauth_refresh_tokens_pkey PRIMARY KEY (token_hash),
    CONSTRAINT oauth_refresh_tokens_client_id_fkey FOREIGN KEY (client_id) REFERENCES oauth_clients (client_id),
    CONSTRAINT oauth_refresh_tokens_user_id_fkey FOREIGN KEY (user_id) REFERENCES profiles (id)
)"""

_MOVED_COLUMNS = "token_hash, family_id, client_id, user_id, resource, expires_at, rotated_at, revoked_at, created_at"


def upgrade() -> None:
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("tokens")}
    if "family_id" not in columns:
        op.execute("ALTER TABLE tokens ADD COLUMN family_id uuid")
    if "rotated_at" not in columns:
        op.execute("ALTER TABLE tokens ADD COLUMN rotated_at timestamp with time zone")
    op.execute("CREATE INDEX IF NOT EXISTS ix_tokens_family_id ON tokens (family_id)")
    op.execute(
        f"INSERT INTO tokens (kind, token_prefix, {_MOVED_COLUMNS}) "
        f"SELECT 'refresh', '', {_MOVED_COLUMNS} FROM oauth_refresh_tokens"
    )
    op.execute("DROP TABLE oauth_refresh_tokens")


def downgrade() -> None:
    op.execute(_REFRESH_TOKENS)
    op.execute("CREATE INDEX IF NOT EXISTS ix_oauth_refresh_tokens_family_id ON oauth_refresh_tokens (family_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_oauth_refresh_tokens_user_id ON oauth_refresh_tokens (user_id)")
    op.execute(
        f"INSERT INTO oauth_refresh_tokens ({_MOVED_COLUMNS}) "
        f"SELECT {_MOVED_COLUMNS} FROM tokens "
        "WHERE kind = 'refresh' AND client_id IN (SELECT client_id FROM oauth_clients)"
    )
    op.execute("DELETE FROM tokens WHERE kind = 'refresh'")
    op.execute("DROP INDEX IF EXISTS ix_tokens_family_id")
    op.execute("ALTER TABLE tokens DROP COLUMN family_id")
    op.execute("ALTER TABLE tokens DROP COLUMN rotated_at")
