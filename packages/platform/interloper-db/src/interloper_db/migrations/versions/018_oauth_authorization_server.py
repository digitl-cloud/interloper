"""interloper is an OAuth authorization server: clients, authorization codes and refresh tokens.

Three new tables, owned by the authorization server: ``oauth_clients``,
``oauth_authorization_codes`` and ``oauth_refresh_tokens``. Access tokens
need no table of their own: they are ``tokens`` rows of kind ``access``,
whose ``client_id`` and ``resource`` migration 015 already added, so
``tokens`` is left as it is.

On a fresh or existing database ``create_all`` has already created the
tables from the models before the upgrade runs, so every statement is
idempotent; the statements create them in the models' shape for a database
upgraded without ``create_all``.

Downgrading drops the three tables and deletes the access tokens, which
nothing before this revision issues.

Revision ID: 018
Revises: 017
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "018"
down_revision: str | None = "017"
branch_labels: str | None = None
depends_on: str | None = None

_CLIENTS = """CREATE TABLE IF NOT EXISTS oauth_clients (
    client_id varchar NOT NULL,
    secret_hash varchar,
    name varchar NOT NULL,
    redirect_uris jsonb NOT NULL,
    registered_by varchar NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT oauth_clients_pkey PRIMARY KEY (client_id)
)"""

_CODES = """CREATE TABLE IF NOT EXISTS oauth_authorization_codes (
    code_hash varchar NOT NULL,
    family_id uuid NOT NULL,
    client_id varchar NOT NULL,
    user_id uuid NOT NULL,
    redirect_uri varchar NOT NULL,
    code_challenge varchar NOT NULL,
    resource varchar NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    used_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT oauth_authorization_codes_pkey PRIMARY KEY (code_hash),
    CONSTRAINT oauth_authorization_codes_client_id_fkey FOREIGN KEY (client_id) REFERENCES oauth_clients (client_id),
    CONSTRAINT oauth_authorization_codes_user_id_fkey FOREIGN KEY (user_id) REFERENCES profiles (id)
)"""

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


def upgrade() -> None:
    op.execute(_CLIENTS)
    op.execute(_CODES)
    op.execute(_REFRESH_TOKENS)
    op.execute("CREATE INDEX IF NOT EXISTS ix_oauth_authorization_codes_user_id ON oauth_authorization_codes (user_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_oauth_refresh_tokens_family_id ON oauth_refresh_tokens (family_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_oauth_refresh_tokens_user_id ON oauth_refresh_tokens (user_id)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS oauth_refresh_tokens")
    op.execute("DROP TABLE IF EXISTS oauth_authorization_codes")
    op.execute("DROP TABLE IF EXISTS oauth_clients")
    op.execute("DELETE FROM tokens WHERE kind = 'access'")
