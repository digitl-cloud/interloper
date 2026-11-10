"""A conversation records the OAuth client that started it: ``conversations.client_id``.

A client calling the agent API with a user's access token starts conversations
that belong to its use of that user's access, so they are told apart from the
app's own, which the app's list leaves out. The column is nullable: every
existing conversation, and every one the app or a personal token starts, has
no client. It references ``oauth_clients`` with no cascade, so a client with
conversations cannot be deleted before they are.

On a fresh database ``create_all`` has already created the column and its
index with the table, so the upgrade adds only what the table lacks.

The downgrade deletes the conversations a client started before it drops the
column: without it they would read as the app's own and show in its list. The
column is dropped in batch mode, which recreates the table on SQLite, the unit
tests' database, where a column a foreign key names cannot be dropped in place.

Revision ID: 021
Revises: 020
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "021"
down_revision: str | None = "020"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if "client_id" not in {column["name"] for column in inspector.get_columns("conversations")}:
        op.execute("ALTER TABLE conversations ADD COLUMN client_id varchar REFERENCES oauth_clients (client_id)")
    if "ix_conversations_client_updated" not in {index["name"] for index in inspector.get_indexes("conversations")}:
        op.execute("CREATE INDEX ix_conversations_client_updated ON conversations (client_id, updated_at)")


def downgrade() -> None:
    op.execute("DELETE FROM conversations WHERE client_id IS NOT NULL")
    op.execute("DROP INDEX IF EXISTS ix_conversations_client_updated")
    with op.batch_alter_table("conversations") as batch:
        batch.drop_column("client_id")
