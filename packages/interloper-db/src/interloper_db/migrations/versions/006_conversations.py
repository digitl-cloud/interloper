"""Add agent conversations.

One row per conversation a member has with the agent, its message history in
a JSONB column. The DDL is idempotent because ``create_all`` creates the
table from the model and *then* runs the chain: on a fresh database it
already exists by the time this runs, and on an existing one it does not.

Revision ID: 006
Revises: 005
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "006"
down_revision: str | None = "005"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS conversations (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id uuid NOT NULL REFERENCES organisations(id),
            user_id uuid NOT NULL REFERENCES profiles(id),
            title varchar,
            messages jsonb NOT NULL,
            created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
            updated_at timestamptz DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_conversations_owner_updated ON conversations (org_id, user_id, updated_at)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS conversations")
