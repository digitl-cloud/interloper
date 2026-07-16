"""Personal access tokens for programmatic access (MCP, CLI).

The table is normally provisioned by ``create_all()`` (it is a plain
SQLModel table); this migration exists so upgrade-only paths get it too.
Idempotent: a no-op when ``create_all`` has already created the table.

Revision ID: 004
Revises: 003
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "004"
down_revision: str | None = "003"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    bind = op.get_bind()
    if sa.inspect(bind).has_table("personal_access_tokens"):
        return

    op.create_table(
        "personal_access_tokens",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("user_id", sa.Uuid(), sa.ForeignKey("profiles.id"), nullable=False),
        sa.Column("organisation_id", sa.Uuid(), sa.ForeignKey("organisations.id"), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("token_prefix", sa.String(), nullable=False),
        sa.Column("token_hash", sa.String(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.create_index(
        "ix_personal_access_tokens_user_id", "personal_access_tokens", ["user_id"]
    )
    op.create_index(
        "ix_personal_access_tokens_organisation_id", "personal_access_tokens", ["organisation_id"]
    )
    op.create_index(
        "ix_personal_access_tokens_token_hash", "personal_access_tokens", ["token_hash"], unique=True
    )


def downgrade() -> None:
    op.drop_table("personal_access_tokens")
