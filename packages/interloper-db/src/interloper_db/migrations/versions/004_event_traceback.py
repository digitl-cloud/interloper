"""Add traceback column to events table.

Stores the formatted Python traceback alongside the error message so
that failure diagnostics are available in the UI without parsing logs.

Revision ID: 004
Revises: 003
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "004"
down_revision: str | None = "003"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Add the traceback column to the events table."""
    op.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS traceback TEXT;")


def downgrade() -> None:
    """Remove the traceback column from the events table."""
    op.execute("ALTER TABLE events DROP COLUMN IF EXISTS traceback;")
