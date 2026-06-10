"""Add level column to events table.

Stores the log level (DEBUG/INFO/WARNING/ERROR) carried by ``log``
events so the UI can display it; NULL for all other event types.

Revision ID: 008
Revises: 007
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "008"
down_revision: str | None = "007"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Add the level column to the events table."""
    op.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS level TEXT;")


def downgrade() -> None:
    """Remove the level column from the events table."""
    op.execute("ALTER TABLE events DROP COLUMN IF EXISTS level;")
