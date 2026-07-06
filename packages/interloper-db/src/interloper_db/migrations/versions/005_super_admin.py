"""Add is_super_admin flag to profiles.

Grants a profile platform-wide super-admin privileges (cross-org management).
The first super-admin is bootstrapped via ``INTERLOPER_AUTH_SUPER_ADMIN_EMAILS``
(promoted on login), or manually::

    UPDATE profiles SET is_super_admin = true WHERE email = '<you>';

Revision ID: 005
Revises: 004
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "005"
down_revision: str | None = "004"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Add the is_super_admin column to the profiles table."""
    op.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS is_super_admin BOOLEAN NOT NULL DEFAULT false;")


def downgrade() -> None:
    """Remove the is_super_admin column from the profiles table."""
    op.execute("ALTER TABLE profiles DROP COLUMN IF EXISTS is_super_admin;")
