"""Drop target relations from webhook hooks.

The ``target`` verb moved from the ``Hook`` anchor to ``TriggerHook`` —
only trigger-style hooks act on other components. ``WebhookHook`` rows
could previously accumulate ``target`` relations (the wizard offered the
step, the store accepted them, nothing ever consulted them); with the
vocabulary now class-level, such rows would fail hydration closed, so the
meaningless relations are removed.

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
    op.execute(
        """
        DELETE FROM component_relations
        WHERE type = 'target'
          AND src_id IN (SELECT id FROM components WHERE key = 'webhook_hook')
        """
    )


def downgrade() -> None:
    # The relations were meaningless; there is nothing to restore.
    pass
