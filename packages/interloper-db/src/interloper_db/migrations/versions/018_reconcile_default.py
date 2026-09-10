"""Retire the destination-level materialization strategy and the ``auto`` value.

Conform runs once, on the asset: destinations no longer carry a
``materialization_strategy``, and a stored key on a destination row would
surface as drift at hydration (unknown config keys are a loud error). The
``auto`` strategy became ``reconcile``, which is what it did with a declared
schema since 0.49.0; the framework still reads ``auto`` as an alias, so the
rewrite below only keeps stored configs spelling the value that exists.

The downgrade is a no-op: ``auto`` and ``reconcile`` behaved the same, and a
destination's retired value is not recoverable.

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


def upgrade() -> None:
    op.execute(
        "UPDATE components SET config = config - 'materialization_strategy' "
        "WHERE kind = 'destination' AND config ? 'materialization_strategy'"
    )
    op.execute(
        "UPDATE components SET config = jsonb_set(config, '{materialization_strategy}', '\"reconcile\"') "
        "WHERE config->>'materialization_strategy' = 'auto'"
    )


def downgrade() -> None:
    pass
