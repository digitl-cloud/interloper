"""The backfill span quota is counted in partitions, not days.

``max_backfill_days`` becomes ``max_backfill_partitions``, finishing the
vocabulary change that made a job's trailing window a partition count. The
number it bounds is unchanged: at daily granularity, the only one an asset may
declare, partitions and days coincide.

Per-organisation overrides live in ``quotas.key``, so the rename is a data
change. The new key cannot already exist (nothing wrote it before this
release), which is why a plain UPDATE is safe. Idempotent: only rows still
carrying the old key are touched.

Revision ID: 011
Revises: 010
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "011"
down_revision: str | None = "010"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    bind = op.get_bind()
    if not sa.inspect(bind).has_table("quotas"):
        return

    op.execute(
        sa.text("UPDATE quotas SET key = 'max_backfill_partitions' WHERE key = 'max_backfill_days'")
    )


def downgrade() -> None:
    bind = op.get_bind()
    if not sa.inspect(bind).has_table("quotas"):
        return

    op.execute(
        sa.text("UPDATE quotas SET key = 'max_backfill_days' WHERE key = 'max_backfill_partitions'")
    )
