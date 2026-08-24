"""Whether a job is partitioned is derived from its targets, not stored.

``partitioned`` was a denormalized copy of what the target assets' catalog
definitions already declare, able to drift from them. The scheduler now
derives it (no partitioned target means a single unwindowed run), so the key
is stripped from persisted job configs. Job configs are plain JSONB (only
secret kinds encrypt), which is what makes this a SQL update.

Idempotent: ``jsonb - key`` is a no-op once the key is gone. No downgrade
data restoration: the value is recomputable from the targets, and pre-013
code treats a missing key as unpartitioned.

Revision ID: 013
Revises: 012
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "013"
down_revision: str | None = "012"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    bind = op.get_bind()
    if not sa.inspect(bind).has_table("components"):
        return

    op.execute(
        sa.text("UPDATE components SET config = config - 'partitioned' WHERE kind = 'job' AND config ? 'partitioned'")
    )


def downgrade() -> None:
    pass
