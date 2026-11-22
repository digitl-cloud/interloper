"""Enable the pgcrypto extension, which gen_random_uuid() server defaults rely on.

The tables come from the models (``SQLModel.metadata.create_all``, see
:mod:`interloper_db.provision`); this chain adds only what a model cannot
say. It starts here: the migrations up to the old revision 026 were folded
into these first three, and a database at 026 is stamped at 0003, which has
the same schema.

Revision ID: 0001
Revises: None
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")


def downgrade() -> None:
    op.execute("DROP EXTENSION IF EXISTS pgcrypto")
