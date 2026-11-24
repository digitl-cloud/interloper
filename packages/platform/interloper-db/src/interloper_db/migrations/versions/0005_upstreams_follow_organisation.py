"""Drop every asset-to-asset row: an upstream relation follows the organisation unless curated.

A row in ``component_relations`` is now a curation. An asset's upstream
relation holding none is resolved when read, to every asset of the
organisation it matches, siblings included. The rows written before were
either sibling edges the store derived itself, or cross-source bindings meant
as "every account": kept, each would freeze its relation as a curation and
an account added later would never join.

One-way: the downgrade restores nothing, since what the rows held is exactly
what the relations now resolve to. A release carrying this revision is a
no-rollback point: the code before it reads every upstream relation as
unbound.

Revision ID: 0005
Revises: 0004
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute("DELETE FROM component_relations WHERE src_kind = 'asset' AND dst_kind = 'asset'")


def downgrade() -> None:
    pass
