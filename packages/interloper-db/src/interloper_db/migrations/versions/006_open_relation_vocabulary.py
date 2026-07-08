"""Drop the per-type relation CHECK constraints.

The four CHECKs pinned each relation type to the kinds it pointed at when
the schema was written — a closed-world snapshot of what is now an open
vocabulary (class-declared ``relation_types``, e.g. the hook kind reuses
``target``). The store enforces the vocabulary's shape at write time, so
the snapshot is redundant where it agrees and wrong where it doesn't.

Revision ID: 006
Revises: 005
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "006"
down_revision: str | None = "005"
branch_labels: str | None = None
depends_on: str | None = None

_CONSTRAINTS = (
    "ck_component_relations_dependency",
    "ck_component_relations_resource",
    "ck_component_relations_destination",
    "ck_component_relations_target",
)


def upgrade() -> None:
    """Drop the closed-world relation CHECKs."""
    for name in _CONSTRAINTS:
        op.execute(f"ALTER TABLE component_relations DROP CONSTRAINT IF EXISTS {name}")


def downgrade() -> None:
    """No-op: the constraints encode a closed world the vocabulary has outgrown."""
