"""Rename the asset-to-asset relation to ``upstream`` and let its slots hold several edges.

The relation type ``dependency`` named the role too loosely (every relation
is a dependency of sorts); ``upstream`` says which way the edge points and
matches the vocabulary the app, the lineage tools and the executor already
use. Persisted rows follow the rename.

A many-valued upstream slot binds every matching upstream, so the per-slot
uniqueness that made re-binding repoint an edge can no longer be a schema
rule for that type. Single-valued upstream slots keep repointing in
``RelationStore``, which knows the slot contract; resources stay unique by
schema.

Revision ID: 017
Revises: 016
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "017"
down_revision: str | None = "016"
branch_labels: str | None = None
depends_on: str | None = None

_INDEX = "uq_component_relations_slot"
_TABLE = "component_relations"
_COLUMNS = ["src_id", "type", "slot"]


def upgrade() -> None:
    op.execute("UPDATE component_relations SET type = 'upstream' WHERE type = 'dependency'")
    op.drop_index(_INDEX, table_name=_TABLE)
    op.create_index(_INDEX, _TABLE, _COLUMNS, unique=True, postgresql_where=sa.text("type = 'resource'"))


def downgrade() -> None:
    # Fails if a slot holds several upstream edges; remove the extra legs first.
    op.drop_index(_INDEX, table_name=_TABLE)
    op.create_index(
        _INDEX, _TABLE, _COLUMNS, unique=True, postgresql_where=sa.text("type IN ('resource', 'dependency')")
    )
    op.execute("UPDATE component_relations SET type = 'dependency' WHERE type = 'upstream'")
