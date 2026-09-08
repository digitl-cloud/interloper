"""Key component relations by name.

A relation row used to carry a ``type`` (the vocabulary entry) and a ``slot``
(empty for list-shaped types). The framework now declares one named
``Relation`` per link, so a row is ``(src_id, name, dst_id)``: ``name`` is the
slot for slotted rows and the plural field name for the others. Whether a
relation is single-valued is a class rule the store enforces, so the partial
unique index on resource slots goes.

The types the old vocabulary ever wrote (``resource``, ``dependency``/
``upstream``, and the three ``_PLURAL`` maps: ``destination``, ``target``,
``watch``) are backfilled into ``name`` below. The final ``DELETE`` is
defensive only: it drops a row left with a NULL ``name`` (a ``type`` outside
that vocabulary) or an empty one (a ``resource`` row whose ``slot`` was
never filled, which the store had no path to write). Production holds
neither, so it is a no-op there.

The downgrade maps every asset-kind row back to ``type = 'upstream'``, so a
pre-017 ``dependency`` type is never recovered: that rename never shipped, so
no row in the wild carries it. In upgrade(), ``_PLURAL`` entries match by
``type``; in downgrade(), they match by ``name``.

``upgrade`` is a no-op on a database whose ``component_relations`` table has
no ``type`` column: ``create_all()`` always provisions the table from the
current model, so a fresh database already has the final (``name``) shape
before Alembic runs at all.

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

_TABLE = "component_relations"
_PLURAL = {"destination": "destinations", "target": "targets", "watch": "watches"}


def upgrade() -> None:
    bind = op.get_bind()
    columns = {column["name"] for column in sa.inspect(bind).get_columns(_TABLE)}
    if "type" not in columns:
        # Fresh database: create_all() already produced the final (name) shape.
        return

    op.add_column(_TABLE, sa.Column("name", sa.String(), nullable=True))
    op.execute("UPDATE component_relations SET name = slot WHERE type IN ('resource', 'dependency', 'upstream')")
    for type_, name in _PLURAL.items():
        op.execute(f"UPDATE component_relations SET name = '{name}' WHERE type = '{type_}'")
    op.execute("DELETE FROM component_relations WHERE name IS NULL OR name = ''")
    op.alter_column(_TABLE, "name", nullable=False)
    op.drop_index("uq_component_relations_slot", table_name=_TABLE)
    op.drop_index("ix_component_relations_org_id_type", table_name=_TABLE)
    op.drop_index("ix_component_relations_dst_id_type", table_name=_TABLE)
    op.drop_constraint("component_relations_pkey", _TABLE, type_="primary")
    op.drop_column(_TABLE, "type")
    op.drop_column(_TABLE, "slot")
    op.create_primary_key("component_relations_pkey", _TABLE, ["src_id", "name", "dst_id"])
    op.create_index("ix_component_relations_org_id_name", _TABLE, ["org_id", "name"])
    op.create_index("ix_component_relations_dst_id_name", _TABLE, ["dst_id", "name"])


def downgrade() -> None:
    op.add_column(_TABLE, sa.Column("type", sa.String(), nullable=True))
    op.add_column(_TABLE, sa.Column("slot", sa.String(), nullable=True, server_default=""))
    for type_, name in _PLURAL.items():
        op.execute(f"UPDATE component_relations SET type = '{type_}' WHERE name = '{name}'")
    op.execute(
        "UPDATE component_relations SET type = 'upstream', slot = name WHERE type IS NULL AND dst_kind = 'asset'"
    )
    op.execute("UPDATE component_relations SET type = 'resource', slot = name WHERE type IS NULL")
    op.alter_column(_TABLE, "type", nullable=False)
    op.drop_index("ix_component_relations_org_id_name", table_name=_TABLE)
    op.drop_index("ix_component_relations_dst_id_name", table_name=_TABLE)
    op.drop_constraint("component_relations_pkey", _TABLE, type_="primary")
    op.drop_column(_TABLE, "name")
    op.create_primary_key("component_relations_pkey", _TABLE, ["src_id", "type", "slot", "dst_id"])
    op.create_index(
        "uq_component_relations_slot",
        _TABLE,
        ["src_id", "type", "slot"],
        unique=True,
        postgresql_where=sa.text("type = 'resource'"),
    )
    op.create_index("ix_component_relations_org_id_type", _TABLE, ["org_id", "type"])
    op.create_index("ix_component_relations_dst_id_type", _TABLE, ["dst_id", "type"])
