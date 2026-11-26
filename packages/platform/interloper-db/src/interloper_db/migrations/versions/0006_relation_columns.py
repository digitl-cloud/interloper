"""Name a relation row's columns after what they hold: ``from``, ``relation`` and ``to``.

A row says that one component, under one of the relations its class
declares, points at another. ``src_id``/``src_kind`` become
``from_id``/``from_kind``, ``dst_id``/``dst_kind`` become ``to_id``/``to_kind``,
and ``name``, the attribute the relation is declared under, becomes
``relation``. The foreign keys, indexes and, from Postgres 18, the named
NOT NULL constraints are renamed with them, so a migrated table matches a
new one. Postgres renames in place, so no row is rewritten, and the realtime
trigger needs no change: it sends the row as it is.

Replicas still on the previous release fail their relation reads between this
revision and their replacement; the downgrade renames everything back.

Revision ID: 0006
Revises: 0005
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | None = None
depends_on: str | None = None

_COLUMNS = (
    ("src_id", "from_id"),
    ("dst_id", "to_id"),
    ("src_kind", "from_kind"),
    ("dst_kind", "to_kind"),
    ("name", "relation"),
)
_CONSTRAINTS = (
    ("fk_component_relations_src", "fk_component_relations_from"),
    ("fk_component_relations_dst", "fk_component_relations_to"),
)
_INDEXES = (
    ("ix_component_relations_org_id_name", "ix_component_relations_org_id_relation"),
    ("ix_component_relations_dst_id_name", "ix_component_relations_to_id_relation"),
)


def _rename_constraint(old: str, new: str) -> None:
    op.execute(
        f"""DO $$ BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_constraint WHERE conrelid = 'component_relations'::regclass AND conname = '{old}'
            ) THEN
                ALTER TABLE component_relations RENAME CONSTRAINT {old} TO {new};
            END IF;
        END $$"""
    )


def _rename(columns: tuple[tuple[str, str], ...]) -> None:
    present = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("component_relations")}
    for old, new in columns:
        if old in present:
            op.alter_column("component_relations", old, new_column_name=new)
            _rename_constraint(f"component_relations_{old}_not_null", f"component_relations_{new}_not_null")


def upgrade() -> None:
    # A table created at this head already carries the new names, so each rename runs only where the old one is left.
    _rename(_COLUMNS)
    for old, new in _CONSTRAINTS:
        _rename_constraint(old, new)
    for old, new in _INDEXES:
        op.execute(f"ALTER INDEX IF EXISTS {old} RENAME TO {new}")


def downgrade() -> None:
    for old, new in _INDEXES:
        op.execute(f"ALTER INDEX IF EXISTS {new} RENAME TO {old}")
    for old, new in _CONSTRAINTS:
        _rename_constraint(new, old)
    _rename(tuple((new, old) for old, new in _COLUMNS))
