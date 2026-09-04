"""Component instances and the typed relations between them."""

from datetime import datetime
from typing import Any, ClassVar, Optional
from uuid import UUID, uuid4

import interloper as il
from sqlalchemy import CheckConstraint, ForeignKey, ForeignKeyConstraint, Index, UniqueConstraint
from sqlmodel import Column, LargeBinary, Relationship, Session, SQLModel, text
from sqlmodel import Field as SQLField

from interloper_db.models.columns import PortableJSON, timestamp_column


class Component(SQLModel, table=True):
    """A persisted component instance of any kind.

    ``kind``/``key`` mirror the framework class identity; ``parent_id`` models
    ownership (asset → source, cascading on delete). ``config`` holds the
    spec, ``state`` holds operator-written runtime state, ``data`` holds the
    encrypted payload of secret-bearing kinds.

    Two persistence details are load-bearing. ``id`` carries a Python-side
    default on top of ``gen_random_uuid()`` so the store can wire relations to
    a component before flush, and so inserts work on the SQLite test
    databases. And ``children`` uses ``passive_deletes="all"`` rather than
    ``True``: deletion is owned by the DB (``parent_id`` is ``ON DELETE
    CASCADE``), and ``True`` still nulls ``parent_id`` on children that happen
    to be loaded in the deleting session, detaching them from the cascade and
    leaving orphaned asset rows.
    """

    __tablename__: ClassVar[str] = "components"
    __table_args__: ClassVar[tuple[Any, ...]] = (
        UniqueConstraint("id", "org_id", "kind", name="uq_components_id_org_kind"),
        Index("ix_components_org_id_kind", "org_id", "kind"),
        CheckConstraint("parent_id IS NULL OR kind = 'asset'", name="ck_components_parent_kind"),
        # Snapshot of the sensitive kinds (see interloper.KINDS) — a new
        # sensitive kind needs this CHECK widened in a migration.
        CheckConstraint(
            "data IS NULL OR kind IN ('connection', 'config', 'resource')",
            name="ck_components_data_kind",
        ),
    )

    id: UUID = SQLField(
        default_factory=uuid4,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    org_id: UUID
    kind: str
    key: str = SQLField(index=True)
    name: str | None = None
    parent_id: UUID | None = SQLField(
        default=None,
        sa_column=Column(ForeignKey("components.id", ondelete="CASCADE"), index=True, nullable=True),
    )
    config: dict[str, Any] | None = SQLField(default=None, sa_column=Column(PortableJSON))
    state: dict[str, Any] | None = SQLField(default=None, sa_column=Column(PortableJSON))
    data: bytes | None = SQLField(default=None, sa_column=Column(LargeBinary))
    encrypted: bool = False
    created_at: datetime | None = timestamp_column()
    updated_at: datetime | None = timestamp_column(onupdate=text("CURRENT_TIMESTAMP"))

    # Spelled Optional[...] rather than "Component" | None: SQLModel cannot
    # resolve the union string form at mapper-configuration time.
    parent: Optional["Component"] = Relationship(
        back_populates="children",
        sa_relationship_kwargs={"remote_side": "Component.id"},
    )
    children: list["Component"] = Relationship(
        back_populates="parent",
        sa_relationship_kwargs={"passive_deletes": "all"},
    )
    out_relations: list["ComponentRelation"] = Relationship(
        sa_relationship_kwargs={
            "primaryjoin": "Component.id == foreign(ComponentRelation.src_id)",
            "viewonly": True,
        },
    )
    in_relations: list["ComponentRelation"] = Relationship(
        sa_relationship_kwargs={
            "primaryjoin": "Component.id == foreign(ComponentRelation.dst_id)",
            "viewonly": True,
        },
    )

    def parent_key(self, session: Session) -> str | None:
        """The owning source's catalog key, for a source-owned asset.

        Assets owned by a source are not flat catalog entries: resolving one
        needs the key of the source that declares it, which lives a row away.

        Args:
            session: Open session the parent row is read through.

        Returns:
            The parent's catalog key, or ``None`` for a row with no parent.
        """
        if self.parent_id is None:
            return None
        parent = session.get(Component, self.parent_id)
        return parent.key if parent else None

    def stamp_state(self, **fields: Any) -> None:
        """Merge machine-owned state fields onto a component row (spec untouched).

        Datetimes are written in the canonical timezone-aware ISO form (so
        lexicographic comparison in SQL stays chronological); the merged payload
        is validated against the kind's ``state_model`` — shape only, stored
        strings are never rewritten. The caller owns the session and commit.

        Args:
            self: The row to merge the state onto.
            **fields: State fields to set, merged over the existing payload.
                Datetime values are stored as timezone-aware ISO strings.
        """
        import datetime as dt

        state = dict(self.state or {})
        for key, value in fields.items():
            state[key] = value.isoformat() if isinstance(value, dt.datetime) else value
        model = il.KINDS[self.kind].state_model
        if model is not None:
            model.model_validate(state)
        self.state = state


class ComponentRelation(SQLModel, table=True):
    """A typed, directed relation between two components.

    ``type`` names the relation; ``slot`` disambiguates multiple relations of
    the same type on one source component (a resource slot name, a dependency
    parameter name — empty when the relation has no slot semantics).

    Checks constrain the types the schema knows and permit any type they
    don't, so new relation types need no schema change.
    """

    __tablename__: ClassVar[str] = "component_relations"
    __table_args__: ClassVar[tuple[Any, ...]] = (
        ForeignKeyConstraint(
            ["src_id", "org_id", "src_kind"],
            ["components.id", "components.org_id", "components.kind"],
            ondelete="CASCADE",
            name="fk_component_relations_src",
        ),
        ForeignKeyConstraint(
            ["dst_id", "org_id", "dst_kind"],
            ["components.id", "components.org_id", "components.kind"],
            ondelete="CASCADE",
            name="fk_component_relations_dst",
        ),
        # Relation shapes (which types a kind may declare, which kinds they may
        # point at, slotted or not) are enforced by the store from the class
        # vocabulary — an open set, so it is deliberately not mirrored in CHECKs.
        Index(
            "uq_component_relations_slot",
            "src_id",
            "type",
            "slot",
            unique=True,
            postgresql_where=text("type IN ('resource', 'dependency')"),
            sqlite_where=text("type IN ('resource', 'dependency')"),
        ),
        Index("ix_component_relations_org_id_type", "org_id", "type"),
        Index("ix_component_relations_dst_id_type", "dst_id", "type"),
    )

    src_id: UUID = SQLField(primary_key=True)
    type: str = SQLField(primary_key=True)
    slot: str = SQLField(default="", primary_key=True)
    dst_id: UUID = SQLField(primary_key=True)
    org_id: UUID
    src_kind: str
    dst_kind: str

    dst: Component = Relationship(
        sa_relationship_kwargs={
            "primaryjoin": "foreign(ComponentRelation.dst_id) == Component.id",
            "viewonly": True,
        },
    )
