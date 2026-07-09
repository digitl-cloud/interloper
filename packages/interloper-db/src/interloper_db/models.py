"""SQLModel database models for interloper persistence.

The schema mirrors the framework's own model: everything is a Component,
persisted in a single ``components`` table, with typed relations in a single
``component_relations`` table. The catalog (Python class definitions) provides
the schema; the database stores instance data.

Key design decisions:
- One row per component instance; ``kind`` discriminates. New kinds need no
  schema changes.
- Three payload columns with distinct contracts: ``config`` (the spec — user
  intent, the Spec init payload), ``state`` (machine-owned runtime
  state, written only by operators via targeted updates, always safe to
  discard), and ``data`` (Fernet-encrypted secrets).
- Relations carry ``org_id``/``src_kind``/``dst_kind`` denormalized but drift-proof:
  composite foreign keys onto ``UNIQUE (id, org_id, kind)`` force them to match
  the referenced rows, giving DB-level kind- and tenant-safety.
- Auth tables (profiles, organisations, sessions) live alongside data
  models so that ``create_all()`` provisions everything in one shot.
"""

import datetime as dt
from datetime import datetime
from typing import Any, ClassVar, Optional
from uuid import UUID, uuid4

from sqlalchemy import JSON, CheckConstraint, DateTime, ForeignKey, ForeignKeyConstraint, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Column, LargeBinary, Relationship, SQLModel, text
from sqlmodel import Field as SQLField

# All datetime columns use TIMESTAMPTZ so SQLAlchemy returns timezone-aware values.
TZDateTime = DateTime(timezone=True)

# JSONB on Postgres; plain JSON elsewhere (in-memory SQLite test databases).
PortableJSON = JSON().with_variant(JSONB(), "postgresql")


def _ts(**kwargs: Any) -> Any:
    """Shorthand for a nullable TIMESTAMPTZ column defaulting to the insert time.

    ``CURRENT_TIMESTAMP`` rather than ``now()`` so the tables also work on the
    in-memory SQLite databases the tests use (identical semantics on Postgres).
    """
    return SQLField(default=None, sa_column=Column(TZDateTime, server_default=text("CURRENT_TIMESTAMP"), **kwargs))


# -- Auth & Organisation -----------------------------------------------------


class Profile(SQLModel, table=True):
    """An authenticated user profile (Google OAuth)."""

    __tablename__: ClassVar[str] = "profiles"

    id: UUID = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    email: str
    name: str | None = None
    google_id: str = SQLField(index=True, unique=True)
    avatar_url: str | None = None
    is_super_admin: bool = SQLField(default=False, sa_column_kwargs={"server_default": text("false")})
    last_organisation_id: UUID | None = SQLField(default=None, foreign_key="organisations.id")
    created_at: datetime | None = _ts()


class Organisation(SQLModel, table=True):
    """A tenant organisation."""

    __tablename__: ClassVar[str] = "organisations"

    id: UUID = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    name: str
    created_at: datetime | None = _ts()


class UserOrganisation(SQLModel, table=True):
    """Junction: user membership in an organisation with a role."""

    __tablename__: ClassVar[str] = "user_organisations"

    user_id: UUID = SQLField(foreign_key="profiles.id", primary_key=True)
    organisation_id: UUID = SQLField(foreign_key="organisations.id", primary_key=True)
    role: str = "viewer"
    created_at: datetime | None = _ts()


class Invitation(SQLModel, table=True):
    """A pending invitation for a user to join an organisation."""

    __tablename__: ClassVar[str] = "invitations"

    id: UUID = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    organisation_id: UUID = SQLField(foreign_key="organisations.id", index=True)
    email: str
    role: str = "viewer"
    token: str = SQLField(index=True, unique=True)
    invited_by: UUID = SQLField(foreign_key="profiles.id")
    created_at: datetime | None = _ts()
    expires_at: datetime = SQLField(sa_column=Column(TZDateTime))


class Session(SQLModel, table=True):
    """A cookie-based user session with optional org context."""

    __tablename__: ClassVar[str] = "sessions"

    id: UUID = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    user_id: UUID = SQLField(foreign_key="profiles.id", index=True)
    organisation_id: UUID | None = SQLField(default=None, foreign_key="organisations.id")
    token_hash: str = SQLField(index=True, unique=True)
    expires_at: datetime = SQLField(sa_column=Column(TZDateTime))
    created_at: datetime | None = _ts()


# -- Components ----------------------------------------------------------------


class Component(SQLModel, table=True):
    """A persisted component instance of any kind.

    ``kind``/``key`` mirror the framework class identity; ``parent_id`` models
    ownership (asset → source, cascading on delete). ``config`` holds the
    spec, ``state`` holds operator-written runtime state, ``data`` holds the
    encrypted payload of secret-bearing kinds.
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

    # Python-side default (not just gen_random_uuid()) so the store can wire
    # relations to a component before flush, and inserts work on SQLite tests.
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
    created_at: datetime | None = _ts()
    updated_at: datetime | None = _ts()

    parent: Optional["Component"] = Relationship(  # noqa: UP045 — SQLModel can't resolve the union string form
        back_populates="children",
        sa_relationship_kwargs={"remote_side": "Component.id"},
    )
    children: list["Component"] = Relationship(
        back_populates="parent",
        sa_relationship_kwargs={"passive_deletes": True},
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


# -- Scheduling ----------------------------------------------------------------


class Backfill(SQLModel, table=True):
    """A backfill spanning a date range with multiple runs."""

    __tablename__: ClassVar[str] = "backfills"

    id: UUID = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    component_id: UUID | None = SQLField(
        default=None,
        sa_column=Column(ForeignKey("components.id", ondelete="SET NULL"), index=True),
    )
    org_id: UUID = SQLField(index=True)
    status: str = "queued"
    start_date: dt.date
    end_date: dt.date
    concurrency: int = 1
    fail_fast: bool = False
    partitions: int = 0
    started_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    completed_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    created_at: datetime | None = _ts()

    runs: list["Run"] = Relationship(back_populates="backfill")


# -- Runs & Events ------------------------------------------------------------


class Run(SQLModel, table=True):
    """A single materialization attempt."""

    __tablename__: ClassVar[str] = "runs"
    __table_args__: ClassVar[tuple[Any, ...]] = (
        Index("ix_runs_org_id_created_at", "org_id", "created_at"),
        Index("ix_runs_backfill_id_status", "backfill_id", "status"),
    )

    id: UUID = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    component_id: UUID | None = SQLField(
        default=None,
        sa_column=Column(ForeignKey("components.id", ondelete="SET NULL"), index=True),
    )
    org_id: UUID
    backfill_id: UUID | None = SQLField(default=None, foreign_key="backfills.id")
    partition_date: dt.date | None = None
    status: str = "queued"
    retry_of: UUID | None = SQLField(
        default=None,
        sa_column=Column(ForeignKey("runs.id", ondelete="SET NULL"), index=True),
    )
    attempt: int = 1
    retry_scope: str | None = None
    started_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    completed_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    created_at: datetime | None = _ts()

    backfill: Backfill | None = Relationship(back_populates="runs")


class Event(SQLModel, table=True):
    """An execution event persisted for observability."""

    __tablename__: ClassVar[str] = "events"
    __table_args__: ClassVar[tuple[Any, ...]] = (
        Index("ix_events_run_id_timestamp", "run_id", "timestamp"),
        Index("ix_events_asset_lookup", "run_id", "asset_key", "event_type", "timestamp"),
    )

    id: UUID = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    org_id: UUID
    run_id: UUID | None = SQLField(default=None, foreign_key="runs.id")
    backfill_id: UUID | None = SQLField(default=None, foreign_key="backfills.id")
    event_type: str
    partition_or_window: str | None = None
    error: str | None = None
    traceback: str | None = None
    asset_id: UUID | None = SQLField(default=None)
    asset_key: str | None = None
    message: str | None = None
    level: str | None = None
    timestamp: datetime = SQLField(sa_column=Column(TZDateTime))
