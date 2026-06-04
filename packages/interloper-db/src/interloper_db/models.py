"""SQLModel database models for interloper persistence.

The schema is designed around the principle that the framework's catalog
(Python class definitions) provides the schema, and the database stores
user-provided instance data.

Key design decisions:
- Resources are first-class: typed, named, optionally encrypted.
- Sources and destinations reference resources via junction tables.
- Assets are thin rows — metadata comes from the catalog.
- Auth tables (profiles, organisations, sessions) live alongside data
  models so that ``create_all()`` provisions everything in one shot.
"""

import datetime as dt
from datetime import datetime
from typing import Any, ClassVar
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, Index
from sqlmodel import ARRAY, JSON, Column, LargeBinary, Relationship, SQLModel, String, text
from sqlmodel import Field as SQLField

# All datetime columns use TIMESTAMPTZ so SQLAlchemy returns timezone-aware values.
TZDateTime = DateTime(timezone=True)


def _ts(**kwargs: Any) -> Any:
    """Shorthand for a nullable TIMESTAMPTZ column with server_default=now()."""
    return SQLField(default=None, sa_column=Column(TZDateTime, server_default=text("now()"), **kwargs))


# -- Auth & Organisation -----------------------------------------------------


class Profile(SQLModel, table=True):
    """An authenticated user profile (Google OAuth)."""

    __tablename__: ClassVar[str] = "profiles"

    id: UUID | None = SQLField(
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

    id: UUID | None = SQLField(
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

    id: UUID | None = SQLField(
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

    id: UUID | None = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    user_id: UUID = SQLField(foreign_key="profiles.id", index=True)
    organisation_id: UUID | None = SQLField(default=None, foreign_key="organisations.id")
    token_hash: str = SQLField(index=True, unique=True)
    expires_at: datetime = SQLField(sa_column=Column(TZDateTime))
    created_at: datetime | None = _ts()


# -- Junction tables (defined first to avoid forward references) ---------------


class SourceResource(SQLModel, table=True):
    """Junction: binds a named resource slot on a source to a resource instance."""

    __tablename__: ClassVar[str] = "source_resources"

    source_id: UUID = SQLField(sa_column=Column(ForeignKey("sources.id", ondelete="CASCADE"), primary_key=True))
    resource_id: UUID = SQLField(sa_column=Column(ForeignKey("resources.id", ondelete="CASCADE"), primary_key=True))
    key: str = SQLField(primary_key=True)


class SourceDestination(SQLModel, table=True):
    """Junction: binds a source to a destination."""

    __tablename__: ClassVar[str] = "source_destinations"

    source_id: UUID = SQLField(
        sa_column=Column(ForeignKey("sources.id", ondelete="CASCADE"), primary_key=True),
    )
    destination_id: UUID = SQLField(
        sa_column=Column(ForeignKey("destinations.id", ondelete="CASCADE"), primary_key=True),
    )


class AssetResource(SQLModel, table=True):
    """Junction: binds a named resource slot on an asset to a resource instance."""

    __tablename__: ClassVar[str] = "asset_resources"

    asset_id: UUID = SQLField(
        sa_column=Column(ForeignKey("assets.id", ondelete="CASCADE"), primary_key=True),
    )
    resource_id: UUID = SQLField(
        sa_column=Column(ForeignKey("resources.id", ondelete="CASCADE"), primary_key=True),
    )
    key: str = SQLField(primary_key=True)


class AssetDestination(SQLModel, table=True):
    """Junction: binds an asset to a destination."""

    __tablename__: ClassVar[str] = "asset_destinations"

    asset_id: UUID = SQLField(
        sa_column=Column(ForeignKey("assets.id", ondelete="CASCADE"), primary_key=True),
    )
    destination_id: UUID = SQLField(
        sa_column=Column(ForeignKey("destinations.id", ondelete="CASCADE"), primary_key=True),
    )


class AssetDependency(SQLModel, table=True):
    """Dependency edge between assets (intra or cross-source).

    ``param_name`` is the function parameter on the downstream asset
    that receives the upstream asset's data at materialization time.
    """

    __tablename__: ClassVar[str] = "asset_dependencies"

    asset_id: UUID = SQLField(
        sa_column=Column(None, ForeignKey("assets.id", ondelete="CASCADE"), primary_key=True),
    )
    upstream_asset_id: UUID = SQLField(
        sa_column=Column(None, ForeignKey("assets.id", ondelete="CASCADE"), primary_key=True),
    )
    param_name: str = SQLField(primary_key=True)  # TODO: rename to `slot`


class DestinationResource(SQLModel, table=True):
    """Junction: binds a named resource slot on a destination to a resource instance."""

    __tablename__: ClassVar[str] = "destination_resources"

    destination_id: UUID = SQLField(
        sa_column=Column(ForeignKey("destinations.id", ondelete="CASCADE"), primary_key=True),
    )
    resource_id: UUID = SQLField(
        sa_column=Column(ForeignKey("resources.id", ondelete="CASCADE"), primary_key=True),
    )
    key: str = SQLField(primary_key=True)


class JobSource(SQLModel, table=True):
    """Junction: binds a job to a source."""

    __tablename__: ClassVar[str] = "job_sources"

    job_id: UUID = SQLField(sa_column=Column(ForeignKey("jobs.id", ondelete="CASCADE"), primary_key=True))
    source_id: UUID = SQLField(sa_column=Column(ForeignKey("sources.id", ondelete="CASCADE"), primary_key=True))


class JobAsset(SQLModel, table=True):
    """Junction: binds a job to a standalone asset."""

    __tablename__: ClassVar[str] = "job_assets"

    job_id: UUID = SQLField(
        sa_column=Column(ForeignKey("jobs.id", ondelete="CASCADE"), primary_key=True),
    )
    asset_id: UUID = SQLField(
        sa_column=Column(ForeignKey("assets.id", ondelete="CASCADE"), primary_key=True),
    )


# -- Resources ----------------------------------------------------------------


class Resource(SQLModel, table=True):
    """A typed, named resource instance (config, connection, credentials).

    The ``kind`` field is the functional category (e.g. ``"connection"``,
    ``"config"``). The ``key`` field is the catalog key that identifies
    the resource class. When ``encrypted`` is True, the ``data`` column
    contains the encrypted blob of the entire JSON config.
    """

    __tablename__: ClassVar[str] = "resources"

    id: UUID | None = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    org_id: UUID = SQLField(index=True)
    kind: str
    key: str = SQLField(index=True)
    name: str
    data: bytes | None = SQLField(default=None, sa_column=Column(LargeBinary))
    encrypted: bool = False
    created_at: datetime | None = _ts()
    updated_at: datetime | None = _ts()


# -- Sources ------------------------------------------------------------------


class Source(SQLModel, table=True):
    """A registered source instance.

    User decisions are stored directly on the row:
    - ``config`` — source config field values (account_id, etc.)

    Resource and destination bindings are stored via junction tables
    (``source_resources`` and ``source_destinations``).
    Asset enablement is controlled per-asset via ``Asset.materializable``.
    """

    __tablename__: ClassVar[str] = "sources"

    id: UUID | None = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    org_id: UUID = SQLField(index=True)
    key: str = SQLField(index=True)
    name: str
    config: dict[str, Any] | None = SQLField(default=None, sa_column=Column(JSON))
    created_at: datetime | None = _ts()
    updated_at: datetime | None = _ts()

    assets: list["Asset"] = Relationship(
        back_populates="source",
        sa_relationship_kwargs={"passive_deletes": True},
    )
    jobs: list["Job"] = Relationship(
        back_populates="sources",
        link_model=JobSource,
        sa_relationship_kwargs={"passive_deletes": True},
    )
    resources: list["Resource"] = Relationship(
        link_model=SourceResource,
        sa_relationship_kwargs={"viewonly": True},
    )
    destinations: list["Destination"] = Relationship(
        link_model=SourceDestination,
        sa_relationship_kwargs={"viewonly": True},
    )


# -- Assets -------------------------------------------------------------------


class Asset(SQLModel, table=True):
    """The primary data entity — represents a single materializable dataset.

    Assets may belong to a source (grouped, with inherited config/resources)
    or be standalone (source_id is None, with their own bindings).
    Per-asset config overrides are stored in ``config`` and take precedence
    over source-level config during hydration.
    """

    __tablename__: ClassVar[str] = "assets"

    id: UUID | None = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    source_id: UUID | None = SQLField(
        default=None,
        sa_column=Column(None, ForeignKey("sources.id", ondelete="CASCADE"), index=True, nullable=True),
    )
    org_id: UUID = SQLField(index=True)
    key: str
    materializable: bool = True
    config: dict[str, Any] | None = SQLField(default=None, sa_column=Column(JSON))
    created_at: datetime | None = _ts()

    source: Source | None = Relationship(back_populates="assets")
    resources: list["Resource"] = Relationship(
        link_model=AssetResource,
        sa_relationship_kwargs={"viewonly": True},
    )
    destinations: list["Destination"] = Relationship(
        link_model=AssetDestination,
        sa_relationship_kwargs={"viewonly": True},
    )


# -- Destinations -------------------------------------------------------------


class Destination(SQLModel, table=True):
    """A standalone destination instance — global, not owned by any source.

    Like connections, destinations are independent entities that sources
    reference by ID.  A single destination can be used by multiple sources.
    Resource bindings are stored via the ``destination_resources`` junction table.
    """

    __tablename__: ClassVar[str] = "destinations"

    id: UUID | None = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    org_id: UUID = SQLField(index=True)
    key: str = SQLField(index=True)
    name: str | None = None
    config: dict[str, Any] | None = SQLField(default=None, sa_column=Column(JSON))
    created_at: datetime | None = _ts()

    resources: list["Resource"] = Relationship(
        link_model=DestinationResource,
        sa_relationship_kwargs={"viewonly": True},
    )


# -- Jobs & Scheduling --------------------------------------------------------


class Job(SQLModel, table=True):
    """A cron-scheduled materialization job."""

    __tablename__: ClassVar[str] = "jobs"

    id: UUID | None = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    org_id: UUID = SQLField(index=True)
    name: str
    cron: str
    tags: list[str] = SQLField(default_factory=list, sa_column=Column(ARRAY(String)))
    enabled: bool = True
    partitioned: bool = False
    backfill_days: int | None = None
    created_at: datetime | None = _ts()
    last_run_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    next_run_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))

    runs: list["Run"] = Relationship(back_populates="job")
    sources: list[Source] = Relationship(
        back_populates="jobs",
        link_model=JobSource,
        sa_relationship_kwargs={"passive_deletes": True},
    )
    assets: list[Asset] = Relationship(
        link_model=JobAsset,
        sa_relationship_kwargs={"viewonly": True},
    )


class Backfill(SQLModel, table=True):
    """A backfill spanning a date range with multiple runs."""

    __tablename__: ClassVar[str] = "backfills"

    id: UUID | None = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    job_id: UUID | None = SQLField(
        default=None,
        sa_column=Column(ForeignKey("jobs.id", ondelete="SET NULL"), index=True),
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

    job: Job | None = Relationship()
    runs: list["Run"] = Relationship(back_populates="backfill")


# -- Runs & Events ------------------------------------------------------------


class Run(SQLModel, table=True):
    """A single materialization attempt."""

    __tablename__: ClassVar[str] = "runs"
    __table_args__: ClassVar[tuple[Any, ...]] = (
        Index("ix_runs_org_id_created_at", "org_id", "created_at"),
        Index("ix_runs_backfill_id_status", "backfill_id", "status"),
    )

    id: UUID | None = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    job_id: UUID | None = SQLField(
        default=None,
        sa_column=Column(ForeignKey("jobs.id", ondelete="SET NULL"), index=True),
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

    job: Job | None = Relationship(back_populates="runs")
    backfill: Backfill | None = Relationship(back_populates="runs")


class Event(SQLModel, table=True):
    """An execution event persisted for observability."""

    __tablename__: ClassVar[str] = "events"
    __table_args__: ClassVar[tuple[Any, ...]] = (
        Index("ix_events_run_id_timestamp", "run_id", "timestamp"),
        Index("ix_events_asset_lookup", "run_id", "asset_key", "event_type", "timestamp"),
    )

    id: UUID | None = SQLField(
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
    timestamp: datetime = SQLField(sa_column=Column(TZDateTime))
