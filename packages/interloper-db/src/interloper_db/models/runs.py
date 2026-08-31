"""Operation runs, the backfills that batch them, and the events they emit."""

from datetime import datetime
from typing import Any, ClassVar
from uuid import UUID

from sqlalchemy import ForeignKey, Index
from sqlmodel import Column, Relationship, SQLModel, text
from sqlmodel import Field as SQLField

from interloper_db.models.columns import PortableJSON, TZDateTime, timestamp_column


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
    start_key: str
    end_key: str
    concurrency: int = 1
    fail_fast: bool = False
    partitions: int = 0
    started_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    completed_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    created_at: datetime | None = timestamp_column()

    runs: list["Run"] = Relationship(back_populates="backfill")


class Run(SQLModel, table=True):
    """A single execution of a component's operation.

    ``quota_reserved_at`` is set when a dispatch-time quota reservation was
    taken; its month tells settlement which usage period to release.
    ``billable`` records the operation's declaration at creation time, so
    quota decisions survive the component (``component_id`` nulls on
    deletion and runs are kept as history).
    """

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
    partition_key: str | None = None
    status: str = "queued"
    retry_of: UUID | None = SQLField(
        default=None,
        sa_column=Column(ForeignKey("runs.id", ondelete="SET NULL"), index=True),
    )
    attempt: int = 1
    retry_scope: str | None = None
    billable: bool = True
    quota_reserved_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    started_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    completed_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    created_at: datetime | None = timestamp_column()

    backfill: Backfill | None = Relationship(back_populates="runs")


class Event(SQLModel, table=True):
    """An execution event persisted for observability.

    Follows the same contract as ``components``: ``component_id``/
    ``component_kind``/``component_key`` reference the component the event
    concerns — any kind, no schema change per kind. Deliberately no foreign
    key: events are history and outlive the component; the denormalized
    kind/key snapshot keeps a deleted component's events readable. The
    structured columns carry only what every consumer renders; the rest of
    the producer's metadata lands losslessly in ``data``.
    """

    __tablename__: ClassVar[str] = "events"
    __table_args__: ClassVar[tuple[Any, ...]] = (
        Index("ix_events_run_id_timestamp", "run_id", "timestamp"),
        Index("ix_events_component_lookup", "run_id", "component_id", "event_type", "timestamp"),
    )

    id: UUID = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    org_id: UUID
    run_id: UUID | None = SQLField(default=None, foreign_key="runs.id")
    event_type: str
    error: str | None = None
    traceback: str | None = None
    component_id: UUID | None = SQLField(default=None)
    component_kind: str | None = None
    component_key: str | None = None
    message: str | None = None
    level: str | None = None
    data: dict[str, Any] | None = SQLField(default=None, sa_column=Column(PortableJSON))
    timestamp: datetime = SQLField(sa_column=Column(TZDateTime))


class AssetExecution(SQLModel, table=True):
    """Read model over the ``asset_executions`` view — never written.

    One row per ``(run, asset)``: the current status derived from lifecycle
    events (severity then recency) plus the queued/started/completed
    timestamps. The view itself is created by migration 002; ``create_all``
    skips view-backed models (see the ``is_view`` marker).
    """

    __tablename__: ClassVar[str] = "asset_executions"
    __table_args__: ClassVar[dict[str, Any]] = {"info": {"is_view": True}}

    run_id: UUID = SQLField(primary_key=True)
    asset_id: UUID = SQLField(primary_key=True)
    org_id: UUID
    asset_key: str | None = None
    status: str
    started_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    completed_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    created_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
