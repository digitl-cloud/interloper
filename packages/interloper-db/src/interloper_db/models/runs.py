"""Operation runs, the backfills that batch them, and the events they emit."""

import json
from datetime import datetime, timezone
from enum import Enum
from typing import Any, ClassVar, Optional
from uuid import UUID, uuid4

import interloper as il
from sqlalchemy import ForeignKey, Index, event
from sqlmodel import AutoString, Column, Relationship, SQLModel, text
from sqlmodel import Field as SQLField

from interloper_db.models.columns import PortableJSON, TZDateTime, timestamp_column
from interloper_db.models.components import Component

_OPERATION_EVENTS = (
    "component_id IS NOT NULL AND event_type IN ('operation_queued', 'operation_skipped', 'operation_started', "
    "'operation_completed', 'operation_failed', 'operation_canceled', 'operation_retried')"
)
"""The events the ``executions`` view derives operation executions from."""

MAX_EVENT_TEXT = 60_000
"""Defensive cap for free-text event fields (well under Postgres limits)."""

PROMOTED_METADATA_KEYS = frozenset(
    {"run_id", "org_id", "component_id", "component_kind", "component_key", "error", "traceback", "message", "level"}
)
"""Metadata keys promoted to their own ``events`` columns; everything else spills into ``data``."""


class RunStatus(str, Enum):
    """Where a run stands in its lifecycle.

    ``pending`` holds a backfill's runs beyond its concurrency until a slot
    frees; the queue claims a ``queued`` run as ``dispatched``, and the
    executor starts it ``running``. The column stores the value, so a row
    read back carries the plain string, which compares equal to its member.
    """

    PENDING = "pending"
    QUEUED = "queued"
    DISPATCHED = "dispatched"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELED = "canceled"


class BackfillStatus(str, Enum):
    """Where a backfill stands: its verdict is its runs' once the last one ends."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELED = "canceled"


TERMINAL_RUN_STATUSES = frozenset({RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.CANCELED})
OPEN_RUN_STATUSES = frozenset({RunStatus.QUEUED, RunStatus.DISPATCHED, RunStatus.RUNNING})
ACTIVE_BACKFILL_STATUSES = frozenset({BackfillStatus.QUEUED, BackfillStatus.RUNNING})


class Backfill(SQLModel, table=True):
    """A backfill spanning a date range with multiple runs.

    ``target`` resolves the component's identity; ``component_id`` nulls on
    deletion (runs and backfills are kept as history), so a ``None`` target
    means exactly that. The relationship is deliberately lazy — a mapped
    eager join would ride into every query, and ``FOR UPDATE`` (the queue
    claim, backfill cancelation) rejects outer joins — so reader queries
    opt in with ``joinedload`` and writers touch it before detaching.
    """

    __tablename__: ClassVar[str] = "backfills"
    __table_args__: ClassVar[tuple[Any, ...]] = (
        Index(
            "ix_backfills_hooks_pending",
            "completed_at",
            postgresql_where=text("hooks_evaluated_at IS NULL"),
            sqlite_where=text("hooks_evaluated_at IS NULL"),
        ),
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
    org_id: UUID = SQLField(index=True)
    status: BackfillStatus = SQLField(default=BackfillStatus.QUEUED, sa_type=AutoString)
    start_key: str
    end_key: str
    concurrency: int = 1
    fail_fast: bool = False
    partitions: int = 0
    started_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    completed_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    hooks_evaluated_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    created_at: datetime | None = timestamp_column()

    runs: list["Run"] = Relationship(back_populates="backfill")
    target: Optional["Component"] = Relationship()


class Run(SQLModel, table=True):
    """A single execution of a component's operation.

    A run is one *attempt*. ``root_run_id`` groups the attempts of one unit of
    work into a stack and is the run's own id for a first attempt, so stack
    membership is one indexed predicate rather than a recursive walk. A stack
    is a linear chain: each attempt number is held by exactly one run.
    ``scheduled_for`` is the earliest instant the queue may claim the run,
    which is how a retry's backoff is served without a second status.

    ``hooks_evaluated_at`` is the hook evaluator's cursor: a terminal run is
    swept until it is stamped, however long it took to commit and whatever
    the scheduler was doing at the time.

    ``quota_reserved_at`` is set when a dispatch-time quota reservation was
    taken; its month tells settlement which usage period to release.
    ``billable`` records the operation's declaration at creation time, so
    quota decisions survive the component (``component_id`` nulls on
    deletion and runs are kept as history). ``target`` resolves the
    component's identity — a ``None`` target means it was deleted; same
    deliberately-lazy contract as :class:`Backfill`.
    """

    __tablename__: ClassVar[str] = "runs"
    __table_args__: ClassVar[tuple[Any, ...]] = (
        Index("ix_runs_org_id_created_at", "org_id", "created_at"),
        Index("ix_runs_backfill_id_status", "backfill_id", "status"),
        Index("ix_runs_root_run_id_attempt", "root_run_id", "attempt", unique=True),
        Index(
            "ix_runs_hooks_pending",
            "completed_at",
            postgresql_where=text("hooks_evaluated_at IS NULL"),
            sqlite_where=text("hooks_evaluated_at IS NULL"),
        ),
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
    status: RunStatus = SQLField(default=RunStatus.QUEUED, sa_type=AutoString)
    retry_of: UUID | None = SQLField(
        default=None,
        sa_column=Column(ForeignKey("runs.id", ondelete="SET NULL"), index=True),
    )
    attempt: int = 1
    retry_scope: str | None = None
    root_run_id: UUID = SQLField(
        default=None,
        sa_column=Column(ForeignKey("runs.id", ondelete="SET NULL"), index=True, nullable=False),
    )
    scheduled_for: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    billable: bool = True
    quota_reserved_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    started_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    completed_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    hooks_evaluated_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    created_at: datetime | None = timestamp_column()

    backfill: Backfill | None = Relationship(back_populates="runs")
    target: Optional["Component"] = Relationship()

    def cancel(self) -> None:
        """Cancel a run that never dispatched; it fires no hooks, so they are settled here."""
        self.status = RunStatus.CANCELED
        self.hooks_evaluated_at = datetime.now(timezone.utc)

    def supersede(self) -> None:
        """Settle a failed run's hooks once a retry supersedes it: its failure is not the stack's verdict.

        A failure whose hooks already ran stays stamped as it was.
        """
        if self.hooks_evaluated_at is None:
            self.hooks_evaluated_at = datetime.now(timezone.utc)

    def event_metadata(self, target: Component | None) -> dict[str, Any]:
        """This run's ids plus its target's identity, for the events it emits.

        The runner spreads this into every event it emits. The ``target_*``
        keys have no structured column on ``events`` — they land in each
        event's ``data``, making events self-describing for telemetry (which
        component the run executed, under what name at the time) without a
        join back through ``runs``.

        Args:
            target: The component this run targets, or None when it no longer
                resolves.

        Returns:
            The metadata dict.
        """
        metadata: dict[str, Any] = {
            "run_id": str(self.id),
            "backfill_id": str(self.backfill_id) if self.backfill_id else None,
            "org_id": str(self.org_id),
        }
        if target is not None:
            metadata |= {
                "target_id": str(target.id),
                "target_kind": target.kind,
                "target_key": target.key,
                "target_name": target.name,
            }
        return metadata


@event.listens_for(Run, "before_insert")
def _stamp_stack_root(_mapper: Any, _connection: Any, target: Run) -> None:
    """Default a run's stack root to itself, and its id to a fresh one.

    A first attempt roots its own stack, which cannot be expressed as a column
    default because it references the row's own id. Doing it here rather than
    at each creation site keeps the invariant in one place: a ``table=True``
    model skips pydantic validation, so a validator would never fire, and
    every caller remembering would be a trap for the next one. The id is
    generated too, because the root cannot be set before it exists; the
    column's server default stays for rows inserted outside the ORM.

    Args:
        _mapper: The mapper being flushed, unused.
        _connection: The connection the flush runs on, unused.
        target: The run row about to be inserted, stamped in place.
    """
    if target.id is None:
        target.id = uuid4()
    if target.root_run_id is None:
        target.root_run_id = target.id


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
        # Error events are a small fraction of the table, so windowed error
        # scans (``EventStore.error_groups``) read a small partial index.
        Index(
            "ix_events_errors",
            "org_id",
            "timestamp",
            postgresql_where=text("error IS NOT NULL"),
            sqlite_where=text("error IS NOT NULL"),
        ),
        # The rows the ``executions`` view ranks, in its windows' partition order.
        Index(
            "ix_events_executions",
            "org_id",
            "run_id",
            "component_id",
            postgresql_where=text(_OPERATION_EVENTS),
            sqlite_where=text(_OPERATION_EVENTS),
        ),
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

    @classmethod
    def from_event(cls, event: il.Event, org_id: UUID, run_id: UUID | None) -> "Event":
        """Build the row a framework event persists as.

        The component reference comes from the ``component_id``,
        ``component_kind`` and ``component_key`` metadata every core emitter
        stamps. Metadata not covered by a structured column lands losslessly
        in ``data``. Free text is sanitised so a stray NUL byte or an
        oversized traceback cannot fail the write.

        Args:
            event: The framework event. A non-UUID ``id`` is replaced by a
                fresh one, which forfeits the upsert's idempotency.
            org_id: Organisation UUID.
            run_id: Run UUID, or ``None`` for an event emitted outside any run.

        Returns:
            The row, not yet persisted.
        """
        metadata = event.metadata
        try:
            event_id = UUID(event.id)
        except (ValueError, TypeError):
            event_id = uuid4()
        component_id = metadata.get("component_id")
        # None values are the absence of a key, not payload: producers emit
        # them unconditionally (backfill_id on non-backfill runs, ...).
        data = {k: v for k, v in metadata.items() if k not in PROMOTED_METADATA_KEYS and v is not None}
        return cls(
            id=event_id,
            org_id=org_id,
            run_id=run_id,
            event_type=event.type.value,
            component_id=UUID(str(component_id)) if component_id else None,
            component_kind=cls._sanitize_text(metadata.get("component_kind")),
            component_key=cls._sanitize_text(metadata.get("component_key")),
            error=cls._sanitize_text(metadata.get("error")),
            traceback=cls._sanitize_text(metadata.get("traceback")),
            message=cls._sanitize_text(metadata.get("message")),
            level=cls._sanitize_text(metadata.get("level")),
            data=cls._sanitize_data(data),
            timestamp=event.timestamp,
        )

    @staticmethod
    def _sanitize_text(value: str | None, *, max_len: int = MAX_EVENT_TEXT) -> str | None:
        """Make a free-text event field safe to persist.

        Postgres ``text`` columns cannot store NUL bytes: a single one makes
        the whole INSERT raise, which, because event persistence is best
        effort, would silently drop the event. NULs are stripped and the
        length capped so an oversized traceback cannot fail the write either.

        Args:
            value: The raw field value, or ``None`` when the producer omitted it.
            max_len: Maximum characters to keep before truncating.

        Returns:
            The cleaned string, or ``None`` if *value* is ``None``.
        """
        if value is None:
            return None
        cleaned = value.replace("\x00", "")
        if len(cleaned) > max_len:
            cleaned = cleaned[:max_len] + "…[truncated]"
        return cleaned

    @staticmethod
    def _sanitize_data(metadata: dict[str, Any]) -> dict[str, Any] | None:
        """Make a metadata dict safe to persist as JSONB, best effort.

        Non-JSON values are coerced through ``str``; a dict that still cannot
        be encoded (circular refs, NaN) is dropped rather than failing the
        event write. Postgres ``jsonb`` rejects NUL escapes the way ``text``
        rejects NUL bytes, so they are stripped from the encoded form; an
        oversized payload is replaced by a marker.

        Args:
            metadata: The event metadata left over once the promoted keys are
                stripped; an empty dict means there is nothing to store.

        Returns:
            The cleaned dict, or ``None`` when there is nothing worth storing.
        """
        if not metadata:
            return None
        try:
            encoded = json.dumps(metadata, default=str, allow_nan=False)
        except (TypeError, ValueError):
            return None
        if len(encoded) > MAX_EVENT_TEXT:
            return {"truncated": True}
        if "\\u0000" in encoded:
            encoded = encoded.replace("\\u0000", "")
        return json.loads(encoded) or None


class Execution(SQLModel, table=True):
    """Read model over the ``executions`` view — never written.

    One row per ``(run, operation)``: the operation's verdict, derived from
    its lifecycle events (latest attempt first, then severity, then recency)
    plus the queued/started/completed timestamps and how many attempts it
    took. The timestamps span every attempt, so a retried operation reads as
    one execution from its first start to its final outcome. The view itself is created by migration 002; ``create_all``
    skips view-backed models (see the ``is_view`` marker).
    """

    __tablename__: ClassVar[str] = "executions"
    __table_args__: ClassVar[dict[str, Any]] = {"info": {"is_view": True}}

    run_id: UUID = SQLField(primary_key=True)
    component_id: UUID = SQLField(primary_key=True)
    org_id: UUID
    component_key: str | None = None
    status: str
    attempts: int = 1
    started_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    completed_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    created_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
