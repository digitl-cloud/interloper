"""Events: the append-only record of what happened during a run.

Events are written by whatever is executing — the host runner, a child
container, the reaper authoring a terminal event on a run's behalf — and read
back for the timeline the UI shows. Producers assign each event a stable id,
so the same logical event dedups when it arrives twice.

Text and payloads are sanitised on the way in: Postgres rejects NUL bytes,
and an oversized payload is replaced rather than allowed to bloat the row.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Sequence
from datetime import datetime
from typing import Any, NamedTuple
from uuid import UUID, uuid4

import interloper as il
from interloper.errors import NotFoundError
from interloper.partitioning import TimeGranularity
from sqlalchemy import Engine, String, case, cast
from sqlalchemy import select as sa_select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import aliased
from sqlmodel import col, func, select

from interloper_db.models import Event, Execution, Run
from interloper_db.session import commit, session_scope
from interloper_db.store.runs import partition_key_range

_MAX_EVENT_TEXT = 60_000
"""Defensive cap for free-text event fields (well under Postgres limits)."""

_PROMOTED_METADATA_KEYS = frozenset(
    {
        "run_id",
        "org_id",
        "component_id",
        "component_kind",
        "component_key",
        "error",
        "traceback",
        "message",
        "level",
    }
)
"""Metadata keys promoted to their own columns rather than spilled into ``data``.

Everything else spills into the ``data`` JSONB column. ``run_id`` and ``org_id``
also arrive via run metadata, but the columns filled from
:meth:`EventStore.save`'s own arguments are the authoritative ones.
"""


class ErrorGroup(NamedTuple):
    """Error events sharing one job, run, component, event type and error text.

    Attributes:
        job_id: The run's target, or ``None`` when it was deleted.
        run_id: The run the events belong to.
        component_key: The component the events concern (``None`` for a
            run-level event).
        event_type: The events' type.
        error: The error text they share.
        count: How many events the group holds.
        first_seen: The earliest event's timestamp.
        last_seen: The latest event's timestamp.
    """

    job_id: UUID | None
    run_id: UUID
    component_key: str | None
    event_type: str
    error: str
    count: int
    first_seen: datetime
    last_seen: datetime


class PartitionExecution(NamedTuple):
    """Whether one asset ever succeeded for one partition of a job.

    Attributes:
        partition_key: The partition.
        component_id: The asset.
        component_key: The asset's key.
        succeeded: Whether any of its executions for that partition succeeded.
    """

    partition_key: str
    component_id: UUID
    component_key: str | None
    succeeded: bool


class CoverageRow(NamedTuple):
    """Whether one asset ever succeeded or failed for one time partition, from runs of any target.

    Attributes:
        asset_id: The asset.
        partition_key: The partition, in its own granularity's key format.
        succeeded: Whether any execution of the asset for that partition succeeded.
        failed: Whether any execution of the asset for that partition failed,
            whether or not another one succeeded; an asset attempted but
            neither succeeded nor failed (in flight, canceled) is neither.
        failed_run_id: The greatest id among the runs whose execution of the
            asset failed, or ``None``.
    """

    asset_id: UUID
    partition_key: str
    succeeded: bool
    failed: bool
    failed_run_id: UUID | None


class EventStore:
    """Run events and the operation executions derived from them."""

    def __init__(self, engine: Engine) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
        """
        self._engine = engine

    # -- Events ----------------------------------------------------------------

    def save(self, event: il.Event, org_id: UUID, run_id: UUID | None = None) -> Event:
        """Persist a framework event to the database, idempotently.

        The event's producer-assigned ``id`` becomes the row primary key
        and the insert is an upsert (``ON CONFLICT DO NOTHING``), so the
        same event delivered more than once — e.g. re-emitted from a child
        container's log stream and also written directly — yields a single
        row rather than a duplicate or an error.  Free-text fields are
        sanitized so a stray NUL byte or oversized traceback can't fail the
        write and silently drop the event.

        Args:
            event: The framework Event.
            org_id: Organisation UUID.
            run_id: Optional run UUID.

        Returns:
            The saved Event row.

        Raises:
            RuntimeError: If the row is gone right after the upsert, which only
                a concurrent delete can cause.
        """
        values = self._event_values(event, org_id, run_id)

        with session_scope(self._engine) as session:
            stmt = pg_insert(Event).values(**values).on_conflict_do_nothing(index_elements=["id"])
            session.execute(stmt)  # ty: ignore[deprecated]
            commit(session)
            saved = session.get(Event, values["id"])
            if saved is None:  # pragma: no cover - only if the row was concurrently deleted
                raise RuntimeError(f"Event {values['id']} missing immediately after upsert")
            return saved

    def list_all(
        self,
        *,
        run_id: UUID | None = None,
        org_id: UUID | None = None,
        component_ids: Sequence[UUID] | None = None,
        event_types: Sequence[str] | None = None,
        has_error: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Event]:
        """List events, optionally filtered by run, component(s), type(s) and error.

        Ordering is ``timestamp ASC, id ASC`` — stable and deterministic so
        ``offset``/``limit`` paging never skips or repeats a row when several
        events share a timestamp.

        Args:
            run_id: Optional run filter.
            org_id: Optional org filter.
            component_ids: Optional filter to events of any of these components.
            event_types: Optional filter to events of any of these types.
            has_error: Keep only events carrying an error.
            limit: Max results (default 100).
            offset: Pagination offset.

        Returns:
            List of Event rows.
        """
        with session_scope(self._engine) as session:
            statement = (
                select(Event)
                .where(*self._event_filters(run_id, org_id, component_ids, event_types, has_error))
                .order_by(col(Event.timestamp).asc(), col(Event.id).asc())
                .offset(offset)
                .limit(limit)
            )
            return list(session.exec(statement).all())

    def count(
        self,
        *,
        run_id: UUID | None = None,
        org_id: UUID | None = None,
        component_ids: Sequence[UUID] | None = None,
        event_types: Sequence[str] | None = None,
        has_error: bool = False,
    ) -> int:
        """Count events matching the same filters as :meth:`list_all`.

        Args:
            run_id: Optional run filter.
            org_id: Optional org filter.
            component_ids: Optional filter to events of any of these components.
            event_types: Optional filter to events of any of these types.
            has_error: Count only events carrying an error.

        Returns:
            Total number of matching events (ignoring limit/offset).
        """
        with session_scope(self._engine) as session:
            statement = (
                select(func.count())
                .select_from(Event)
                .where(*self._event_filters(run_id, org_id, component_ids, event_types, has_error))
            )
            return session.exec(statement).one()

    def get(self, event_id: UUID, *, org_id: UUID | None = None) -> Event:
        """Load an event by ID.

        Args:
            event_id: The event UUID.
            org_id: Organisation the event must belong to (``None`` accepts
                any); a mismatch raises ``NotFoundError`` like an absent row.

        Returns:
            The Event row.

        Raises:
            NotFoundError: If the event is not found, or belongs to another
                organisation.
        """
        with session_scope(self._engine) as session:
            db_event = session.get(Event, event_id)
            if not db_event or (org_id is not None and db_event.org_id != org_id):
                raise NotFoundError(f"Event {event_id} not found")
            return db_event

    def error_groups(
        self,
        org_id: UUID,
        *,
        event_types: Sequence[str],
        since: datetime | None = None,
        until: datetime | None = None,
        job_id: UUID | None = None,
        backfill_id: UUID | None = None,
        run_id: UUID | None = None,
        max_rows: int = 20_000,
    ) -> tuple[list[ErrorGroup], bool]:
        """Group an organisation's error events by job, run, component, type and text.

        Identical texts collapse here, in the database, so a caller classifying
        errors reads each distinct text once per run rather than every event.
        Largest groups come first, so a capped scan keeps the loudest errors.

        Args:
            org_id: Organisation UUID.
            event_types: The event types to read; the caller picks the ones
                that record each failure once.
            since: Keep events at or after this instant.
            until: Keep events before this instant.
            job_id: Keep events of runs targeting this component.
            backfill_id: Keep events of this backfill's runs.
            run_id: Keep events of this run.
            max_rows: Cap on the groups returned.

        Returns:
            The groups, and whether the cap cut any off.
        """
        filters: list[Any] = [
            Event.org_id == org_id,
            col(Event.error).is_not(None),
            col(Event.event_type).in_(event_types),
        ]
        if since is not None:
            filters.append(col(Event.timestamp) >= since)
        if until is not None:
            filters.append(col(Event.timestamp) < until)
        if job_id is not None:
            filters.append(Run.component_id == job_id)
        if backfill_id is not None:
            filters.append(Run.backfill_id == backfill_id)
        if run_id is not None:
            filters.append(Event.run_id == run_id)
        count = func.count().label("count")
        statement = (
            sa_select(
                col(Run.component_id),
                col(Event.run_id),
                col(Event.component_key),
                col(Event.event_type),
                col(Event.error),
                count,
                func.min(col(Event.timestamp)),
                func.max(col(Event.timestamp)),
            )
            .join(Run, col(Run.id) == col(Event.run_id))
            .where(*filters)
            .group_by(
                col(Run.component_id),
                col(Event.run_id),
                col(Event.component_key),
                col(Event.event_type),
                col(Event.error),
            )
            .order_by(count.desc(), func.max(col(Event.timestamp)).desc())
            .limit(max_rows + 1)
        )
        with session_scope(self._engine) as session:
            rows = [ErrorGroup(*row) for row in session.execute(statement).all()]  # ty: ignore[deprecated]
        return rows[:max_rows], len(rows) > max_rows

    # -- Executions --------------------------------------------------------------

    def list_executions(self, run_id: UUID) -> list[Execution]:
        """List a run's operation executions from the ``executions`` view.

        Args:
            run_id: The run UUID.

        Returns:
            One read-model row per operation touched by the run.
        """
        with session_scope(self._engine) as session:
            statement = select(Execution).where(Execution.run_id == run_id)
            return list(session.exec(statement).all())

    def count_executions(self, run_ids: Sequence[UUID]) -> dict[UUID, dict[str, int]]:
        """Count each run's operation executions by status, in one query.

        Args:
            run_ids: The runs to count, typically one listing page.

        Returns:
            Per run, its execution count per status; a run with no executions
            yet is absent.
        """
        if not run_ids:
            return {}
        statement = (
            select(Execution.run_id, Execution.status, func.count())
            .where(col(Execution.run_id).in_(run_ids))
            .group_by(col(Execution.run_id), col(Execution.status))
        )
        counts: dict[UUID, dict[str, int]] = {}
        with session_scope(self._engine) as session:
            for run_id, status, count in session.exec(statement).all():
                counts.setdefault(run_id, {})[status] = count
        return counts

    def partition_coverage(
        self, org_id: UUID, job_id: UUID, start_key: str, end_key: str
    ) -> list[PartitionExecution]:
        """Whether each asset ever succeeded, per partition of a job's runs.

        Args:
            org_id: Organisation UUID.
            job_id: The job whose runs are read.
            start_key: First partition key of the range.
            end_key: Last partition key of the range (inclusive); must share
                the start key's granularity.

        Returns:
            One row per partition and asset that executed at least once.
        """
        statement = (
            select(
                col(Run.partition_key),
                col(Execution.component_id),
                func.max(col(Execution.component_key)),
                func.max(case((col(Execution.status) == "success", 1), else_=0)),
            )
            .join(Run, col(Run.id) == col(Execution.run_id))
            .where(
                Execution.org_id == org_id,
                Run.org_id == org_id,
                Run.component_id == job_id,
                *partition_key_range(start_key, end_key),
            )
            .group_by(col(Run.partition_key), col(Execution.component_id))
        )
        with session_scope(self._engine) as session:
            return [
                PartitionExecution(partition_key, component_id, component_key, bool(succeeded))
                for partition_key, component_id, component_key, succeeded in session.exec(statement).all()
                if partition_key is not None
            ]

    def coverage_rows(self, org_id: UUID) -> list[CoverageRow]:
        """Per asset and time partition, all-time, whether it ever succeeded or failed.

        Runs of every target count (a job, a source, the asset itself, a
        backfill, a deleted target): coverage is a property of the asset's
        data, not of what triggered it. Keys of every time granularity are
        read, and of every period: the caller derives both each asset's
        attempted span and a window's days from these rows, because any read
        of the executions view scans the organisation's operation events
        whole, so one all-time read costs less than a bounds read plus a
        windowed one.

        Args:
            org_id: Organisation UUID.

        Returns:
            One row per asset and time partition key that executed at least once.
        """
        key = col(Run.partition_key)
        key_lengths = [
            len(granularity.format(dt.datetime(2000, 1, 1)))
            for granularity in TimeGranularity
            if granularity.key_format is not None
        ]
        # Cast for a portable max(): Postgres has no max(uuid), and UUID() parses both its dashed text and SQLite's hex.
        failed_run = func.max(case((col(Execution.status) == "failed", cast(col(Run.id), String))))
        # The asset id comes back as text and is parsed once per asset: a UUID per row costs more than the roll-up.
        asset = cast(col(Execution.component_id), String)
        statement = (
            sa_select(asset, key, func.max(case((col(Execution.status) == "success", 1), else_=0)), failed_run)
            .join(Run, col(Run.id) == col(Execution.run_id))
            .where(
                col(Execution.org_id) == org_id,
                col(Run.org_id) == org_id,
                key.is_not(None),
                func.length(key).in_(key_lengths),
            )
            .group_by(col(Execution.component_id), key)
        )
        asset_ids: dict[str, UUID] = {}
        with session_scope(self._engine) as session:
            return [
                CoverageRow(
                    asset_ids.get(asset_text) or asset_ids.setdefault(asset_text, UUID(asset_text)),
                    partition_key,
                    succeeded=bool(succeeded),
                    failed=failed is not None,
                    failed_run_id=UUID(failed) if failed else None,
                )
                for asset_text, partition_key, succeeded, failed in session.execute(statement).all()  # ty: ignore[deprecated]
            ]

    def latest_executions(self, org_id: UUID) -> list[Execution]:
        """The most recent execution of every asset in an organisation.

        Args:
            org_id: The organisation UUID.

        Returns:
            One row per asset that has ever executed, carrying its newest run's
            status and timestamps.
        """
        rank = (
            func.row_number()
            .over(partition_by=col(Execution.component_id), order_by=col(Execution.created_at).desc())
            .label("rank")
        )
        ranked = select(Execution, rank).where(Execution.org_id == org_id).subquery()
        latest = aliased(Execution, ranked)
        with session_scope(self._engine) as session:
            statement = select(latest).where(ranked.c.rank == 1)
            return list(session.exec(statement).all())

    # -- Internals -------------------------------------------------------------

    @staticmethod
    def _sanitize_text(value: str | None, *, max_len: int = _MAX_EVENT_TEXT) -> str | None:
        """Make a free-text event field safe to persist.

        Postgres ``text`` columns cannot store NUL bytes (``0x00``) — a single
        one makes the whole INSERT raise, which (because event persistence is
        best-effort) would silently drop the event.  Strip NULs and cap the
        length so an oversized traceback can't fail the write either.

        Args:
            value: The raw field value, or ``None`` when the producer omitted it.
            max_len: Maximum characters to keep before truncating, defaulting to
                ``_MAX_EVENT_TEXT``. Longer values are cut and marked truncated.

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
        """Make a metadata dict safe to persist as JSONB, best-effort.

        Non-JSON values are coerced through ``str``; a dict that still can't be
        encoded (circular refs, NaN) is dropped rather than failing the event
        write. Postgres ``jsonb`` rejects NUL escapes the same way ``text``
        rejects NUL bytes, so they are stripped from the encoded form; an
        oversized payload is replaced by a marker so the write can't fail on
        size either.

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
        if len(encoded) > _MAX_EVENT_TEXT:
            return {"truncated": True}
        if "\\u0000" in encoded:
            encoded = encoded.replace("\\u0000", "")
        return json.loads(encoded) or None

    @staticmethod
    def _event_values(event: il.Event, org_id: UUID, run_id: UUID | None) -> dict[str, Any]:
        """Map a framework event onto ``events`` column values.

        The component reference comes from ``component_id``/``component_kind``/
        ``component_key`` metadata — the identity keys every core emitter
        stamps. Metadata not covered by a structured column lands losslessly
        in ``data``.

        Args:
            event: The framework Event to map. A non-UUID ``id`` is replaced by
                a fresh one, which forfeits the upsert's idempotency.
            org_id: Organisation UUID for the ``org_id`` column.
            run_id: Run UUID for the ``run_id`` column, or ``None`` for an event
                emitted outside any run.

        Returns:
            Column values for an ``events`` insert.
        """
        metadata = event.metadata
        try:
            event_id = UUID(event.id)
        except (ValueError, TypeError):
            event_id = uuid4()

        component_id = metadata.get("component_id")
        component_kind = metadata.get("component_kind")
        component_key = metadata.get("component_key")

        return {
            "id": event_id,
            "org_id": org_id,
            "run_id": run_id,
            "event_type": event.type.value,
            "component_id": UUID(str(component_id)) if component_id else None,
            "component_kind": EventStore._sanitize_text(component_kind),
            "component_key": EventStore._sanitize_text(component_key),
            "error": EventStore._sanitize_text(metadata.get("error")),
            "traceback": EventStore._sanitize_text(metadata.get("traceback")),
            "message": EventStore._sanitize_text(metadata.get("message")),
            "level": EventStore._sanitize_text(metadata.get("level")),
            # None values are the absence of a key, not payload — producers emit
            # them unconditionally (backfill_id on non-backfill runs, …).
            "data": EventStore._sanitize_data(
                {k: v for k, v in metadata.items() if k not in _PROMOTED_METADATA_KEYS and v is not None}
            ),
            "timestamp": event.timestamp,
        }

    @staticmethod
    def _event_filters(
        run_id: UUID | None,
        org_id: UUID | None,
        component_ids: Sequence[UUID] | None,
        event_types: Sequence[str] | None,
        has_error: bool = False,
    ) -> list[Any]:
        """The shared where-clauses of :meth:`EventStore.list_all` / ``count``.

        One builder for both so listing and counting can never disagree.

        Args:
            run_id: Keep events of this run; ``None`` applies no run filter.
            org_id: Keep events of this organisation; ``None`` applies no filter.
            component_ids: Keep events of any of these components; ``None`` or
                empty applies no filter.
            event_types: Keep events of any of these types; ``None`` or empty
                applies no filter.
            has_error: Keep only events carrying an error.

        Returns:
            Filter expressions for the given (optional) criteria.
        """
        filters: list[Any] = []
        if run_id:
            filters.append(Event.run_id == run_id)
        if org_id:
            filters.append(Event.org_id == org_id)
        if component_ids:
            filters.append(col(Event.component_id).in_(component_ids))
        if event_types:
            filters.append(col(Event.event_type).in_(event_types))
        if has_error:
            filters.append(col(Event.error).is_not(None))
        return filters
