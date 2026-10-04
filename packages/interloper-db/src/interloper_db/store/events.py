"""Events: the append-only record of what happened during a run.

Events are written by whatever is executing — the host runner, a child
container, the reaper authoring a terminal event on a run's behalf — and read
back for the timeline the UI shows. Producers assign each event a stable id,
so the same logical event dedups when it arrives twice.

Text and payloads are sanitised on the way in: Postgres rejects NUL bytes,
and an oversized payload is replaced rather than allowed to bloat the row.
"""

from __future__ import annotations

import builtins
import json
from collections.abc import Sequence
from datetime import datetime
from typing import Any, NamedTuple
from uuid import UUID, uuid4

import interloper as il
from interloper.errors import NotFoundError
from sqlalchemy import Engine
from sqlalchemy import select as sa_select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlmodel import col, func, select

from interloper_db.models import Event, Run
from interloper_db.session import commit, session_scope
from interloper_db.store.page import Page, PageQuery

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


class EventQuery(PageQuery):
    """Which events a listing reads.

    Attributes:
        component_id: Keep events of any of these components.
        event_type: Keep events of any of these types.
        has_error: Keep only events carrying an error.
    """

    component_id: list[UUID] | None = None
    event_type: list[str] | None = None
    has_error: bool = False


class EventStore:
    """Store methods for run events."""

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

    def list(self, org_id: UUID, query: EventQuery, *, run_id: UUID | None = None) -> Page[Event]:
        """List an organisation's events, oldest first.

        Ordering is ``timestamp ASC, id ASC`` — stable and deterministic so
        offset paging never skips or repeats a row when several events share
        a timestamp.

        Args:
            org_id: Organisation UUID.
            query: Which components, event types and errors, and the window
                to read.
            run_id: Keep the events of this run; ``None`` keeps every run's.

        Returns:
            The page of events.
        """
        statement = (
            select(Event)
            .where(Event.org_id == org_id)
            .order_by(col(Event.timestamp).asc(), col(Event.id).asc())
        )
        if run_id is not None:
            statement = statement.where(Event.run_id == run_id)
        if query.component_id:
            statement = statement.where(col(Event.component_id).in_(query.component_id))
        if query.event_type:
            statement = statement.where(col(Event.event_type).in_(query.event_type))
        if query.has_error:
            statement = statement.where(col(Event.error).is_not(None))
        with session_scope(self._engine) as session:
            return Page.read(session, statement, query)

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
    ) -> tuple[builtins.list[ErrorGroup], bool]:
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
