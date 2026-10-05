"""Events: the append-only record of what happened during a run.

Events are written by whatever is executing — the host runner, a child
container, the reaper authoring a terminal event on a run's behalf — and read
back for the timeline the UI shows. Producers assign each event a stable id,
so the same logical event dedups when it arrives twice.

The row the framework event becomes, text and payload sanitised, is
:meth:`Event.from_event`'s; the store only upserts it.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import interloper as il
from interloper.errors import NotFoundError
from interloper.runner.state import RunState
from sqlalchemy import Engine
from sqlmodel import col, select

from interloper_db.models import Event, Run
from interloper_db.session import commit, dialect_insert, session_scope
from interloper_db.store.page import Page, PageQuery

_OPERATION_EVENT_TYPES = (
    "operation_queued",
    "operation_started",
    "operation_retried",
    "operation_completed",
    "operation_failed",
    "operation_canceled",
    "operation_skipped",
)
_OPERATION_VERDICTS = frozenset({"operation_completed", "operation_failed", "operation_canceled", "operation_skipped"})


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
        row = Event.from_event(event, org_id, run_id)
        values = {name: getattr(row, name) for name in Event.model_fields}
        with session_scope(self._engine) as session:
            table = Event.__table__  # ty: ignore[unresolved-attribute]
            statement = dialect_insert(session)(table).values(**values).on_conflict_do_nothing(index_elements=["id"])
            session.execute(statement)  # ty: ignore[deprecated]
            commit(session)
            saved = session.get(Event, row.id)
            if saved is None:  # pragma: no cover - only if the row was concurrently deleted
                raise RuntimeError(f"Event {row.id} missing immediately after upsert")
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
            select(Event).where(Event.org_id == org_id).order_by(col(Event.timestamp).asc(), col(Event.id).asc())
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
            org_id: Organisation the row must belong to; a mismatch raises
                ``NotFoundError`` like an absent row, so a caller cannot learn
                that an id exists in another tenant. ``None`` accepts any
                organisation, for a caller that authorizes by the row's own
                ``org_id`` afterwards (the API) or serves every organisation
                (the scheduler).

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

    def close_operations(self, db_run: Run, *, error: str | None) -> None:
        """Record a verdict for every operation a run left without one.

        A run that ends outside its executor (reaped, timed out, canceled)
        leaves its in-flight and queued operations open, and the executions
        view would read them as running forever. Each operation's latest
        attempt that has no verdict gets one: ``operation_failed`` carrying
        *error* when it had started, ``operation_canceled`` otherwise or when
        no *error* is given. The ids are the ones the runner derives, so the
        same verdict arriving late from a stopping pod dedups.

        Args:
            db_run: The run being ended, in the caller's transaction.
            error: Why the run ended as a failure, or ``None`` for a cancel.
        """
        statement = (
            select(Event)
            .where(Event.run_id == db_run.id, col(Event.event_type).in_(_OPERATION_EVENT_TYPES))
            .order_by(col(Event.timestamp).asc(), col(Event.id).asc())
        )
        with session_scope(self._engine) as session:
            latest: dict[UUID, tuple[int, list[Event]]] = {}
            for event in session.exec(statement).all():
                if event.component_id is None:
                    continue
                attempt = int((event.data or {}).get("attempt", 1))
                current = latest.get(event.component_id)
                if current is None or attempt > current[0]:
                    latest[event.component_id] = (attempt, [event])
                elif attempt == current[0]:
                    current[1].append(event)

            for component_id, (attempt, events) in latest.items():
                types = {event.event_type for event in events}
                if types & _OPERATION_VERDICTS:
                    continue
                failed = "operation_started" in types and error is not None
                event_type = il.EventType.OPERATION_FAILED if failed else il.EventType.OPERATION_CANCELED
                last = events[-1]
                metadata: dict[str, Any] = {
                    **(last.data or {}),
                    "component_id": str(component_id),
                    "component_kind": last.component_kind,
                    "component_key": last.component_key,
                    "attempt": attempt,
                    "message": f"Operation '{last.component_key}' {'failed' if failed else 'canceled'}",
                }
                if failed:
                    metadata["error"] = error
                event_id = RunState.operation_event_id(str(db_run.id), str(component_id), event_type, attempt=attempt)
                self.save(il.Event(type=event_type, metadata=metadata, id=event_id), db_run.org_id, db_run.id)
            commit(session)
