"""Events: the append-only record of what happened during a run.

Events are written by whatever is executing — the host runner, a child
container, the reaper authoring a terminal event on a run's behalf — and read
back for the timeline the UI shows. Producers assign each event a stable id,
so the same logical event dedups when it arrives twice.

The row the framework event becomes, text and payload sanitised, is
:meth:`Event.from_event`'s; the store only upserts it.
"""

from __future__ import annotations

from uuid import UUID

import interloper as il
from interloper.errors import NotFoundError
from sqlalchemy import Engine
from sqlmodel import col, select

from interloper_db.models import Event
from interloper_db.session import commit, dialect_insert, session_scope
from interloper_db.store.page import Page, PageQuery


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
