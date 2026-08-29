"""Events: the append-only record of what happened during a run.

Events are written by whatever is executing — the host runner, a child
container, the reaper authoring a terminal event on a run's behalf — and read
back for the timeline the UI shows. Producers assign each event a stable id,
so the same logical event dedups when it arrives twice.

Text and payloads are sanitised on the way in: Postgres rejects NUL bytes,
and an oversized payload is replaced rather than allowed to bloat the row.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any
from uuid import UUID, uuid4

import interloper as il
from sqlalchemy import Engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlmodel import col, func, select

from interloper_db.models import AssetExecution, Event
from interloper_db.session import commit, session_scope

_MAX_EVENT_TEXT = 60_000
"""Defensive cap for free-text event fields (well under Postgres limits)."""

_PROMOTED_METADATA_KEYS = frozenset(
    {
        "run_id",
        "org_id",
        "asset_id",
        "asset_key",
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

Everything else spills into the ``data`` JSONB column. ``asset_id``/``asset_key``
are the compat aliases core emitters use for the component columns; ``run_id`` and
``org_id`` also arrive via run metadata, but the columns filled from
:meth:`EventStore.save_event`'s own arguments are the authoritative ones.
"""


class EventStore:
    """Run events and the asset executions derived from them."""

    def __init__(self, engine: Engine) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
        """
        self._engine = engine

    def save_event(self, event: il.Event, org_id: UUID, run_id: UUID | None = None) -> Event:
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

    def list_events(
        self,
        *,
        run_id: UUID | None = None,
        org_id: UUID | None = None,
        component_ids: Sequence[UUID] | None = None,
        event_types: Sequence[str] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Event]:
        """List events, optionally filtered by run, component(s) and/or type(s).

        Ordering is ``timestamp ASC, id ASC`` — stable and deterministic so
        ``offset``/``limit`` paging never skips or repeats a row when several
        events share a timestamp.

        Args:
            run_id: Optional run filter.
            org_id: Optional org filter.
            component_ids: Optional filter to events of any of these components.
            event_types: Optional filter to events of any of these types.
            limit: Max results (default 100).
            offset: Pagination offset.

        Returns:
            List of Event rows.
        """
        with session_scope(self._engine) as session:
            statement = (
                select(Event)
                .where(*self._event_filters(run_id, org_id, component_ids, event_types))
                .order_by(col(Event.timestamp).asc(), col(Event.id).asc())
                .offset(offset)
                .limit(limit)
            )
            return list(session.exec(statement).all())

    def count_events(
        self,
        *,
        run_id: UUID | None = None,
        org_id: UUID | None = None,
        component_ids: Sequence[UUID] | None = None,
        event_types: Sequence[str] | None = None,
    ) -> int:
        """Count events matching the same filters as :meth:`list_events`.

        Args:
            run_id: Optional run filter.
            org_id: Optional org filter.
            component_ids: Optional filter to events of any of these components.
            event_types: Optional filter to events of any of these types.

        Returns:
            Total number of matching events (ignoring limit/offset).
        """
        with session_scope(self._engine) as session:
            statement = (
                select(func.count())
                .select_from(Event)
                .where(*self._event_filters(run_id, org_id, component_ids, event_types))
            )
            return session.exec(statement).one()

    def list_asset_executions(self, run_id: UUID) -> list[AssetExecution]:
        """List a run's asset executions from the ``asset_executions`` view.

        Args:
            run_id: The run UUID.

        Returns:
            One read-model row per asset touched by the run.
        """
        with session_scope(self._engine) as session:
            statement = select(AssetExecution).where(AssetExecution.run_id == run_id)
            return list(session.exec(statement).all())

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
        ``component_key`` metadata; the ``asset_id``/``asset_key`` keys the asset
        runners emit map onto the same columns (with kind ``"asset"``), so core
        emitters need no knowledge of the persistence schema. Metadata not
        covered by a structured column lands losslessly in ``data``.

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

        component_id = metadata.get("component_id") or metadata.get("asset_id")
        component_kind = metadata.get("component_kind") or ("asset" if metadata.get("asset_id") else None)
        component_key = metadata.get("component_key") or metadata.get("asset_key")

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
    ) -> list[Any]:
        """The shared where-clauses of :meth:`EventStore.list_events` / ``count_events``.

        One builder for both so listing and counting can never disagree.

        Args:
            run_id: Keep events of this run; ``None`` applies no run filter.
            org_id: Keep events of this organisation; ``None`` applies no filter.
            component_ids: Keep events of any of these components; ``None`` or
                empty applies no filter.
            event_types: Keep events of any of these types; ``None`` or empty
                applies no filter.

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
        return filters
