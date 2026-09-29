"""Execution context and serialization for toolkit functions."""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from uuid import UUID

from interloper.errors import NotFoundError

if TYPE_CHECKING:
    from interloper_db.models import Backfill, Component, Run
    from interloper_db.store import Store


@dataclass(frozen=True)
class ToolkitContext:
    """Everything a toolkit function needs: persistence, catalog, tenant.

    Attributes:
        store: The Store to query.
        catalog: The *dumped* catalog (``Catalog.dump()``) — a plain dict of
            component definitions keyed by catalog key.
        org_id: The organisation every query is scoped to.
    """

    store: Store
    catalog: dict[str, Any]
    org_id: UUID

    def run(self, run_id: str | UUID) -> Run:
        """Load one of the organisation's runs, or raise ``NotFoundError``.

        Args:
            run_id: The run's UUID, as the caller passed it.

        Returns:
            The run row.
        """
        return self._owned(self.store.runs.get, run_id, "Run")

    def component(self, component_id: str | UUID, *, kind: str | None = None) -> Component:
        """Load one of the organisation's components, or raise ``NotFoundError``.

        Args:
            component_id: The component's UUID, as the caller passed it.
            kind: Require this kind; ``None`` accepts any.

        Returns:
            The component row.
        """
        return self._owned(lambda i: self.store.components.get(i, kind=kind), component_id, "Component")

    def backfill(self, backfill_id: str | UUID) -> Backfill:
        """Load one of the organisation's backfills, or raise ``NotFoundError``.

        Args:
            backfill_id: The backfill's UUID, as the caller passed it.

        Returns:
            The backfill row.
        """
        return self._owned(self.store.runs.get_backfill, backfill_id, "Backfill")

    def _owned(self, load: Any, row_id: str | UUID, label: str) -> Any:
        """Load a row by id and keep it only if it belongs to the organisation.

        Ids arrive from the model, so every id-taking tool resolves through
        here. Another organisation's row reads exactly like a missing one,
        which keeps its existence private.

        Args:
            load: The store getter, raising ``NotFoundError`` for a missing id.
            row_id: The id as passed by the caller.
            label: Row noun for the error message.

        Returns:
            The loaded row.

        Raises:
            NotFoundError: If the row is missing or belongs to another
                organisation.
        """
        uid = row_id if isinstance(row_id, UUID) else UUID(row_id)
        try:
            row = load(uid)
        except NotFoundError:
            row = None
        if row is None or row.org_id != self.org_id:
            raise NotFoundError(f"{label} {uid} not found")
        return row


def serialize(obj: Any) -> Any:
    """Convert a SQLModel instance (or collection) to a JSON-safe dict.

    Recursively handles UUIDs, datetimes, dates, lists, and nested models.

    Args:
        obj: A SQLModel row, dict, list, or primitive.

    Returns:
        The value with rows, UUIDs and datetimes rendered JSON-safe.

    """
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, datetime.date):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [serialize(item) for item in obj]
    # SQLModel / Pydantic BaseModel
    if hasattr(obj, "model_dump"):
        return serialize(obj.model_dump())
    return str(obj)
