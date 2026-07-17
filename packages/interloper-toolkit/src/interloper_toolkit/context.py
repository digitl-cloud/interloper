"""Execution context and serialization for toolkit functions."""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
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


def serialize(obj: Any) -> Any:
    """Convert a SQLModel instance (or collection) to a JSON-safe dict.

    Recursively handles UUIDs, datetimes, dates, lists, and nested models.

    Args:
        obj: A SQLModel row, dict, list, or primitive.
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
