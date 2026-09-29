"""Execution context and serialization helpers for toolkit functions."""

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


def clip(text: str | None, limit: int, *, tail: bool = False) -> str | None:
    """Cut a string to *limit* characters, marking how much was cut.

    Args:
        text: The string to clip, or ``None``.
        limit: Characters to keep.
        tail: Keep the last *limit* characters instead of the first, for text
            whose useful part is at the end (a traceback's raising frame).

    Returns:
        The string unchanged when it fits, else the kept part with an
        ``…[+N chars]`` marker on the cut side; ``None`` stays ``None``.
    """
    if text is None or len(text) <= limit:
        return text
    marker = f"…[+{len(text) - limit} chars]"
    return marker + text[-limit:] if tail else text[:limit] + marker


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
