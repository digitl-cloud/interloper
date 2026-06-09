"""Store, catalog, and session context for agent tools."""

from __future__ import annotations

import datetime
from typing import Any
from uuid import UUID

from google.adk.tools.tool_context import ToolContext
from interloper.catalog.base import Catalog
from interloper_db import Store, init_engine

_store: Store | None = None
_catalog: Catalog | None = None


def init(database_url: str, catalog: Catalog) -> None:
    """Initialize the agent context with a database connection and catalog.

    Args:
        database_url: PostgreSQL connection string.
        catalog: Catalog instance.
    """
    global _store, _catalog  # noqa: PLW0603
    init_engine(database_url)
    _catalog = catalog
    _store = Store.from_settings(catalog=catalog)


def set_store(store: Store) -> None:
    """Set the global Store instance (used by interloper-api integration).

    Args:
        store: An already-initialized Store.
    """
    global _store  # noqa: PLW0603
    _store = store


def set_catalog(catalog: Catalog) -> None:
    """Set the global catalog instance (used by interloper-api integration).

    Args:
        catalog: Catalog instance.
    """
    global _catalog  # noqa: PLW0603
    _catalog = catalog


def get_store() -> Store:
    """Return the global Store instance."""
    if _store is None:
        raise RuntimeError("Agent context not initialized. Call init() or set_store() first.")
    return _store


def get_catalog() -> dict[str, Any]:
    """Return the global catalog as a serialized dict."""
    if _catalog is None:
        raise RuntimeError("Agent context not initialized. Call init() or set_catalog() first.")
    return _catalog.dump()


def get_org_id(tool_context: ToolContext | None) -> UUID:
    """Extract the organisation ID from ADK session state.

    The caller must set ``session.state["org_id"]`` before invoking the agent.

    Args:
        tool_context: Injected by ADK. ``None`` only if a tool is invoked
            outside the ADK runtime, which is a programming error.
    """
    if tool_context is None:
        raise ValueError("tool_context not provided (must be invoked via the ADK runtime)")
    raw = tool_context.state.get("org_id")
    if raw is None:
        raise ValueError("org_id not set in session state")
    return UUID(str(raw))


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
