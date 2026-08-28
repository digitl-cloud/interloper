"""Shared helpers for reading component rows into API responses.

Store methods return ``Component`` rows with children and relations eager-loaded;
these helpers extract the shapes the response models need without touching
the database again.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from interloper_db import Component
from pydantic import BaseModel


class DestinationResponse(BaseModel):
    """Response body for a destination, standalone or nested."""

    id: UUID
    key: str
    name: str | None = None
    config: dict[str, Any] | None = None
    resources: dict[str, str] = {}
    created_at: str | None = None


def destination_response(destination: Component) -> DestinationResponse:
    """Convert a destination component row to its response model.

    Args:
        destination: The destination component row, with its relations
            eager-loaded.

    Returns:
        The response model.
    """
    return DestinationResponse(
        id=destination.id,
        key=destination.key,
        name=destination.name,
        config=destination.config,
        resources=resource_map(destination),
        created_at=timestamp(destination.created_at),
    )


def timestamp(value: datetime | None) -> str | None:
    """Render an optional timestamp the way every response does.

    Args:
        value: The timestamp to render, or None.

    Returns:
        The stringified timestamp, or None when *value* is None.
    """
    return str(value) if value else None


def resource_map(component: Component) -> dict[str, str]:
    """Build a ``{slot: resource_id}`` map from the component's resource relations.

    Args:
        component: The component row, with its relations eager-loaded.

    Returns:
        The resource id bound to each slot, keyed by slot name.
    """
    return {relation.slot: str(relation.dst_id) for relation in component.out_relations if relation.type == "resource"}


def destination_rows(component: Component) -> list[Component]:
    """The destination components bound to this component, eager-loaded.

    Args:
        component: The component row, with its relations eager-loaded.

    Returns:
        The target component of each ``destination`` relation.
    """
    return [relation.dst for relation in component.out_relations if relation.type == "destination"]


def materializable(component: Component) -> bool:
    """An asset's materializable toggle (defaults to true when unset).

    Args:
        component: The asset component row.

    Returns:
        Whether the asset may be materialized.
    """
    return bool((component.config or {}).get("materializable", True))


def user_config(component: Component) -> dict[str, Any] | None:
    """The user-facing config, without the materializable toggle.

    Args:
        component: The component row.

    Returns:
        The config minus the ``materializable`` key, or None when nothing is
        left.
    """
    config = {key: value for key, value in (component.config or {}).items() if key != "materializable"}
    return config or None
