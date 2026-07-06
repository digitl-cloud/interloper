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


def destination_response(dest: Component) -> DestinationResponse:
    """Convert a destination component row to its response model."""
    return DestinationResponse(
        id=dest.id,
        key=dest.key,
        name=dest.name,
        config=dest.config,
        resources=resource_map(dest),
        created_at=timestamp(dest.created_at),
    )


def timestamp(value: datetime | None) -> str | None:
    """Render an optional timestamp the way every response does."""
    return str(value) if value else None


def resource_map(component: Component) -> dict[str, str]:
    """Build a ``{slot: resource_id}`` map from the component's resource relations."""
    return {rel.slot: str(rel.dst_id) for rel in component.out_relations if rel.type == "resource"}


def destination_rows(component: Component) -> list[Component]:
    """The destination components bound to this component, eager-loaded."""
    return [rel.dst for rel in component.out_relations if rel.type == "destination"]


def materializable(component: Component) -> bool:
    """An asset's materializable toggle (defaults to true when unset)."""
    return bool((component.config or {}).get("materializable", True))


def user_config(component: Component) -> dict[str, Any] | None:
    """The user-facing config, without the materializable toggle."""
    config = {key: value for key, value in (component.config or {}).items() if key != "materializable"}
    return config or None
