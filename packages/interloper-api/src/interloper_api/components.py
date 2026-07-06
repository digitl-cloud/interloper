"""Shared helpers for reading component rows into API responses.

Store methods return ``Component`` rows with children and relations eager-loaded;
these helpers extract the shapes the response models need without touching
the database again.
"""

from __future__ import annotations

from typing import Any

from interloper_db import Component


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
