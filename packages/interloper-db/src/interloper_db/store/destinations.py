"""Destination persistence: CRUD for standalone destination management."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from interloper.errors import NotFoundError
from sqlmodel import Session

from interloper_db.engine import get_engine
from interloper_db.models import Component
from interloper_db.store.components import list_components, load_component, sync_relations


class DestinationMixin:
    """Store methods for destination CRUD."""

    def list_destinations(self, org_id: UUID) -> list[Component]:
        """List all destinations for an organisation."""
        with Session(get_engine()) as session:
            return list_components(session, org_id, kind="destination")

    def get_destination(self, destination_id: UUID) -> Component:
        """Get a single destination by ID."""
        with Session(get_engine()) as session:
            return load_component(session, destination_id, kind="destination")

    def create_destination(
        self,
        org_id: UUID,
        *,
        key: str,
        name: str | None = None,
        config: dict[str, Any] | None = None,
        resources: dict[str, str] | None = None,
    ) -> Component:
        """Create a standalone destination."""
        with Session(get_engine()) as session:
            db_destination = Component(org_id=org_id, kind="destination", key=key, name=name, config=config)
            session.add(db_destination)
            session.flush()
            bindings = {slot: UUID(rid) for slot, rid in (resources or {}).items()}
            sync_relations(session, db_destination, "resource", bindings)
            session.commit()
            return load_component(session, db_destination.id, kind="destination")

    def update_destination(
        self,
        destination_id: UUID,
        *,
        name: str | None = None,
        config: dict[str, Any] | None = None,
        resources: dict[str, str] | None = None,
    ) -> Component:
        """Update a destination's config/resources."""
        with Session(get_engine()) as session:
            db_destination = session.get(Component, destination_id)
            if not db_destination or db_destination.kind != "destination":
                raise NotFoundError(f"Destination {destination_id} not found")
            if name is not None:
                db_destination.name = name
            if config is not None:
                db_destination.config = config
            if resources is not None:
                bindings = {slot: UUID(rid) for slot, rid in resources.items()}
                sync_relations(session, db_destination, "resource", bindings)
            session.commit()
            return load_component(session, destination_id, kind="destination")

    def delete_destination(self, destination_id: UUID) -> None:
        """Delete a destination. Bindings cascade via the relation FKs."""
        with Session(get_engine()) as session:
            db_destination = session.get(Component, destination_id)
            if db_destination and db_destination.kind == "destination":
                session.delete(db_destination)
                session.commit()
