"""Destination persistence: CRUD for standalone destination management."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy.orm import selectinload
from sqlmodel import Session, select

from interloper_db.engine import get_engine
from interloper_db.models import Destination, DestinationResource


def _load_destination(session: Session, destination_id: UUID) -> Destination:
    """Load a destination with resources eager-loaded."""
    statement = (
        select(Destination)
        .where(Destination.id == destination_id)
        .options(selectinload(Destination.resources))  # type: ignore[arg-type]
    )
    db_destination = session.exec(statement).first()
    if not db_destination:
        from interloper.errors import NotFoundError

        raise NotFoundError(f"Destination {destination_id} not found")
    return db_destination


def _sync_resource_bindings(
    session: Session,
    db_destination: Destination,
    resources: dict[str, str] | None,
) -> None:
    """Replace all resource bindings for a destination."""
    existing_bindings = session.exec(
        select(DestinationResource).where(DestinationResource.destination_id == db_destination.id)
    ).all()
    for binding in existing_bindings:
        session.delete(binding)
    for key, resource_id in (resources or {}).items():
        session.add(
            DestinationResource(destination_id=db_destination.id, resource_id=UUID(resource_id), key=key)  # type: ignore[arg-type]
        )


class DestinationMixin:
    """Store methods for destination CRUD."""

    def list_destinations(self, org_id: UUID) -> list[Destination]:
        """List all destinations for an organisation."""
        with Session(get_engine()) as session:
            statement = (
                select(Destination)
                .where(Destination.org_id == org_id)
                .options(selectinload(Destination.resources))  # type: ignore[arg-type]
            )
            return list(session.exec(statement).all())

    def get_destination(self, destination_id: UUID) -> Destination:
        """Get a single destination by ID."""
        with Session(get_engine()) as session:
            return _load_destination(session, destination_id)

    def create_destination(
        self,
        org_id: UUID,
        *,
        key: str,
        name: str | None = None,
        config: dict[str, Any] | None = None,
        resources: dict[str, str] | None = None,
    ) -> Destination:
        """Create a standalone destination."""
        with Session(get_engine()) as session:
            db_destination = Destination(
                org_id=org_id,
                key=key,
                name=name,
                config=config,
            )
            session.add(db_destination)
            session.flush()
            created_id = db_destination.id
            _sync_resource_bindings(session, db_destination, resources)
            session.commit()
        with Session(get_engine()) as session:
            return _load_destination(session, created_id)  # type: ignore[arg-type]

    def update_destination(
        self,
        destination_id: UUID,
        *,
        name: str | None = None,
        config: dict[str, Any] | None = None,
        resources: dict[str, str] | None = None,
    ) -> Destination:
        """Update a destination's config/resources."""
        with Session(get_engine()) as session:
            db_destination = session.get(Destination, destination_id)
            if not db_destination:
                from interloper.errors import NotFoundError

                raise NotFoundError(f"Destination {destination_id} not found")
            if name is not None:
                db_destination.name = name
            if config is not None:
                db_destination.config = config
            if resources is not None:
                _sync_resource_bindings(session, db_destination, resources)
            session.commit()
        with Session(get_engine()) as session:
            return _load_destination(session, destination_id)

    def delete_destination(self, destination_id: UUID) -> None:
        """Delete a destination. Source and resource bindings cascade via FK."""
        with Session(get_engine()) as session:
            db_destination = session.get(Destination, destination_id)
            if not db_destination:
                return
            session.delete(db_destination)
            session.commit()
