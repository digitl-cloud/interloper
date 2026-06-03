"""Resource persistence: CRUD, hydration, and encryption."""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import interloper as il
from interloper.errors import ConfigError, HydrationError, ResourceNotFoundError
from sqlmodel import Session, select

from interloper_db.engine import get_engine
from interloper_db.hydration import Hydrator
from interloper_db.models import Resource


class ResourceMixin:
    """Store methods for resource management."""

    _encrypt: Any
    _decrypt: Any
    _hydrator: Hydrator

    def _encode_data(self, data: dict[str, Any], encrypted: bool | None) -> tuple[bytes, bool]:
        """Serialise ``data`` and encrypt it according to ``encrypted``.

        Args:
            data: Resource configuration dict.
            encrypted: ``True`` to require encryption, ``False`` to force
                plaintext, or ``None`` (default) to encrypt only when an
                encryption key is configured.

        Returns:
            A ``(blob, encrypted)`` tuple: the bytes to persist and whether
            they are encrypted.

        Raises:
            ConfigError: If encryption is explicitly requested but no
                encryption key is configured.
        """
        should_encrypt = self._encrypt is not None if encrypted is None else encrypted
        raw = json.dumps(data).encode()
        if should_encrypt:
            if not self._encrypt:
                raise ConfigError(
                    "Cannot store an encrypted resource: INTERLOPER_ENCRYPTION_KEY is not configured"
                )
            raw = self._encrypt(raw)
        return raw, should_encrypt

    def create_resource(
        self,
        org_id: UUID,
        *,
        kind: str,
        key: str,
        name: str,
        data: dict[str, Any],
        encrypted: bool | None = None,
    ) -> Resource:
        """Create a new resource.

        Args:
            org_id: Organisation UUID.
            kind: Functional category (e.g. ``"connection"``, ``"config"``).
            key: Catalog key identifying the resource class.
            name: User-facing label.
            data: Resource configuration dict.
            encrypted: ``True`` to require encryption, ``False`` to force
                plaintext, or ``None`` (default) to encrypt when an
                encryption key is configured.

        Returns:
            The saved Resource row.
        """
        raw, encrypted = self._encode_data(data, encrypted)

        with Session(get_engine()) as session:
            db_resource = Resource(
                org_id=org_id,
                kind=kind,
                key=key,
                name=name,
                data=raw,
                encrypted=encrypted,
            )
            session.add(db_resource)
            session.commit()
            session.refresh(db_resource)
            return db_resource

    def update_resource(
        self,
        resource_id: UUID,
        *,
        kind: str,
        key: str,
        name: str,
        data: dict[str, Any],
        encrypted: bool | None = None,
    ) -> Resource:
        """Update an existing resource.

        Args:
            resource_id: The resource UUID.
            kind: Functional category.
            key: Catalog key.
            name: User-facing label.
            data: Resource configuration dict.
            encrypted: ``True`` to require encryption, ``False`` to force
                plaintext, or ``None`` (default) to encrypt when an
                encryption key is configured.

        Returns:
            The updated Resource row.

        Raises:
            ResourceNotFoundError: If the resource is not found.
        """
        raw, encrypted = self._encode_data(data, encrypted)

        with Session(get_engine()) as session:
            db_resource = session.get(Resource, resource_id)
            if not db_resource:
                raise ResourceNotFoundError(f"Resource {resource_id} not found")
            db_resource.kind = kind
            db_resource.key = key
            db_resource.name = name
            db_resource.data = raw
            db_resource.encrypted = encrypted
            session.commit()
            session.refresh(db_resource)
            return db_resource

    def load_resource(self, resource_id: UUID) -> Resource:
        """Load a resource row by ID.

        Args:
            resource_id: The resource UUID.

        Returns:
            The Resource row.

        Raises:
            ResourceNotFoundError: If the resource is not found.
        """
        with Session(get_engine()) as session:
            db_resource = session.get(Resource, resource_id)
            if not db_resource:
                raise ResourceNotFoundError(f"Resource {resource_id} not found")
            return db_resource

    def list_resources(self, org_id: UUID, kind: str | None = None) -> list[Resource]:
        """List resources, optionally filtered by kind.

        Args:
            org_id: Organisation UUID.
            kind: Optional resource kind filter.

        Returns:
            List of matching Resource rows.
        """
        with Session(get_engine()) as session:
            statement = select(Resource).where(Resource.org_id == org_id)
            if kind:
                statement = statement.where(Resource.kind == kind)
            return list(session.exec(statement).all())

    def delete_resource(self, resource_id: UUID) -> None:
        """Delete a resource by ID. Source and destination bindings cascade via FK.

        Args:
            resource_id: The resource UUID.
        """
        with Session(get_engine()) as session:
            db_resource = session.get(Resource, resource_id)
            if db_resource:
                session.delete(db_resource)
                session.commit()

    def hydrate_resource(self, db_resource: Resource) -> il.Resource:
        """Hydrate a framework Resource from a DB row.

        Delegates to :class:`~interloper_db.hydration.Hydrator` to build a
        ``ComponentSpec``, then reconstructs the live instance via
        ``spec.reconstruct()``.

        Args:
            db_resource: The DB Resource row.

        Returns:
            The hydrated framework Resource instance.
        """
        spec = self._hydrator.build_resource_spec(db_resource)
        try:
            return spec.reconstruct()  # type: ignore[return-value]
        except Exception as e:
            raise HydrationError(
                f"Failed to hydrate resource '{db_resource.key}' ({db_resource.id}): {e}"
            ) from e

    def decode_resource_data(self, db_resource: Resource) -> dict[str, Any]:
        """Return the decoded ``data`` blob of a Resource row.

        Thin wrapper around :meth:`Hydrator.decode_resource_data` for
        API consumers that want the raw config dict (e.g. for display
        in an admin UI) without reconstructing the Resource.

        Args:
            db_resource: The DB Resource row.

        Returns:
            The decoded (and decrypted, when applicable) config dict.
        """
        return self._hydrator.decode_resource_data(db_resource)
