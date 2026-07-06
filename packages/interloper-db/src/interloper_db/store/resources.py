"""Resource persistence: CRUD, hydration, and encryption.

Resources are components whose payload lives in the encrypted ``data``
column rather than ``config`` — user credentials never touch a plaintext
column unless explicitly opted out.
"""

from __future__ import annotations

import json
from typing import Any, cast
from uuid import UUID

import interloper as il
from interloper.errors import ConfigError, HydrationError, ResourceNotFoundError
from sqlmodel import Session, col, select

from interloper_db.engine import get_engine
from interloper_db.hydration import SECRET_KINDS, Hydrator
from interloper_db.models import Component


class ResourceMixin:
    """Store methods for resource management."""

    _encrypt: Any
    _decrypt: Any
    _hydrator: Hydrator

    def _encode_data(self, data: dict[str, Any], encrypted: bool | None) -> tuple[bytes, bool]:
        """Serialise ``data`` and encrypt it according to ``encrypted``.

        Args:
            data: Resource configuration dict.
            encrypted: ``True`` or ``None`` (default) to encrypt — both require
                a configured key; ``False`` to opt out and store plaintext.

        Returns:
            A ``(blob, encrypted)`` tuple: the bytes to persist and whether
            they are encrypted.

        Raises:
            ConfigError: If encryption is required (the default, or an explicit
                ``True``) but no encryption key is configured. Fails closed so
                secrets are never silently written in plaintext.
        """
        should_encrypt = True if encrypted is None else encrypted
        raw = json.dumps(data).encode()
        if should_encrypt:
            if not self._encrypt:
                raise ConfigError(
                    "Refusing to store a resource without encryption at rest: "
                    "INTERLOPER_ENCRYPTION_KEY is not configured. Set it, or pass "
                    "encrypted=false to store this resource in plaintext."
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
    ) -> Component:
        """Create a new resource.

        Args:
            org_id: Organisation UUID.
            kind: Functional category (e.g. ``"connection"``, ``"config"``).
            key: Catalog key identifying the resource class.
            name: User-facing label.
            data: Resource configuration dict.
            encrypted: ``True`` or ``None`` (default) to encrypt (requires a
                configured key); ``False`` to opt out and store plaintext.

        Returns:
            The saved component row.
        """
        raw, encrypted = self._encode_data(data, encrypted)

        with Session(get_engine()) as session:
            db_resource = Component(
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
    ) -> Component:
        """Update an existing resource.

        Args:
            resource_id: The resource UUID.
            kind: Functional category.
            key: Catalog key.
            name: User-facing label.
            data: Resource configuration dict.
            encrypted: ``True`` or ``None`` (default) to encrypt (requires a
                configured key); ``False`` to opt out and store plaintext.

        Returns:
            The updated component row.

        Raises:
            ResourceNotFoundError: If the resource is not found.
        """
        raw, encrypted = self._encode_data(data, encrypted)

        with Session(get_engine()) as session:
            db_resource = session.get(Component, resource_id)
            if not db_resource or db_resource.kind not in SECRET_KINDS:
                raise ResourceNotFoundError(f"Resource {resource_id} not found")
            db_resource.kind = kind
            db_resource.key = key
            db_resource.name = name
            db_resource.data = raw
            db_resource.encrypted = encrypted
            session.commit()
            session.refresh(db_resource)
            return db_resource

    def load_resource(self, resource_id: UUID) -> Component:
        """Load a resource row by ID.

        Args:
            resource_id: The resource UUID.

        Returns:
            The component row.

        Raises:
            ResourceNotFoundError: If the resource is not found.
        """
        with Session(get_engine()) as session:
            db_resource = session.get(Component, resource_id)
            if not db_resource or db_resource.kind not in SECRET_KINDS:
                raise ResourceNotFoundError(f"Resource {resource_id} not found")
            return db_resource

    def list_resources(self, org_id: UUID, kind: str | None = None) -> list[Component]:
        """List resources, optionally filtered by kind.

        Args:
            org_id: Organisation UUID.
            kind: Optional resource kind filter.

        Returns:
            List of matching component rows.
        """
        with Session(get_engine()) as session:
            statement = select(Component).where(
                Component.org_id == org_id,
                col(Component.kind).in_([kind] if kind else list(SECRET_KINDS)),
            )
            return list(session.exec(statement).all())

    def delete_resource(self, resource_id: UUID) -> None:
        """Delete a resource by ID. Bindings cascade via the relation FKs.

        Args:
            resource_id: The resource UUID.
        """
        with Session(get_engine()) as session:
            db_resource = session.get(Component, resource_id)
            if db_resource:
                session.delete(db_resource)
                session.commit()

    def hydrate_resource(self, db_resource: Component) -> il.Resource:
        """Hydrate a framework Resource from a DB row.

        Delegates to :class:`~interloper_db.hydration.Hydrator` to build a
        ``ComponentSpec``, then reconstructs the live instance via
        ``spec.reconstruct()``.

        Args:
            db_resource: The DB component row.

        Returns:
            The hydrated framework Resource instance.
        """
        with Session(get_engine()) as session:
            spec = self._hydrator.build_component_spec(session, db_resource)
        try:
            return cast(il.Resource, spec.reconstruct())
        except Exception as e:
            raise HydrationError(
                f"Failed to hydrate resource '{db_resource.key}' ({db_resource.id}): {e}"
            ) from e

    def decode_resource_data(self, db_resource: Component) -> dict[str, Any]:
        """Return the decoded ``data`` blob of a resource row.

        Thin wrapper around :meth:`Hydrator.decode_data` for API consumers
        that want the raw config dict (e.g. for display in an admin UI)
        without reconstructing the Resource.

        Args:
            db_resource: The DB component row.

        Returns:
            The decoded (and decrypted, when applicable) config dict.
        """
        return self._hydrator.decode_data(db_resource)
