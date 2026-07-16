"""Store: framework-level persistence for interloper components.

The store bridges the catalog (Python class definitions) and the database
(user-provided instance data). It hydrates framework objects from DB rows
and persists user choices back.

Usage::

    from interloper_db import Store

    store = Store.from_settings(catalog)  # wires connection + encryption from settings

    # Hydrate any component from DB
    source = store.load(source_id)

    # Save a new resource
    store.create_component(org_id, kind="connection", key="demo_connection", ...)

All component kinds persist to the same two tables (``components`` +
``component_relations``) and share one generic surface
(:class:`~interloper_db.store.components.ComponentMixin`): CRUD, relations,
and hydration for any kind, with the semantics a kind genuinely owns
(encryption, child-asset sync, target drift checks) applied by ``kind``.

- ``AuthMixin`` — profiles, sessions, organisations, memberships
- ``TokenMixin`` — personal access tokens (programmatic/MCP access)
- ``ComponentMixin`` — component CRUD, relations, hydration (all kinds)
- ``RunMixin`` — runs, events, backfills
- ``DriftMixin`` — catalog-resolution status for stored keys
"""

from __future__ import annotations

import logging
from typing import Any

from interloper.catalog.base import Catalog
from sqlalchemy import Engine

from interloper_db.engine import engine_from_settings, get_engine
from interloper_db.hydration import Hydrator
from interloper_db.store.auth import AuthMixin
from interloper_db.store.components import ComponentMixin
from interloper_db.store.drift import DriftMixin
from interloper_db.store.runs import RunMixin
from interloper_db.store.tokens import TokenMixin

logger = logging.getLogger(__name__)


class Store(AuthMixin, TokenMixin, ComponentMixin, RunMixin, DriftMixin):
    """Framework-level persistence layer.

    Bridges catalog definitions and database rows to hydrate and persist
    interloper components. All operations use short-lived sessions.
    Hydration is delegated to a :class:`~interloper_db.hydration.Hydrator`
    that builds ``Spec`` trees; reconstruction happens at the
    call site via ``spec.reconstruct()``.
    """

    def __init__(
        self,
        catalog: Catalog,
        engine: Engine | None = None,
        encrypt: Any | None = None,
        decrypt: Any | None = None,
    ) -> None:
        """Initialize the store.

        Args:
            catalog: Catalog instance. Required for hydration.
            engine: Database engine the store operates on. Defaults to the
                already-initialized process engine.
            encrypt: Optional callable ``(data: bytes) -> bytes`` for resource encryption.
            decrypt: Optional callable ``(data: bytes) -> bytes`` for resource decryption.
        """
        self._catalog = catalog
        self._engine = engine or get_engine()
        self._encrypt = encrypt
        self._decrypt = decrypt
        self._hydrator = Hydrator(catalog, decrypt=decrypt)

    @classmethod
    def from_settings(cls, catalog: Catalog | None = None) -> Store:
        """Build a Store with connection and encryption wired from runtime settings.

        The engine is the process engine, initialized from
        ``AppSettings.postgres`` on first use — no prior ``init_engine``
        call is needed. Encryption reads ``INTERLOPER_ENCRYPTION_KEY``:
        when set, the derived cipher is attached so resources are encrypted
        at rest; when unset, the store has no cipher and resource
        persistence fails closed (raising rather than writing secrets in
        plaintext).

        This is the canonical constructor for every long-lived process (API,
        scheduler, runner, agent) — prefer it over ``Store(catalog)`` so the
        connection and crypto wiring stay consistent across entry points.

        Args:
            catalog: Catalog for hydration. Defaults to the
                settings-configured catalog.

        Returns:
            A configured Store.
        """
        from interloper.settings import AppSettings

        catalog = catalog if catalog is not None else Catalog.from_settings()
        engine = engine_from_settings()
        key = AppSettings.get().secrets.encryption_key
        if not key:
            logger.warning(
                "INTERLOPER_ENCRYPTION_KEY is not configured; resource persistence will "
                "fail closed (writes are rejected rather than stored in plaintext). Set it "
                "to enable encrypted resources at rest."
            )
            return cls(catalog=catalog, engine=engine)

        from interloper_db.crypto import make_cipher

        encrypt, decrypt = make_cipher(key)
        return cls(catalog=catalog, engine=engine, encrypt=encrypt, decrypt=decrypt)
