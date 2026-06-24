"""Store: framework-level persistence for interloper components.

The store bridges the catalog (Python class definitions) and the database
(user-provided instance data). It hydrates framework objects from DB rows
and persists user choices back.

Usage::

    from interloper_db import Store, init_engine

    init_engine("postgresql://...")
    store = Store.from_settings(catalog)  # wires encryption from settings

    # Hydrate a source from DB
    source = store.load_source(source_id)

    # Save a new resource
    store.create_resource(org_id, kind="demo_config", name="My Config", ...)

Implementation is split across domain-specific mixins:

- ``AuthMixin`` — profiles, sessions, organisations, memberships
- ``ResourceMixin`` — resource CRUD, hydration, encryption
- ``SourceMixin`` — source CRUD, hydration, graph management
- ``AssetMixin`` — asset get/list/update, dependencies
- ``JobMixin`` — job CRUD
- ``RunMixin`` — runs, events, backfills
"""

from __future__ import annotations

import logging
from typing import Any

from interloper.catalog.base import Catalog

from interloper_db.drift import DriftMixin
from interloper_db.hydration import Hydrator
from interloper_db.store.assets import AssetMixin
from interloper_db.store.auth import AuthMixin
from interloper_db.store.destinations import DestinationMixin
from interloper_db.store.jobs import JobMixin
from interloper_db.store.resources import ResourceMixin
from interloper_db.store.runs import RunMixin
from interloper_db.store.sources import SourceMixin

logger = logging.getLogger(__name__)


class Store(AuthMixin, ResourceMixin, SourceMixin, AssetMixin, JobMixin, RunMixin, DestinationMixin, DriftMixin):
    """Framework-level persistence layer.

    Bridges catalog definitions and database rows to hydrate and persist
    interloper components. All operations use short-lived sessions.
    Hydration is delegated to a :class:`~interloper_db.hydration.Hydrator`
    that builds ``ComponentSpec`` trees; reconstruction happens at the
    call site via ``spec.reconstruct()``.
    """

    def __init__(
        self,
        catalog: Catalog,
        encrypt: Any | None = None,
        decrypt: Any | None = None,
    ) -> None:
        """Initialize the store.

        Args:
            catalog: Catalog instance. Required for hydration.
            encrypt: Optional callable ``(data: bytes) -> bytes`` for resource encryption.
            decrypt: Optional callable ``(data: bytes) -> bytes`` for resource decryption.
        """
        self._catalog = catalog
        self._encrypt = encrypt
        self._decrypt = decrypt
        self._hydrator = Hydrator(catalog, decrypt=decrypt)

    @classmethod
    def from_settings(cls, catalog: Catalog) -> Store:
        """Build a Store with encryption wired from runtime settings.

        Reads ``INTERLOPER_ENCRYPTION_KEY`` via :class:`AppSettings`. When set, the
        derived cipher is attached so resources are encrypted at rest; when
        unset, the store has no cipher and resource persistence fails closed
        (raising rather than writing secrets in plaintext).

        This is the canonical constructor for every long-lived process (API,
        scheduler, runner, agent) — prefer it over ``Store(catalog)`` so the
        crypto wiring stays consistent across entry points.

        Args:
            catalog: Catalog instance. Required for hydration.

        Returns:
            A configured Store.
        """
        from interloper.settings import AppSettings

        key = AppSettings.get().secrets.encryption_key
        if not key:
            logger.warning(
                "INTERLOPER_ENCRYPTION_KEY is not configured; resource persistence will "
                "fail closed (writes are rejected rather than stored in plaintext). Set it "
                "to enable encrypted resources at rest."
            )
            return cls(catalog=catalog)

        from interloper_db.crypto import make_cipher

        encrypt, decrypt = make_cipher(key)
        return cls(catalog=catalog, encrypt=encrypt, decrypt=decrypt)
