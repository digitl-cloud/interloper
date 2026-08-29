"""Store: the framework's persistence layer and the facets it is made of."""

from __future__ import annotations

import logging
from contextlib import AbstractContextManager
from typing import Any

from interloper.catalog.base import Catalog
from sqlalchemy import Engine
from sqlmodel import Session

from interloper_db.engine import engine_from_settings, get_engine
from interloper_db.session import transaction
from interloper_db.store.auth import AuthStore
from interloper_db.store.components import ComponentStore
from interloper_db.store.events import EventStore
from interloper_db.store.hydration import Hydrator
from interloper_db.store.organisations import OrganisationStore
from interloper_db.store.quotas import QuotaStore
from interloper_db.store.relations import RelationStore
from interloper_db.store.runs import RunStore
from interloper_db.store.tokens import TokenStore

logger = logging.getLogger(__name__)


class Store:
    """Framework-level persistence layer.

    Bridges catalog definitions and database rows to hydrate and persist
    interloper components. The store owns the engine, the catalog and the
    session policy; each area of the schema is a facet reached through it —
    ``store.components``, ``store.runs``, ``store.auth`` and so on. Hydration
    is delegated to a :class:`~interloper_db.hydration.Hydrator` that builds
    ``Spec`` trees; reconstruction happens at the call site via
    ``spec.reconstruct()``.

    Attributes:
        auth: Profiles, sessions, organisations, members and invitations.
        tokens: Personal access tokens.
        relations: The vocabulary-checked edges between components.
        components: Component CRUD and hydration, for every kind.
        events: Run events and the asset executions derived from them.
        runs: Runs and backfills.
        drift: Whether a stored key still resolves against the catalog.
        quotas: Limit resolution, enforcement gates and the usage ledger.
    """

    def __init__(
        self,
        catalog: Catalog,
        engine: Engine | None = None,
        encrypt: Any | None = None,
        decrypt: Any | None = None,
        quota_defaults: Any | None = None,
    ) -> None:
        """Initialize the store.

        Args:
            catalog: Catalog instance. Required for hydration.
            engine: Database engine the store operates on. Defaults to the
                already-initialized process engine.
            encrypt: Optional callable ``(data: bytes) -> bytes`` for resource encryption.
            decrypt: Optional callable ``(data: bytes) -> bytes`` for resource decryption.
            quota_defaults: QuotaSettings-shaped default limits enforced when
                an organisation has no override. None = everything unlimited.
        """
        self._catalog = catalog
        self._engine = engine or get_engine()
        self._encrypt = encrypt
        self._decrypt = decrypt
        self._quota_defaults = quota_defaults
        self._hydrator = Hydrator(catalog, decrypt=decrypt)

        # Each facet is handed what it works through, so its dependencies read
        # off its constructor and nothing reaches back into the store.
        self.auth = AuthStore(self._engine)
        self.organisations = OrganisationStore(self._engine)
        self.tokens = TokenStore(self._engine, self.organisations)
        self.relations = RelationStore(self._engine, catalog)
        self.quotas = QuotaStore(self._engine, lambda: self._quota_defaults)
        self.events = EventStore(self._engine)
        self.runs = RunStore(self._engine, self.quotas)
        self.components = ComponentStore(
            self._engine, catalog, self._hydrator, encrypt, self.quotas, self.relations
        )

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
        settings = AppSettings.get()
        key = settings.secrets.encryption_key
        if not key:
            logger.warning(
                "INTERLOPER_ENCRYPTION_KEY is not configured; resource persistence will "
                "fail closed (writes are rejected rather than stored in plaintext). Set it "
                "to enable encrypted resources at rest."
            )
            return cls(catalog=catalog, engine=engine, quota_defaults=settings.quota)

        from interloper_db.crypto import make_cipher

        encrypt, decrypt = make_cipher(key)
        return cls(catalog=catalog, engine=engine, encrypt=encrypt, decrypt=decrypt, quota_defaults=settings.quota)

    # -- Session policy --------------------------------------------------------

    @property
    def engine(self) -> Engine:
        """The engine this store operates on.

        Exposed so co-located machinery (the scheduler's claim/sweep SQL) runs
        against the same engine as the store, not an ambient global.

        Returns:
            The engine.
        """
        return self._engine

    def transaction(self) -> AbstractContextManager[Session]:
        """Run several store calls as one atomic unit of work.

        Returns:
            A context manager yielding the session the calls will share.
        """
        return transaction(self._engine)
