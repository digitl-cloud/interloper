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
from interloper_db.store.backfills import BackfillStore
from interloper_db.store.components import ComponentStore, Hydrator
from interloper_db.store.conversations import ConversationStore
from interloper_db.store.events import EventStore
from interloper_db.store.executions import ExecutionStore
from interloper_db.store.insights import InsightStore
from interloper_db.store.invitations import InvitationStore
from interloper_db.store.members import MemberStore
from interloper_db.store.organisations import OrganisationStore
from interloper_db.store.profiles import ProfileStore
from interloper_db.store.quotas import QuotaStore, UsageStore
from interloper_db.store.relations import RelationStore
from interloper_db.store.runs import RunStore
from interloper_db.store.sessions import SessionStore
from interloper_db.store.tokens import TokenStore

logger = logging.getLogger(__name__)


class Store:
    """Framework-level persistence layer.

    Bridges catalog definitions and database rows to hydrate and persist
    interloper components. The store owns the engine, the catalog and the
    session policy; each entity is a facet reached through it —
    ``store.components``, ``store.runs``, ``store.members`` and so on.
    ``store.components.load`` hydrates a live component through the
    :class:`~interloper_db.store.components.Hydrator`.

    Attributes:
        profiles: Who a person is.
        sessions: The login sessions proving it.
        organisations: The tenants.
        members: Who belongs to an organisation, with which role.
        invitations: Memberships not yet accepted.
        tokens: Personal access tokens.
        conversations: A member's conversations with the agent.
        components: Component CRUD, hydration and catalog status, for every kind.
        relations: The vocabulary-checked edges between components.
        runs: Runs, their attempts and their completion.
        backfills: Batches of runs over a partition range.
        events: What happened during a run.
        executions: Each operation's verdict in a run, derived from its events.
        insights: Health, outcomes, failures and coverage, one definition each.
        quotas: Limit resolution and the enforcement gates.
        usage: The usage ledger and the counts it is reconciled against.
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

        # Each facet is handed what it works through, so its dependencies read
        # off its constructor and nothing reaches back into the store.
        self.profiles = ProfileStore(self._engine)
        self.sessions = SessionStore(self._engine)
        self.organisations = OrganisationStore(self._engine)
        self.members = MemberStore(self._engine)
        self.invitations = InvitationStore(self._engine)
        self.tokens = TokenStore(self._engine, self.members)
        self.conversations = ConversationStore(self._engine)
        self.relations = RelationStore(self._engine, catalog)
        self.quotas = QuotaStore(self._engine, lambda: self._quota_defaults)
        self.usage = UsageStore(self._engine)
        self.backfills = BackfillStore(self._engine, self.quotas)
        self.events = EventStore(self._engine)
        self.runs = RunStore(self._engine, self.quotas, self.backfills, self.events)
        self.executions = ExecutionStore(self._engine)
        self.components = ComponentStore(
            self._engine,
            catalog,
            Hydrator(self._engine, catalog, decrypt=decrypt),
            encrypt,
            decrypt,
            self.quotas,
            self.relations,
        )
        self.insights = InsightStore(self._engine, catalog, self.components, self.runs, self.backfills, self.executions)

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

        Exposed for provisioning and tests; everything else reads and writes
        through the facets.

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
