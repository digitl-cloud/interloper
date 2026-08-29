"""Store: framework-level persistence for interloper components.

The store bridges the catalog (Python class definitions) and the database
(user-provided instance data). It hydrates framework objects from DB rows and
persists user choices back.

Usage::

    from interloper_db import Store

    store = Store.from_settings(catalog)

    source = store.components.load(source_id)
    store.components.create(org_id, kind="connection", key="demo", ...)

Each area of the schema is a facet reached through the store, so a caller
depends on the part it uses rather than on all of it:

- ``store.auth`` — profiles, sessions, organisations, memberships, invitations
- ``store.tokens`` — personal access tokens (programmatic/MCP access)
- ``store.components`` — component CRUD, hydration and catalog status, for every kind
- ``store.relations`` — the vocabulary-checked edges between components
- ``store.events`` — run events and asset executions
- ``store.runs`` — runs and backfills
- ``store.quotas`` — per-org limits, enforcement gates, the usage ledger
"""

from interloper_db.session import commit, session_scope, transaction
from interloper_db.store.auth import AuthStore
from interloper_db.store.base import Store
from interloper_db.store.components import ComponentStore
from interloper_db.store.events import EventStore
from interloper_db.store.quotas import QuotaStore
from interloper_db.store.relations import RelationStore
from interloper_db.store.runs import RunStore
from interloper_db.store.tokens import TokenStore

__all__ = [
    "AuthStore",
    "ComponentStore",
    "EventStore",
    "QuotaStore",
    "RelationStore",
    "RunStore",
    "Store",
    "TokenStore",
    "commit",
    "session_scope",
    "transaction",
]
