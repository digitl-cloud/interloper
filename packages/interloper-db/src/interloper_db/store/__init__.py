"""Store: framework-level persistence for interloper components.

The store bridges the catalog (Python class definitions) and the database
(user-provided instance data). It hydrates framework objects from DB rows and
persists user choices back.

Usage::

    from interloper_db import Store

    store = Store.from_settings(catalog)

    source = store.components.load(source_id)
    store.components.create(org_id, kind="connection", key="demo", ...)

Each entity is a facet reached through the store, so a caller depends on the
part it uses rather than on all of it:

- ``store.profiles`` — who a person is
- ``store.sessions`` — the login sessions proving it
- ``store.organisations`` — the tenants
- ``store.members`` — who belongs to an organisation, with which role
- ``store.invitations`` — memberships not yet accepted
- ``store.tokens`` — personal access tokens (programmatic/MCP access)
- ``store.conversations`` — a member's conversations with the agent
- ``store.components`` — component CRUD, hydration and catalog status, for every kind
- ``store.relations`` — the vocabulary-checked edges between components
- ``store.runs`` — runs, their attempts and their completion
- ``store.backfills`` — batches of runs over a partition range
- ``store.events`` — what happened during a run
- ``store.executions`` — each operation's verdict in a run, derived from its events
- ``store.insights`` — health, outcomes, failures and coverage, one definition each
- ``store.quotas`` — per-org limits and the enforcement gates
- ``store.usage`` — the usage ledger and the counts it is reconciled against

Every facet speaks the same verbs over its own rows — ``get``, ``list``,
``create``, ``update``, ``delete`` — plus the domain verbs a row genuinely has
(``complete``, ``retry``, ``cancel``, ``revoke``, ``accept``). A listing takes
the scope it reads in (``org_id``, a parent id) and a query object extending
:class:`PageQuery`, and returns a :class:`Page`.

Every facet keeps the same error contract. Anything addressed by primary key
— ``get``, and every mutation — raises :class:`~interloper.errors.NotFoundError`
when the row is absent, so the caller writes the happy path and the API turns
one exception into one 404. Returning ``None`` is reserved for lookups where
absence is an ordinary answer rather than a failure: resolving a session token,
a Google id, or an invitation token, where "no match" is what the caller asked.
A mutation that is deliberately idempotent (clearing a session's org, re-adding
an existing member) says so in its own docstring.
"""

from interloper_db.session import commit, session_scope, transaction
from interloper_db.store.backfills import BackfillQuery, BackfillStore
from interloper_db.store.base import Store
from interloper_db.store.components import ComponentQuery, ComponentReading, ComponentStore, DeleteImpact
from interloper_db.store.conversations import ConversationStore
from interloper_db.store.events import EventQuery, EventStore
from interloper_db.store.executions import ExecutionQuery, ExecutionStore
from interloper_db.store.insights import InsightStore
from interloper_db.store.invitations import InvitationStore
from interloper_db.store.members import MemberStore
from interloper_db.store.organisations import ActivityEntry, OrganisationQuery, OrganisationStore
from interloper_db.store.page import Page, PageQuery
from interloper_db.store.profiles import ProfileStore
from interloper_db.store.quotas import QuotaStore, UsageDrift, UsageQuery, UsageStore
from interloper_db.store.relations import RelationQuery, RelationStore
from interloper_db.store.runs import RunQuery, RunStore
from interloper_db.store.sessions import SessionStore
from interloper_db.store.tokens import TokenQuery, TokenStore

__all__ = [
    "ActivityEntry",
    "BackfillQuery",
    "BackfillStore",
    "ComponentQuery",
    "ComponentReading",
    "ComponentStore",
    "ConversationStore",
    "DeleteImpact",
    "EventQuery",
    "EventStore",
    "ExecutionQuery",
    "ExecutionStore",
    "InsightStore",
    "InvitationStore",
    "MemberStore",
    "OrganisationQuery",
    "OrganisationStore",
    "Page",
    "PageQuery",
    "ProfileStore",
    "QuotaStore",
    "RelationQuery",
    "RelationStore",
    "RunQuery",
    "RunStore",
    "SessionStore",
    "Store",
    "TokenQuery",
    "TokenStore",
    "UsageDrift",
    "UsageQuery",
    "UsageStore",
    "commit",
    "session_scope",
    "transaction",
]
