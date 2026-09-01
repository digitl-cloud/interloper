"""SQLModel database models for interloper persistence.

The schema mirrors the framework's own model: everything is a Component,
persisted in a single ``components`` table, with typed relations in a single
``component_relations`` table. The catalog (Python class definitions) provides
the schema; the database stores instance data.

Key design decisions:
- One row per component instance; ``kind`` discriminates. New kinds need no
  schema changes.
- Three payload columns with distinct contracts: ``config`` (the spec — user
  intent, the Spec init payload), ``state`` (machine-owned runtime
  state, written only by operators via targeted updates, always safe to
  discard), and ``data`` (Fernet-encrypted secrets).
- Relations carry ``org_id``/``src_kind``/``dst_kind`` denormalized but drift-proof:
  composite foreign keys onto ``UNIQUE (id, org_id, kind)`` force them to match
  the referenced rows, giving DB-level kind- and tenant-safety.
- Auth tables (profiles, organisations, sessions) live alongside data
  models so that ``create_all()`` provisions everything in one shot.
"""

from interloper_db.models.auth import AuthSession, Invitation, Organisation, Profile, UserOrganisation
from interloper_db.models.components import Component, ComponentRelation
from interloper_db.models.quotas import Quota, Usage
from interloper_db.models.runs import Backfill, Event, Execution, Run
from interloper_db.models.tokens import PersonalAccessToken

__all__ = [
    "AuthSession",
    "Backfill",
    "Component",
    "ComponentRelation",
    "Event",
    "Execution",
    "Invitation",
    "Organisation",
    "PersonalAccessToken",
    "Profile",
    "Quota",
    "Run",
    "Usage",
    "UserOrganisation",
]
