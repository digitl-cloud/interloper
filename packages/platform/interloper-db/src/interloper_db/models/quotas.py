"""Per-organisation limits and the usage ledger they are charged against."""

import datetime as dt
from typing import ClassVar
from uuid import UUID

from sqlmodel import Field as SQLField
from sqlmodel import SQLModel


class Quota(SQLModel, table=True):
    """Per-organisation quota limit overrides, one row per quota key.

    Rows are created lazily; a null ``limit`` is a cleared override (and the
    enforcement lock anchor) — resolution falls back to the global default
    (``QuotaSettings``), and null there means unlimited. New quota keys need
    no schema change; the valid set is ``QUOTA_KEYS`` in the store. Bare
    ``org_id`` (no FK) like every org-scoped data table.
    """

    __tablename__: ClassVar[str] = "quotas"

    org_id: UUID = SQLField(primary_key=True)
    key: str = SQLField(primary_key=True)
    limit: int | None = None


class Usage(SQLModel, table=True):
    """Per-period usage counters — the append-only ledger billing reads.

    ``used`` counts successful runs, charged at completion; ``reserved``
    holds dispatch-time reservations not yet settled. Rows are never
    deleted — deliberately not even with their organisation — so billing
    history survives everything. Counters are only ever moved by atomic
    increments; drift against the ``runs`` table is a bug signal, not
    something to clamp away.
    """

    __tablename__: ClassVar[str] = "usage"

    org_id: UUID = SQLField(primary_key=True)
    metric: str = SQLField(primary_key=True)
    period_start: dt.date = SQLField(primary_key=True)
    used: int = 0
    reserved: int = 0
