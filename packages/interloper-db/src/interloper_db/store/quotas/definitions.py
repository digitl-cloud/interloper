"""What a quota is: the registered kinds and how each one decides.

A quota is one :class:`QuotaDefinition` per key, registered in :data:`QUOTAS`.
The definition owns how usage is measured and compared; resolving the
effective limit and calling the gate is :class:`~interloper_db.store.quotas.base.QuotaStore`'s job.

Quotas are code-registered rather than an entry-point group: enforcement is
welded into the store, so they are not a plugin surface. Limits are stored one
row per key, so adding a quota is a registration plus its enforcement site,
never a schema change. Instance defaults live as same-named optional fields on
``QuotaSettings``.
"""

from __future__ import annotations

import abc
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar
from uuid import UUID

from interloper.errors import QuotaExceededError
from interloper.registry import Registry
from sqlalchemy import func
from sqlmodel import Session, col, select

from interloper_db.models import Component
from interloper_db.store.quotas.metering import METRIC_SUCCESSFUL_RUNS, METRICS, UsageLedger

QUOTA_MAX_SOURCES = "max_sources"
QUOTA_MAX_ASSETS_PER_SOURCE = "max_assets_per_source"
QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH = "max_successful_runs_per_month"
QUOTA_MAX_BACKFILL_PARTITIONS = "max_backfill_partitions"


# -- Definitions ---------------------------------------------------------------


@dataclass(frozen=True)
class QuotaDefinition(abc.ABC):
    """One per-organisation quota: its key, label, and check semantics.

    Subclasses own how usage is measured and compared; the
    :class:`~interloper_db.store.quotas.base.QuotaStore` only resolves the effective limit and delegates.
    ``subject`` is caller-supplied context interpolated into the rejection
    message — the part only the call site knows (an entity label, the
    operation being attempted).

    Subclasses that measure existing state set ``requires_lock`` so the limit
    is resolved under the ``(org, key)`` row lock. Consumption checks leave it
    false: their authoritative gate is the atomic ledger reservation, not the
    check.
    """

    key: str
    label: str
    message: Callable[[int, int, str | None], str]

    requires_lock: ClassVar[bool] = False

    def __post_init__(self) -> None:
        """Validate the identity every gate and rejection message depends on.

        Raises:
            ValueError: If the key or the label is empty.
        """
        if not self.key or not self.label:
            raise ValueError("A quota definition needs a key and a label")

    @abc.abstractmethod
    def check(
        self,
        session: Session,
        org_id: UUID,
        limit: int,
        *,
        used: int | None = None,
        subject: str | None = None,
    ) -> None:
        """Raise :class:`QuotaExceededError` when the limit rejects the operation.

        Args:
            session: Open session the work is done through.
            org_id: Organisation the quota is enforced for.
            limit: The effective limit, already resolved by the caller.
            used: Usage stated by the call site, or None to let the definition
                measure it (not every definition can).
            subject: Context interpolated into the rejection message, or None
                when the message needs none.
        """

    def _reject(self, used: int, limit: int, subject: str | None) -> None:
        """Refuse the operation with the definition's own message.

        Args:
            used: The usage figure that breached the limit.
            limit: The effective limit that rejected the operation.
            subject: Context interpolated into the message, or None when the
                message needs none.

        Raises:
            QuotaExceededError: Always; the method exists to raise it.
        """
        raise QuotaExceededError(self.message(used, limit, subject), quota=self.key, limit=limit, used=used)


@dataclass(frozen=True)
class CapacityQuota(QuotaDefinition):
    """Limits how many of something can exist right now (never metered).

    Two check flavors: without ``used`` the current amount is measured via
    ``count`` and admitting one more must stay within the limit; with
    ``used`` the caller states the *desired final* amount (declarative —
    no counting race regardless of what exists today).
    """

    requires_lock: ClassVar[bool] = True

    count: Callable[[Session, UUID], int] | None = None

    def check(
        self,
        session: Session,
        org_id: UUID,
        limit: int,
        *,
        used: int | None = None,
        subject: str | None = None,
    ) -> None:
        """Admit the operation only if capacity stays within the limit.

        Args:
            session: Open session the work is done through.
            org_id: Organisation the quota is enforced for.
            limit: The effective limit, already resolved under the row lock.
            used: The desired final amount, or None to measure the current
                amount and admit one more.
            subject: Context interpolated into the rejection message, or None
                when the message needs none.

        Raises:
            ValueError: If ``used`` is omitted and the definition carries no
                ``count`` callback.
        """
        if used is None:
            if self.count is None:
                raise ValueError(f"Quota '{self.key}' has no usage counter; pass used= to check it declaratively")
            current = self.count(session, org_id)
            if current >= limit:
                self._reject(current, limit, subject)
        elif used > limit:
            self._reject(used, limit, subject)


@dataclass(frozen=True)
class BoundQuota(QuotaDefinition):
    """Bounds a single operation's magnitude (stateless — no count, no ledger).

    The gate always supplies ``used`` (the operation's size); nothing is
    measured or reserved, so no lock is needed: the check is a pure
    comparison against the effective limit.
    """

    def check(
        self,
        session: Session,
        org_id: UUID,
        limit: int,
        *,
        used: int | None = None,
        subject: str | None = None,
    ) -> None:
        """Admit the operation only if its magnitude stays within the limit.

        Args:
            session: Open session the work is done through.
            org_id: Organisation the quota is enforced for, unused for the
                same reason.
            limit: The effective limit, already resolved by the caller.
            used: The operation's size; required, since nothing is measured.
            subject: Context interpolated into the rejection message, or None
                when the message needs none.

        Raises:
            ValueError: If ``used`` is omitted.
        """
        if used is None:
            raise ValueError(f"Quota '{self.key}' bounds a single operation; pass used= with its size")
        if used > limit:
            self._reject(used, limit, subject)


@dataclass(frozen=True)
class ConsumptionQuota(QuotaDefinition):
    """Limits what accumulates per period, charged under ``metric`` in the ledger."""

    metric: str = ""

    def __post_init__(self) -> None:
        """Validate the identity plus the ledger metric this quota charges under.

        Raises:
            ValueError: If the key, the label, or the metric is empty.
        """
        super().__post_init__()
        if not self.metric:
            raise ValueError(f"Consumption quota '{self.key}' needs the ledger metric it charges under")
        if self.metric not in METRICS:
            raise ValueError(f"Consumption quota '{self.key}' charges under unknown metric '{self.metric}'")

    def committed(self, session: Session, org_id: UUID) -> int:
        """The org's committed usage this period: ledger ``used + reserved``.

        Args:
            session: Open session the work is done through.
            org_id: Organisation whose ledger row is read.

        Returns:
            The committed count, or 0 when the period has no ledger row yet.
        """
        return UsageLedger(session).committed(org_id, self.metric)

    def check(
        self,
        session: Session,
        org_id: UUID,
        limit: int,
        *,
        used: int | None = None,
        subject: str | None = None,
    ) -> None:
        """Admit the operation only if committed usage is below the limit.

        Args:
            session: Open session the work is done through.
            org_id: Organisation the quota is enforced for.
            limit: The effective limit, already resolved by the caller.
            used: Committed usage stated by the call site, or None to read it
                from the ledger.
            subject: Context interpolated into the rejection message, or None
                when the message needs none.
        """
        committed = used if used is not None else self.committed(session, org_id)
        if committed >= limit:
            self._reject(committed, limit, subject)

    def reserve(self, session: Session, org_id: UUID, limit: int) -> datetime | None:
        """Atomically reserve one unit of this quota against the period's ledger.

        Args:
            session: Open session the work is done through.
            org_id: Organisation the unit is reserved for.
            limit: The effective limit the reservation is conditioned on.

        Returns:
            The reservation timestamp (DB clock), or None when exhausted.
        """
        return UsageLedger(session).reserve(org_id, self.metric, limit)


# -- Registration --------------------------------------------------------------


QUOTAS: Registry[QuotaDefinition] = Registry()


def _count_sources(session: Session, org_id: UUID) -> int:
    """Current number of sources — the usage side of ``max_sources``.

    Args:
        session: Open session the work is done through.
        org_id: Organisation whose sources are counted.

    Returns:
        The number of source components the organisation owns.
    """
    return session.exec(
        select(func.count())
        .select_from(Component)
        .where(col(Component.org_id) == org_id, col(Component.kind) == "source")
    ).one()


QUOTAS.register(
    QUOTA_MAX_SOURCES,
    CapacityQuota(
        key=QUOTA_MAX_SOURCES,
        label="Max sources",
        count=_count_sources,
        message=lambda used, limit, _subject: f"Organisation is at its source limit ({used}/{limit})",
    ),
)
QUOTAS.register(
    QUOTA_MAX_ASSETS_PER_SOURCE,
    CapacityQuota(
        key=QUOTA_MAX_ASSETS_PER_SOURCE,
        label="Max assets per source",
        message=lambda used, limit, subject: (
            f"Source '{subject}' would have {used} assets, exceeding the limit of {limit}"
        ),
    ),
)
QUOTAS.register(
    QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH,
    ConsumptionQuota(
        key=QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH,
        label="Max successful runs / month",
        metric=METRIC_SUCCESSFUL_RUNS,
        message=lambda used, limit, subject: (
            f"Cannot queue {subject or 'run'}: the monthly successful-run quota is exhausted ({used}/{limit})"
        ),
    ),
)
QUOTAS.register(
    QUOTA_MAX_BACKFILL_PARTITIONS,
    BoundQuota(
        key=QUOTA_MAX_BACKFILL_PARTITIONS,
        label="Max backfill partitions",
        message=lambda used, limit, _subject: f"Backfill spans {used} partitions, exceeding the limit of {limit}",
    ),
)
