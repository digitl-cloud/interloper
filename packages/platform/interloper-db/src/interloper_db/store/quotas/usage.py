"""Usage reads: what the ledger holds, and what the live tables say it should.

The ledger (:mod:`.metering`) is written in the transactions that charge it;
this facet only reads it, alongside the counts recomputed from the live tables
that the ledger and the capacity quotas are checked against.
"""

from __future__ import annotations

import builtins
import datetime as dt
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from interloper.utils import add_months
from sqlalchemy import Engine, func
from sqlalchemy import select as sa_select
from sqlmodel import col, select

from interloper_db.models import Component, Run, RunStatus, Usage
from interloper_db.session import session_scope
from interloper_db.store.page import Page, PageQuery
from interloper_db.store.quotas.metering import METRIC_SUCCESSFUL_RUNS, UsageLedger


class UsageQuery(PageQuery):
    """Which ledger rows a listing reads.

    Attributes:
        period_start: Keep the rows of the UTC month starting on this day.
        org_id: Keep this organisation's rows.
    """

    period_start: dt.date | None = None
    org_id: UUID | None = None


@dataclass(frozen=True)
class UsageDrift:
    """An organisation whose ledger disagrees with the runs table this period.

    Attributes:
        org_id: The organisation.
        period_start: First day of the UTC month compared.
        ledger: The successful runs the ledger has charged.
        recomputed: The successful billable runs the runs table holds.
    """

    org_id: UUID
    period_start: dt.date
    ledger: int
    recomputed: int


class UsageStore:
    """Store methods for reading the usage ledger and the counts behind it."""

    def __init__(self, engine: Engine) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
        """
        self._engine = engine

    def list(self, query: UsageQuery) -> Page[Usage]:
        """List ledger rows, one per ``(org, metric, period)``.

        Args:
            query: Which period and organisation, and the window to read.

        Returns:
            The page of ledger rows, by period then organisation.
        """
        statement = select(Usage).order_by(col(Usage.period_start), col(Usage.org_id), col(Usage.metric))
        if query.period_start is not None:
            statement = statement.where(Usage.period_start == query.period_start)
        if query.org_id is not None:
            statement = statement.where(Usage.org_id == query.org_id)
        with session_scope(self._engine) as session:
            return Page.read(session, statement, query)

    def current_period(self) -> dt.date:
        """The UTC calendar month usage is currently charged into, per the database clock.

        Returns:
            The first day of that month.
        """
        with session_scope(self._engine) as session:
            return UsageLedger(session).current_period()

    def successful_runs_by_org(self, period_start: dt.date) -> dict[UUID, int]:
        """Recompute the ledger's truth: successful billable runs completed in the period.

        Counts on ``completed_at`` — the nearest column to the charge moment.
        Non-billable runs are left out, as settlement never charges them.

        Args:
            period_start: First day of the UTC month to recompute, whose
                following month bounds the window.

        Returns:
            The successful billable-run count keyed by org id; organisations
            with none are absent.
        """
        lower = datetime.combine(period_start, dt.time.min, tzinfo=timezone.utc)
        upper = datetime.combine(add_months(period_start, 1), dt.time.min, tzinfo=timezone.utc)
        statement = (
            select(Run.org_id, func.count())
            .where(
                Run.status == RunStatus.SUCCESS,
                col(Run.billable).is_(True),
                col(Run.completed_at) >= lower,
                col(Run.completed_at) < upper,
            )
            .group_by(col(Run.org_id))
        )
        with session_scope(self._engine) as session:
            return dict(session.exec(statement).all())

    def sources_by_org(self) -> dict[UUID, int]:
        """Current number of sources per organisation.

        Returns:
            The source count keyed by org id; organisations with no sources
            are absent.
        """
        statement = (
            select(Component.org_id, func.count()).where(Component.kind == "source").group_by(col(Component.org_id))
        )
        with session_scope(self._engine) as session:
            return dict(session.exec(statement).all())

    def max_assets_per_source_by_org(self) -> dict[UUID, int]:
        """The largest child-asset count of any single source, per organisation.

        Returns:
            The peak per-source asset count keyed by org id; organisations
            with no parented assets are absent.
        """
        per_source = (
            sa_select(col(Component.org_id).label("org_id"), func.count().label("n"))
            .where(col(Component.kind) == "asset", col(Component.parent_id).is_not(None))
            .group_by(col(Component.org_id), col(Component.parent_id))
        ).subquery()
        statement = sa_select(per_source.c.org_id, func.max(per_source.c.n)).group_by(per_source.c.org_id)
        with session_scope(self._engine) as session:
            return {org_id: count for org_id, count in session.execute(statement).all()}  # ty: ignore[deprecated]

    def reconcile(self) -> builtins.list[UsageDrift]:
        """Compare the current period's ledger against the runs table.

        Both sides move in the same transaction on completion, so persistent
        drift is a bug signal; transient off-by-ones are possible (the two
        reads are separate queries, and charge months are DB-clock while
        ``completed_at`` is the executor's clock at the boundary).

        Returns:
            One entry per drifting organisation, ordered by org id; empty when
            the ledger agrees with the runs table.
        """
        # Scoped, though the handle is unused: the reads below join it, so the
        # ledger and the runs table are compared at one point in time.
        with session_scope(self._engine):
            period_start = self.current_period()
            recomputed = self.successful_runs_by_org(period_start)
            ledger = {
                row.org_id: row.used
                for row in self.list(UsageQuery(period_start=period_start, limit=None)).items
                if row.metric == METRIC_SUCCESSFUL_RUNS
            }
        return [
            UsageDrift(org_id, period_start, ledger.get(org_id, 0), recomputed.get(org_id, 0))
            for org_id in sorted(set(recomputed) | set(ledger), key=str)
            if recomputed.get(org_id, 0) != ledger.get(org_id, 0)
        ]
