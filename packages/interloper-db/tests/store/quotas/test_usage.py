"""Tests for the usage ledger reads (``interloper_db.store.quotas.usage``)."""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from datetime import datetime, timezone
from uuid import UUID, uuid4

from interloper.utils import month_start
from sqlmodel import Session

from interloper_db.models import Component, Run
from interloper_db.store import Store, UsageDrift, UsageQuery
from interloper_db.store.quotas import METRIC_SUCCESSFUL_RUNS, UsageLedger

RunFactory = Callable[..., Run]


class TestList:
    def test_list_usage_filters(self, store: Store, org_id: UUID):
        other_org = uuid4()
        with Session(store.engine) as session:
            UsageLedger(session).increment(org_id, METRIC_SUCCESSFUL_RUNS, dt.date(2026, 7, 1), used=1)
            UsageLedger(session).increment(org_id, METRIC_SUCCESSFUL_RUNS, dt.date(2026, 8, 1), used=2)
            UsageLedger(session).increment(other_org, METRIC_SUCCESSFUL_RUNS, dt.date(2026, 8, 1), used=3)
            session.commit()
        august = store.usage.list(UsageQuery(period_start=dt.date(2026, 8, 1), limit=None))
        assert {(row.org_id, row.used) for row in august.items} == {(org_id, 2), (other_org, 3)}
        assert august.total == 2
        july = store.usage.list(UsageQuery(org_id=org_id, period_start=dt.date(2026, 7, 1), limit=None))
        assert [row.used for row in july.items] == [1]

    def test_a_window_reports_the_whole_count(self, store: Store, org_id: UUID):
        with Session(store.engine) as session:
            for month in (6, 7, 8):
                UsageLedger(session).increment(org_id, METRIC_SUCCESSFUL_RUNS, dt.date(2026, month, 1), used=month)
            session.commit()

        page = store.usage.list(UsageQuery(org_id=org_id, limit=1, offset=1))

        assert [row.period_start for row in page.items] == [dt.date(2026, 7, 1)]
        assert page.total == 3


class TestCounts:
    def test_capacity_counts(self, store: Store, org_id: UUID):
        other_org = uuid4()
        with Session(store.engine) as session:
            big = Component(org_id=org_id, kind="source", key="s1")
            small = Component(org_id=org_id, kind="source", key="s2")
            other = Component(org_id=other_org, kind="source", key="s3")
            session.add_all([big, small, other])
            session.flush()
            for key in ("a", "b", "c"):
                session.add(Component(org_id=org_id, kind="asset", key=key, parent_id=big.id))
            session.add(Component(org_id=org_id, kind="asset", key="a", parent_id=small.id))
            session.commit()

        assert store.usage.sources_by_org() == {org_id: 2, other_org: 1}
        assert store.usage.max_assets_per_source_by_org() == {org_id: 3}

    def test_successful_runs_by_org(self, store: Store, org_id: UUID):
        period = dt.date(2026, 8, 1)
        inside = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
        outside = datetime(2026, 7, 31, 12, 0, tzinfo=timezone.utc)
        with Session(store.engine) as session:
            session.add(Run(id=uuid4(), org_id=org_id, status="success", completed_at=inside))
            session.add(Run(id=uuid4(), org_id=org_id, status="success", completed_at=outside))
            session.add(Run(id=uuid4(), org_id=org_id, status="failed", completed_at=inside))
            session.add(Run(id=uuid4(), org_id=org_id, status="success", completed_at=inside, billable=False))
            session.commit()
        assert store.usage.successful_runs_by_org(period) == {org_id: 1}

    def test_current_period_is_this_month(self, store: Store):
        assert store.usage.current_period() == month_start(datetime.now(timezone.utc))


class TestReconcile:
    def test_reports_drift_both_ways(self, store: Store, org_id: UUID):
        period = month_start(datetime.now(timezone.utc))
        with Session(store.engine) as session:
            UsageLedger(session).increment(org_id, METRIC_SUCCESSFUL_RUNS, period, used=5)
            session.commit()
        drifts = store.usage.reconcile()
        assert drifts == [UsageDrift(org_id=org_id, period_start=period, ledger=5, recomputed=0)]
        assert (drifts[0].ledger, drifts[0].recomputed) == (5, 0)

    def test_an_unmetered_success_is_drift_the_other_way(self, store: Store, org_id: UUID, make_run: RunFactory):
        run = make_run()
        with Session(store.engine) as session:
            stored = session.get(Run, run.id)
            assert stored is not None
            stored.status = "success"
            stored.completed_at = datetime.now(timezone.utc)
            session.add(stored)
            session.commit()

        (drift,) = store.usage.reconcile()

        assert isinstance(drift, UsageDrift)
        assert (drift.org_id, drift.ledger, drift.recomputed) == (org_id, 0, 1)

    def test_in_sync_reports_nothing(self, store: Store, make_run: RunFactory):
        run = make_run()
        store.runs.complete(run.id, success=True)
        assert store.usage.reconcile() == []

    def test_a_successful_non_billable_run_is_not_drift(self, store: Store, make_run: RunFactory):
        run = make_run(billable=False)
        store.runs.complete(run.id, success=True)
        assert store.usage.reconcile() == []
