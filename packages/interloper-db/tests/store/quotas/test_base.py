"""Tests for limit resolution and the enforcement gates (``interloper_db.store.quotas.base``)."""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from interloper.errors import QuotaExceededError
from interloper.utils import month_start
from sqlmodel import Session

from interloper_db.models import Component, Quota, Run
from interloper_db.store import Store
from interloper_db.store.quotas import METRIC_SUCCESSFUL_RUNS, UsageLedger

UsageRows = Callable[[], dict[dt.date, tuple[int, int]]]
RunFactory = Callable[..., Run]


def _defaults(**limits: int | None) -> SimpleNamespace:
    return SimpleNamespace(**limits)


class TestQuotaReads:
    def test_get_and_list_overrides(self, store: Store, org_id: UUID):
        assert store.quotas.get_quota_overrides(org_id) == {}
        with Session(store.engine) as session:
            session.add(Quota(org_id=org_id, key="max_sources", limit=3))
            session.add(Quota(org_id=org_id, key="max_assets_per_source", limit=None))  # cleared/anchor row
            session.commit()
        assert store.quotas.get_quota_overrides(org_id) == {"max_sources": 3}
        assert store.quotas.list_quota_overrides() == {org_id: {"max_sources": 3}}

    def test_list_usage_filters(self, store: Store, org_id: UUID):
        other_org = uuid4()
        with Session(store.engine) as session:
            UsageLedger(session).increment(org_id, METRIC_SUCCESSFUL_RUNS, dt.date(2026, 7, 1), used=1)
            UsageLedger(session).increment(org_id, METRIC_SUCCESSFUL_RUNS, dt.date(2026, 8, 1), used=2)
            UsageLedger(session).increment(other_org, METRIC_SUCCESSFUL_RUNS, dt.date(2026, 8, 1), used=3)
            session.commit()
        august = store.quotas.list_usage(period_start=dt.date(2026, 8, 1))
        assert {(row.org_id, row.used) for row in august} == {(org_id, 2), (other_org, 3)}
        assert [row.used for row in store.quotas.list_usage(org_id=org_id, period_start=dt.date(2026, 7, 1))] == [1]

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

        assert store.quotas.count_sources_by_org() == {org_id: 2, other_org: 1}
        assert store.quotas.max_assets_per_source_by_org() == {org_id: 3}

    def test_count_successful_runs_by_org(self, store: Store, org_id: UUID):
        period = dt.date(2026, 8, 1)
        inside = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
        outside = datetime(2026, 7, 31, 12, 0, tzinfo=timezone.utc)
        with Session(store.engine) as session:
            session.add(Run(id=uuid4(), org_id=org_id, status="success", completed_at=inside))
            session.add(Run(id=uuid4(), org_id=org_id, status="success", completed_at=outside))
            session.add(Run(id=uuid4(), org_id=org_id, status="failed", completed_at=inside))
            session.commit()
        assert store.quotas.count_successful_runs_by_org(period) == {org_id: 1}

    def test_current_period_start_is_this_month(self, store: Store):
        assert store.quotas.current_period_start() == month_start(datetime.now(timezone.utc))


class TestRunCreationGate:
    def _exhaust(self, store: Store, org_id: UUID, *, used: int = 0, reserved: int = 0) -> None:
        with Session(store.engine) as session:
            period = month_start(datetime.now(timezone.utc))
            UsageLedger(session).increment(org_id, METRIC_SUCCESSFUL_RUNS, period, used=used, reserved=reserved)
            session.commit()

    def test_create_run_blocked_at_limit(self, store: Store, org_id: UUID):
        store._quota_defaults = _defaults(max_successful_runs_per_month=2)
        self._exhaust(store, org_id, used=1, reserved=1)  # reserved counts against the limit
        with pytest.raises(QuotaExceededError) as excinfo:
            store.runs.create(org_id)
        assert excinfo.value.quota == "max_successful_runs_per_month"
        assert (excinfo.value.limit, excinfo.value.used) == (2, 2)

    def test_create_run_allowed_below_limit(self, store: Store, org_id: UUID):
        store._quota_defaults = _defaults(max_successful_runs_per_month=2)
        self._exhaust(store, org_id, used=1)
        assert store.runs.create(org_id).status == "queued"

    def test_org_override_wins_over_default(self, store: Store, org_id: UUID):
        store._quota_defaults = _defaults(max_successful_runs_per_month=1)
        with Session(store.engine) as session:
            session.add(Quota(org_id=org_id, key="max_successful_runs_per_month", limit=3))
            session.commit()
        self._exhaust(store, org_id, used=2)
        assert store.runs.create(org_id).status == "queued"

    def test_retry_blocked_at_limit(self, store: Store, org_id: UUID):
        run = store.runs.create(org_id)
        store.runs.complete(run.id, success=False)
        store._quota_defaults = _defaults(max_successful_runs_per_month=1)
        self._exhaust(store, org_id, used=1)
        with pytest.raises(QuotaExceededError, match="retry"):
            store.runs.retry(run.id)

    def test_backfill_blocked_at_limit(self, store: Store, org_id: UUID):
        store._quota_defaults = _defaults(max_successful_runs_per_month=1)
        self._exhaust(store, org_id, used=1)
        with pytest.raises(QuotaExceededError, match="backfill"):
            store.runs.create_backfill(org_id, start_key="2026-01-01", end_key="2026-01-02")

    def test_backfill_span_override_wins(self, store: Store, org_id: UUID):
        store._quota_defaults = _defaults(max_backfill_partitions=2)
        with Session(store.engine) as session:
            session.add(Quota(org_id=org_id, key="max_backfill_partitions", limit=3))
            session.commit()
        backfill = store.runs.create_backfill(org_id, start_key="2026-01-01", end_key="2026-01-03")
        assert backfill.partitions == 3

    def test_backfill_span_cap(self, store: Store, org_id: UUID):
        store._quota_defaults = _defaults(max_backfill_partitions=2)
        with pytest.raises(QuotaExceededError, match="exceeding the limit of 2"):
            store.runs.create_backfill(org_id, start_key="2026-01-01", end_key="2026-01-03")
        backfill = store.runs.create_backfill(org_id, start_key="2026-01-01", end_key="2026-01-02")
        assert backfill.partitions == 2


class TestTryReserveRun:
    """The reservation joins the dispatching caller's unit of work when there is one."""

    def test_reservation_is_durable_without_an_enclosing_transaction(
        self,
        store: Store,
        usage_rows: UsageRows,
        make_run: RunFactory,
    ):
        """Called on its own the reservation must persist, not vanish at scope exit."""
        store._quota_defaults = _defaults(max_successful_runs_per_month=1)
        run = make_run()
        assert store.quotas.try_reserve_run(run) is True
        with Session(store.engine) as session:
            reserved = session.get(Run, run.id)
            assert reserved is not None and reserved.quota_reserved_at is not None
        (counts,) = usage_rows().values()
        assert counts == (0, 1)

    def test_unlimited_admits_without_ledger(self, store: Store, usage_rows: UsageRows, make_run: RunFactory):
        with store.transaction():
            assert store.quotas.try_reserve_run(make_run()) is True
        assert usage_rows() == {}

    def test_reserves_and_stamps(self, store: Store, usage_rows: UsageRows, make_run: RunFactory):
        store._quota_defaults = _defaults(max_successful_runs_per_month=1)
        run = make_run()
        with store.transaction():
            assert store.quotas.try_reserve_run(run) is True
        (counts,) = usage_rows().values()
        assert counts == (0, 1)
        with Session(store.engine) as session:
            reserved = session.get(Run, run.id)
            assert reserved is not None and reserved.quota_reserved_at is not None

    def test_denies_when_exhausted(self, store: Store, org_id: UUID, usage_rows: UsageRows, make_run: RunFactory):
        with Session(store.engine) as session:
            period = month_start(datetime.now(timezone.utc))
            UsageLedger(session).increment(org_id, METRIC_SUCCESSFUL_RUNS, period, used=1)
            session.commit()
        store._quota_defaults = _defaults(max_successful_runs_per_month=1)
        run = make_run()
        with store.transaction():
            assert store.quotas.try_reserve_run(run) is False
        (counts,) = usage_rows().values()
        assert counts == (1, 0)
        with Session(store.engine) as session:
            released = session.get(Run, run.id)
            assert released is not None and released.quota_reserved_at is None

    def test_zero_limit_denies(self, store: Store, make_run: RunFactory):
        store._quota_defaults = _defaults(max_successful_runs_per_month=0)
        with store.transaction():
            assert store.quotas.try_reserve_run(make_run()) is False


class TestReconcileUsage:
    def test_reports_drift_both_ways(self, store: Store, org_id: UUID):
        period = month_start(datetime.now(timezone.utc))
        with Session(store.engine) as session:
            UsageLedger(session).increment(org_id, METRIC_SUCCESSFUL_RUNS, period, used=5)
            session.commit()
        drifts = store.quotas.reconcile_usage()
        assert drifts == [{"org_id": org_id, "period_start": period, "ledger": 5, "recomputed": 0}]

    def test_in_sync_reports_nothing(self, store: Store, make_run: RunFactory):
        run = make_run()
        store.runs.complete(run.id, success=True)
        assert store.quotas.reconcile_usage() == []


class TestSetQuota:
    def test_creates_then_partially_updates(self, store: Store, org_id: UUID):
        assert store.quotas.set_quota(org_id, {"max_sources": 5}) == {"max_sources": 5}
        assert store.quotas.set_quota(org_id, {"max_successful_runs_per_month": 100}) == {
            "max_sources": 5,
            "max_successful_runs_per_month": 100,
        }

    def test_none_clears_an_override(self, store: Store, org_id: UUID):
        store.quotas.set_quota(org_id, {"max_sources": 5})
        assert store.quotas.set_quota(org_id, {"max_sources": None}) == {}

    def test_rejects_unknown_and_negative(self, store: Store, org_id: UUID):
        with pytest.raises(ValueError, match="Unknown quota limit"):
            store.quotas.set_quota(org_id, {"max_bananas": 1})
        with pytest.raises(ValueError, match=">= 0"):
            store.quotas.set_quota(org_id, {"max_sources": -1})
