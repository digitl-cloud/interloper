"""Tests for quota limits and usage metering (``interloper_db.store.quotas``)."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from interloper.errors import QuotaExceededError
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, select

from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Component, Quota, Run, Usage
from interloper_db.store.quotas import (
    METRIC_SUCCESSFUL_RUNS,
    QuotaMixin,
    increment_usage,
    month_start,
    next_month_start,
    settle_run_usage,
    try_reserve_run,
)
from interloper_db.store.runs import RunMixin

_ORG_ID = uuid4()


class _Store(RunMixin, QuotaMixin):
    """The run + quota slice of the full store, for integration tests."""


@pytest.fixture
def store() -> Iterator[_Store]:
    """A quota-capable store over a fresh in-memory SQLite database."""
    eng = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(eng, "connect")
    def _sqlite_uuid(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    for model in (Component, Backfill, Run, Quota, Usage):
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    try:
        mixin = _Store()
        mixin._engine = eng
        yield mixin
    finally:
        eng.dispose()
        engine_module._engine = None


def _usage_rows(store: _Store) -> dict[dt.date, tuple[int, int]]:
    with Session(store._engine) as session:
        rows = session.exec(select(Usage).where(Usage.org_id == _ORG_ID)).all()
        return {row.period_start: (row.used, row.reserved) for row in rows}


def _run(store: _Store, *, quota_reserved_at: datetime | None = None) -> Run:
    with Session(store._engine) as session:
        run = Run(id=uuid4(), org_id=_ORG_ID, status="running", quota_reserved_at=quota_reserved_at)
        session.add(run)
        session.commit()
        session.refresh(run)
        return run


class TestPeriods:
    def test_month_start_normalizes_to_utc(self):
        # 00:30 Berlin on July 1st is still June in UTC.
        berlin = timezone(dt.timedelta(hours=2))
        assert month_start(datetime(2026, 7, 1, 0, 30, tzinfo=berlin)) == dt.date(2026, 6, 1)
        assert month_start(datetime(2026, 7, 1, 0, 30)) == dt.date(2026, 7, 1)  # naive = UTC

    def test_next_month_start_rolls_the_year(self):
        assert next_month_start(dt.date(2026, 12, 1)) == dt.date(2027, 1, 1)
        assert next_month_start(dt.date(2026, 1, 1)) == dt.date(2026, 2, 1)


class TestIncrementUsage:
    def test_creates_then_increments(self, store: _Store):
        period = dt.date(2026, 8, 1)
        with Session(store._engine) as session:
            increment_usage(session, _ORG_ID, METRIC_SUCCESSFUL_RUNS, period, used=1)
            increment_usage(session, _ORG_ID, METRIC_SUCCESSFUL_RUNS, period, used=1, reserved=2)
            session.commit()
        assert _usage_rows(store) == {period: (2, 2)}

    def test_unknown_metric_rejected(self, store: _Store):
        with Session(store._engine) as session:
            with pytest.raises(ValueError, match="Unknown usage metric"):
                increment_usage(session, _ORG_ID, "sucessful_runs", dt.date(2026, 8, 1), used=1)


class TestSettleRunUsage:
    def test_success_without_reservation_charges_used(self, store: _Store):
        run = _run(store)
        with Session(store._engine) as session:
            settle_run_usage(session, run, success=True)
            session.commit()
        (counts,) = _usage_rows(store).values()
        assert counts == (1, 0)

    def test_success_with_same_month_reservation_converts_it(self, store: _Store):
        now = datetime.now(timezone.utc)
        run = _run(store, quota_reserved_at=now)
        with Session(store._engine) as session:
            increment_usage(session, _ORG_ID, METRIC_SUCCESSFUL_RUNS, month_start(now), reserved=1)
            settle_run_usage(session, run, success=True)
            session.commit()
        assert _usage_rows(store) == {month_start(now): (1, 0)}

    def test_failure_with_reservation_releases_it(self, store: _Store):
        now = datetime.now(timezone.utc)
        run = _run(store, quota_reserved_at=now)
        with Session(store._engine) as session:
            increment_usage(session, _ORG_ID, METRIC_SUCCESSFUL_RUNS, month_start(now), reserved=1)
            settle_run_usage(session, run, success=False)
            session.commit()
        assert _usage_rows(store) == {month_start(now): (0, 0)}

    def test_failure_without_reservation_is_a_no_op(self, store: _Store):
        run = _run(store)
        with Session(store._engine) as session:
            settle_run_usage(session, run, success=False)
            session.commit()
        assert _usage_rows(store) == {}

    def test_cross_month_reservation_settles_both_periods(self, store: _Store):
        reserved_at = datetime(2026, 7, 31, 23, 59, tzinfo=timezone.utc)
        run = _run(store, quota_reserved_at=reserved_at)
        with Session(store._engine) as session:
            increment_usage(session, _ORG_ID, METRIC_SUCCESSFUL_RUNS, dt.date(2026, 7, 1), reserved=1)
            settle_run_usage(session, run, success=True)
            session.commit()
        rows = _usage_rows(store)
        assert rows[dt.date(2026, 7, 1)] == (0, 0)  # reservation released where taken
        current = month_start(datetime.now(timezone.utc))
        assert rows[current] == (1, 0)  # charge lands in the completion month

    def test_complete_run_charges_the_ledger(self, store: _Store):
        run = _run(store)
        completed = store.complete_run(run.id, success=True)
        assert completed.status == "success"
        (counts,) = _usage_rows(store).values()
        assert counts == (1, 0)


class TestQuotaReads:
    def test_get_and_list_quotas(self, store: _Store):
        assert store.get_quota(_ORG_ID) is None
        with Session(store._engine) as session:
            session.add(Quota(org_id=_ORG_ID, max_sources=3))
            session.commit()
        quota = store.get_quota(_ORG_ID)
        assert quota is not None and quota.max_sources == 3
        assert [q.org_id for q in store.list_quotas()] == [_ORG_ID]

    def test_list_usage_filters(self, store: _Store):
        other_org = uuid4()
        with Session(store._engine) as session:
            increment_usage(session, _ORG_ID, METRIC_SUCCESSFUL_RUNS, dt.date(2026, 7, 1), used=1)
            increment_usage(session, _ORG_ID, METRIC_SUCCESSFUL_RUNS, dt.date(2026, 8, 1), used=2)
            increment_usage(session, other_org, METRIC_SUCCESSFUL_RUNS, dt.date(2026, 8, 1), used=3)
            session.commit()
        august = store.list_usage(period_start=dt.date(2026, 8, 1))
        assert {(row.org_id, row.used) for row in august} == {(_ORG_ID, 2), (other_org, 3)}
        assert [row.used for row in store.list_usage(org_id=_ORG_ID, period_start=dt.date(2026, 7, 1))] == [1]

    def test_capacity_counts(self, store: _Store):
        other_org = uuid4()
        with Session(store._engine) as session:
            big = Component(org_id=_ORG_ID, kind="source", key="s1")
            small = Component(org_id=_ORG_ID, kind="source", key="s2")
            other = Component(org_id=other_org, kind="source", key="s3")
            session.add_all([big, small, other])
            session.flush()
            for key in ("a", "b", "c"):
                session.add(Component(org_id=_ORG_ID, kind="asset", key=key, parent_id=big.id))
            session.add(Component(org_id=_ORG_ID, kind="asset", key="a", parent_id=small.id))
            session.commit()

        assert store.count_sources_by_org() == {_ORG_ID: 2, other_org: 1}
        assert store.max_assets_per_source_by_org() == {_ORG_ID: 3}

    def test_count_successful_runs_by_org(self, store: _Store):
        period = dt.date(2026, 8, 1)
        inside = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
        outside = datetime(2026, 7, 31, 12, 0, tzinfo=timezone.utc)
        with Session(store._engine) as session:
            session.add(Run(id=uuid4(), org_id=_ORG_ID, status="success", completed_at=inside))
            session.add(Run(id=uuid4(), org_id=_ORG_ID, status="success", completed_at=outside))
            session.add(Run(id=uuid4(), org_id=_ORG_ID, status="failed", completed_at=inside))
            session.commit()
        assert store.count_successful_runs_by_org(period) == {_ORG_ID: 1}

    def test_current_period_start_is_this_month(self, store: _Store):
        assert store.current_period_start() == month_start(datetime.now(timezone.utc))


# -- Enforcement ------------------------------------------------------------------


def _defaults(**limits: int | None) -> SimpleNamespace:
    return SimpleNamespace(**limits)


class TestRunCreationGate:
    def _exhaust(self, store: _Store, *, used: int = 0, reserved: int = 0) -> None:
        with Session(store._engine) as session:
            period = month_start(datetime.now(timezone.utc))
            increment_usage(session, _ORG_ID, METRIC_SUCCESSFUL_RUNS, period, used=used, reserved=reserved)
            session.commit()

    def test_create_run_blocked_at_limit(self, store: _Store):
        store._quota_defaults = _defaults(max_successful_runs_per_month=2)
        self._exhaust(store, used=1, reserved=1)  # reserved counts against the limit
        with pytest.raises(QuotaExceededError) as excinfo:
            store.create_run(_ORG_ID)
        assert excinfo.value.quota == "max_successful_runs_per_month"
        assert (excinfo.value.limit, excinfo.value.used) == (2, 2)

    def test_create_run_allowed_below_limit(self, store: _Store):
        store._quota_defaults = _defaults(max_successful_runs_per_month=2)
        self._exhaust(store, used=1)
        assert store.create_run(_ORG_ID).status == "queued"

    def test_org_override_wins_over_default(self, store: _Store):
        store._quota_defaults = _defaults(max_successful_runs_per_month=1)
        with Session(store._engine) as session:
            session.add(Quota(org_id=_ORG_ID, max_successful_runs_per_month=3))
            session.commit()
        self._exhaust(store, used=2)
        assert store.create_run(_ORG_ID).status == "queued"

    def test_retry_blocked_at_limit(self, store: _Store):
        run = store.create_run(_ORG_ID)
        store.complete_run(run.id, success=False)
        store._quota_defaults = _defaults(max_successful_runs_per_month=1)
        self._exhaust(store, used=1)
        with pytest.raises(QuotaExceededError, match="retry"):
            store.retry_run(run.id)

    def test_backfill_blocked_at_limit(self, store: _Store):
        store._quota_defaults = _defaults(max_successful_runs_per_month=1)
        self._exhaust(store, used=1)
        with pytest.raises(QuotaExceededError, match="backfill"):
            store.create_backfill(_ORG_ID, start_date=dt.date(2026, 1, 1), end_date=dt.date(2026, 1, 2))

    def test_backfill_span_cap(self, store: _Store):
        store._quota_defaults = _defaults(max_backfill_days=2)
        with pytest.raises(ValueError, match="caps backfills at 2 days"):
            store.create_backfill(_ORG_ID, start_date=dt.date(2026, 1, 1), end_date=dt.date(2026, 1, 3))
        backfill = store.create_backfill(_ORG_ID, start_date=dt.date(2026, 1, 1), end_date=dt.date(2026, 1, 2))
        assert backfill.partitions == 2


class TestTryReserveRun:
    def test_unlimited_admits_without_ledger(self, store: _Store):
        run = _run(store)
        with Session(store._engine) as session:
            db_run = session.get(Run, run.id)
            assert db_run is not None
            assert try_reserve_run(session, db_run, None) is True
            session.commit()
        assert _usage_rows(store) == {}

    def test_reserves_and_stamps(self, store: _Store):
        run = _run(store)
        with Session(store._engine) as session:
            db_run = session.get(Run, run.id)
            assert db_run is not None
            assert try_reserve_run(session, db_run, _defaults(max_successful_runs_per_month=1)) is True
            session.commit()
        (counts,) = _usage_rows(store).values()
        assert counts == (0, 1)
        with Session(store._engine) as session:
            reserved = session.get(Run, run.id)
            assert reserved is not None and reserved.quota_reserved_at is not None

    def test_denies_when_exhausted(self, store: _Store):
        with Session(store._engine) as session:
            period = month_start(datetime.now(timezone.utc))
            increment_usage(session, _ORG_ID, METRIC_SUCCESSFUL_RUNS, period, used=1)
            session.commit()
        run = _run(store)
        with Session(store._engine) as session:
            db_run = session.get(Run, run.id)
            assert db_run is not None
            assert try_reserve_run(session, db_run, _defaults(max_successful_runs_per_month=1)) is False
            session.commit()
        (counts,) = _usage_rows(store).values()
        assert counts == (1, 0)
        with Session(store._engine) as session:
            released = session.get(Run, run.id)
            assert released is not None and released.quota_reserved_at is None

    def test_zero_limit_denies(self, store: _Store):
        run = _run(store)
        with Session(store._engine) as session:
            db_run = session.get(Run, run.id)
            assert db_run is not None
            assert try_reserve_run(session, db_run, _defaults(max_successful_runs_per_month=0)) is False


class TestReconcileUsage:
    def test_reports_drift_both_ways(self, store: _Store):
        period = month_start(datetime.now(timezone.utc))
        with Session(store._engine) as session:
            increment_usage(session, _ORG_ID, METRIC_SUCCESSFUL_RUNS, period, used=5)
            session.commit()
        drifts = store.reconcile_usage()
        assert drifts == [{"org_id": _ORG_ID, "period_start": period, "ledger": 5, "recomputed": 0}]

    def test_in_sync_reports_nothing(self, store: _Store):
        run = _run(store)
        store.complete_run(run.id, success=True)
        assert store.reconcile_usage() == []
