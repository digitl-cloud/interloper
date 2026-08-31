"""Tests for the usage ledger (``interloper_db.store.quotas.metering``)."""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from datetime import datetime, timezone
from uuid import UUID

import pytest
from interloper.utils import month_start
from sqlmodel import Session

from interloper_db.models import Run
from interloper_db.store import Store
from interloper_db.store.quotas import METRIC_SUCCESSFUL_RUNS, UsageLedger

UsageRows = Callable[[], dict[dt.date, tuple[int, int]]]
RunFactory = Callable[..., Run]


class TestIncrementUsage:
    def test_creates_then_increments(self, store: Store, org_id: UUID, usage_rows: UsageRows):
        period = dt.date(2026, 8, 1)
        with Session(store.engine) as session:
            UsageLedger(session).increment(org_id, METRIC_SUCCESSFUL_RUNS, period, used=1)
            UsageLedger(session).increment(org_id, METRIC_SUCCESSFUL_RUNS, period, used=1, reserved=2)
            session.commit()
        assert usage_rows() == {period: (2, 2)}

    def test_unknown_metric_rejected(self, store: Store, org_id: UUID):
        with Session(store.engine) as session, pytest.raises(ValueError, match="Unknown usage metric"):
            UsageLedger(session).increment(org_id, "sucessful_runs", dt.date(2026, 8, 1), used=1)


class TestSettleRunUsage:
    def test_success_without_reservation_charges_used(self, store: Store, usage_rows: UsageRows, make_run: RunFactory):
        run = make_run()
        with Session(store.engine) as session:
            UsageLedger(session).settle_run(run, success=True)
            session.commit()
        (counts,) = usage_rows().values()
        assert counts == (1, 0)

    def test_success_with_same_month_reservation_converts_it(
        self,
        store: Store,
        org_id: UUID,
        usage_rows: UsageRows,
        make_run: RunFactory,
    ):
        now = datetime.now(timezone.utc)
        run = make_run(quota_reserved_at=now)
        with Session(store.engine) as session:
            UsageLedger(session).increment(org_id, METRIC_SUCCESSFUL_RUNS, month_start(now), reserved=1)
            UsageLedger(session).settle_run(run, success=True)
            session.commit()
        assert usage_rows() == {month_start(now): (1, 0)}

    def test_failure_with_reservation_releases_it(
        self,
        store: Store,
        org_id: UUID,
        usage_rows: UsageRows,
        make_run: RunFactory,
    ):
        now = datetime.now(timezone.utc)
        run = make_run(quota_reserved_at=now)
        with Session(store.engine) as session:
            UsageLedger(session).increment(org_id, METRIC_SUCCESSFUL_RUNS, month_start(now), reserved=1)
            UsageLedger(session).settle_run(run, success=False)
            session.commit()
        assert usage_rows() == {month_start(now): (0, 0)}

    def test_failure_without_reservation_is_a_no_op(self, store: Store, usage_rows: UsageRows, make_run: RunFactory):
        run = make_run()
        with Session(store.engine) as session:
            UsageLedger(session).settle_run(run, success=False)
            session.commit()
        assert usage_rows() == {}

    def test_cross_month_reservation_settles_both_periods(
        self,
        store: Store,
        org_id: UUID,
        usage_rows: UsageRows,
        make_run: RunFactory,
    ):
        reserved_at = datetime(2026, 7, 31, 23, 59, tzinfo=timezone.utc)
        run = make_run(quota_reserved_at=reserved_at)
        with Session(store.engine) as session:
            UsageLedger(session).increment(org_id, METRIC_SUCCESSFUL_RUNS, dt.date(2026, 7, 1), reserved=1)
            UsageLedger(session).settle_run(run, success=True)
            session.commit()
        rows = usage_rows()
        assert rows[dt.date(2026, 7, 1)] == (0, 0)  # reservation released where taken
        current = month_start(datetime.now(timezone.utc))
        assert rows[current] == (1, 0)  # charge lands in the completion month

    def test_complete_run_charges_the_ledger(self, store: Store, usage_rows: UsageRows, make_run: RunFactory):
        run = make_run()
        completed = store.runs.complete(run.id, success=True)
        assert completed.status == "success"
        (counts,) = usage_rows().values()
        assert counts == (1, 0)


class TestNonBillableRunExemption:
    """Settlement skips a run recorded as non-billable, success or not."""

    def test_settle_charges_nothing_on_success(
        self,
        store: Store,
        usage_rows: UsageRows,
        make_run: RunFactory,
    ):
        run = make_run(billable=False)
        with Session(store.engine) as session:
            UsageLedger(session).settle_run(run, success=True)
            session.commit()

        assert usage_rows() == {}
