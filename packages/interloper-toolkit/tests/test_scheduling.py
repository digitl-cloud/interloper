"""Tests for ``interloper_toolkit.scheduling``."""

from __future__ import annotations

import dataclasses
import datetime
from typing import Any
from uuid import uuid4

import pytest
from interloper_db import RunStatus
from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Component, Event, Run
from interloper_db.store import ComponentQuery, RunQuery, Store
from sqlmodel import Session, select

from interloper_toolkit import ToolkitContext, scheduling
from interloper_toolkit.models import ToolError


class TestScheduling:
    def test_get_run_detail_of_another_orgs_run_is_not_found(self, ctx: ToolkitContext):
        run = Run(id=uuid4(), org_id=uuid4(), status="failed")
        run_id = run.id
        with Session(engine_module.get_engine()) as session:
            session.add(run)
            session.commit()

        result = scheduling.get_run_detail(ctx, str(run_id))

        assert result.status == "error"
        assert "not found" in result.error


def _event(org_id: Any, run_id: Any, event_type: str, second: int, **fields: Any) -> Event:
    return Event(
        id=uuid4(),
        org_id=org_id,
        run_id=run_id,
        event_type=event_type,
        component_key="orders",
        timestamp=datetime.datetime(2026, 9, 29, 4, 0, 0, tzinfo=datetime.timezone.utc)
        + datetime.timedelta(seconds=second),
        **fields,
    )


def _job_id(org_id: Any) -> Any:
    """A bare job in *org_id*, the target a backfill needs.

    Returns:
        The job id.
    """
    job = Component(org_id=org_id, kind="job", key="backfilled")
    job_id = job.id
    with Session(engine_module.get_engine()) as session:
        session.add(job)
        session.commit()
    return job_id


def _failed_run_with_many_events(org_id: Any, *, early: int = 150, error: str = "HTTPStatusError: 429") -> Any:
    """A failed run whose failure events sit after *early* lifecycle events, as a wide run's do.

    Returns:
        The run id.
    """
    run = Run(id=uuid4(), org_id=org_id, status="failed")
    run_id = run.id
    events = [_event(org_id, run_id, "operation_queued", i) for i in range(early)]
    events += [
        _event(org_id, run_id, "asset_data_failed", early + 1, error=error),
        _event(org_id, run_id, "operation_failed", early + 2, error=error, traceback="Traceback…\nboom"),
        _event(org_id, run_id, "run_failed", early + 3, error="Run failed (1 operation(s) failed)"),
    ]
    with Session(engine_module.get_engine()) as session:
        session.add(run)
        session.commit()
        session.add_all(events)
        session.commit()
    return run_id


class TestListFailures:
    def test_failure_events_are_found_past_the_first_hundred_events(self, ctx: ToolkitContext):
        _failed_run_with_many_events(ctx.org_id)

        result = scheduling.list_failures(ctx)

        assert result.status == "success"
        assert result.total == 1
        failure = result.failures[0]
        assert failure.error_count == 2
        assert [e.error for e in failure.errors] == ["HTTPStatusError: 429", "Run failed (1 operation(s) failed)"]

    def test_errors_are_clipped_but_counted_whole(self, ctx: ToolkitContext):
        _failed_run_with_many_events(ctx.org_id, early=0, error="x" * 2_000)

        result = scheduling.list_failures(ctx)

        assert result.status == "success"
        failure = result.failures[0]
        assert failure.errors[0].error.startswith("x" * 1_000)
        assert failure.errors[0].error.endswith("…[+1000 chars]")

    def test_pages_report_the_total(self, ctx: ToolkitContext):
        for _ in range(3):
            _failed_run_with_many_events(ctx.org_id, early=0)

        result = scheduling.list_failures(ctx, limit=2, offset=2)

        assert result.status == "success"
        assert (result.count, result.total) == (1, 3)


class TestRunEvents:
    def test_get_run_detail_carries_executions_not_events(self, ctx: ToolkitContext):
        run_id = _failed_run_with_many_events(ctx.org_id)

        result = scheduling.get_run_detail(ctx, str(run_id))

        assert result.status == "success"
        assert not hasattr(result, "events")

    def test_list_run_events_pages_and_filters(self, ctx: ToolkitContext):
        run_id = _failed_run_with_many_events(ctx.org_id, early=150)

        page = scheduling.list_run_events(ctx, str(run_id), limit=10, offset=145)
        errors = scheduling.list_run_events(ctx, str(run_id), errors_only=True)
        typed = scheduling.list_run_events(ctx, str(run_id), event_types=["run_failed"])

        assert page.status == "success"
        assert errors.status == "success"
        assert typed.status == "success"
        assert (page.count, page.total) == (8, 153)
        assert [e.event_type for e in errors.events] == ["asset_data_failed", "operation_failed", "run_failed"]
        assert errors.total == 3
        assert "traceback" not in errors.events[1].model_dump()
        assert typed.total == 1

    def test_list_run_events_of_another_orgs_run_is_not_found(self, ctx: ToolkitContext):
        run_id = _failed_run_with_many_events(uuid4(), early=0)

        result = scheduling.list_run_events(ctx, str(run_id))

        assert result.status == "error"

    def test_get_event_carries_the_traceback_clipped_from_the_tail(self, ctx: ToolkitContext):
        run_id = _failed_run_with_many_events(ctx.org_id, early=0)
        listed = scheduling.list_run_events(ctx, str(run_id), event_types=["operation_failed"])
        assert listed.status == "success"
        failed = listed.events[0]
        with Session(engine_module.get_engine()) as session:
            row = session.get(Event, failed.id)
            assert row is not None
            row.traceback = "head" + "x" * 20_000 + "tail"
            session.add(row)
            session.commit()

        result = scheduling.get_event(ctx, str(failed.id))

        assert result.status == "success"
        assert result.event.traceback is not None
        assert result.event.traceback.startswith("…[+10008 chars]")
        assert result.event.traceback.endswith("tail")

    def test_get_event_of_another_orgs_run_is_not_found(self, ctx: ToolkitContext):
        run_id = _failed_run_with_many_events(uuid4(), early=0)
        with Session(engine_module.get_engine()) as session:
            event_id = session.exec(select(Event.id).where(Event.run_id == run_id)).first()

        assert scheduling.get_event(ctx, str(event_id)).status == "error"


class TestListingTotals:
    def test_list_jobs_and_runs_and_backfills_page_with_totals(self, ctx: ToolkitContext, store: Store):
        jobs = [Component(org_id=ctx.org_id, kind="job", key=f"job_{i}") for i in range(3)]
        job_id = jobs[0].id
        runs = [Run(id=uuid4(), org_id=ctx.org_id, component_id=job_id, status="success") for _ in range(4)]
        with Session(engine_module.get_engine()) as session:
            session.add_all([*jobs, *runs])
            session.commit()
        for _ in range(2):
            store.backfills.create(ctx.org_id, component_id=job_id, start_key="2026-07-01", end_key="2026-07-01")

        job_page = scheduling.list_jobs(ctx, limit=2, offset=2)
        run_page = scheduling.list_recent_runs(ctx, limit=3)
        backfill_page = scheduling.list_backfills(ctx, active_only=False, limit=1)

        assert job_page.status == "success"
        assert run_page.status == "success"
        assert backfill_page.status == "success"
        assert (job_page.count, job_page.total) == (1, 3)
        assert (run_page.count, run_page.total) == (3, 6)  # the two backfills add a run each
        assert (backfill_page.count, backfill_page.total) == (1, 2)


class TestErrorBreakdown:
    def _seed(self, ctx: ToolkitContext) -> tuple[Any, Any]:
        job = Component(org_id=ctx.org_id, kind="job", key="amazon", name="Amazon SP")
        job_id = job.id
        run = Run(id=uuid4(), org_id=ctx.org_id, component_id=job_id, status="failed")
        run_id = run.id
        error = HTTPX = (
            "HTTPStatusError: Client error '429 Too Many Requests' for url "
            "'https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports/1'"
        )
        events = [
            _event(ctx.org_id, run_id, "asset_data_failed", 1, error=error),
            _event(ctx.org_id, run_id, "operation_retried", 2, error=error),
            _event(ctx.org_id, run_id, "asset_data_failed", 3, error=HTTPX.replace("/1'", "/2'")),
            _event(ctx.org_id, run_id, "operation_failed", 4, error=HTTPX.replace("/1'", "/2'")),
            _event(ctx.org_id, run_id, "run_failed", 5, error="Run failed (1 operation(s) failed)"),
        ]
        with Session(engine_module.get_engine()) as session:
            session.add_all([job, run])
            session.commit()
            session.add_all(events)
            session.commit()
        return job_id, run_id

    def test_counts_each_failed_attempt_once_and_merges_by_cause(self, ctx: ToolkitContext):
        job_id, run_id = self._seed(ctx)

        result = scheduling.error_breakdown(ctx, since="2026-09-01")

        assert result.status == "success"
        assert result.total == 2
        top = result.groups[0]
        assert (top.job_id, top.job_name, top.asset_key) == (job_id, "Amazon SP", "orders")
        assert (top.failed_attempts, top.terminal_failures, top.runs_affected) == (2, 1, 1)
        assert top.cause is not None
        assert top.cause.http_status == 429
        assert top.sample_run_id == run_id
        assert result.scan.truncated is False

    def test_grouping_by_job_alone_collapses_assets_and_causes(self, ctx: ToolkitContext):
        self._seed(ctx)

        result = scheduling.error_breakdown(ctx, since="2026-09-01", group_by=["job"])

        assert result.status == "success"
        assert result.total == 1
        assert result.groups[0].failed_attempts == 3
        assert result.groups[0].asset_key is None
        assert result.groups[0].cause is None

    def test_a_run_scope_opens_the_window(self, ctx: ToolkitContext):
        _, run_id = self._seed(ctx)

        result = scheduling.error_breakdown(ctx, run_id=str(run_id))

        assert result.status == "success"
        assert result.since is None
        assert result.total == 2

    def test_unknown_group_keys_are_refused(self, ctx: ToolkitContext):
        assert scheduling.error_breakdown(ctx, group_by=["asset", "colour"]).status == "error"


class TestBackfillTimeline:
    def test_observed_concurrency_and_timing(self, ctx: ToolkitContext, store: Store):
        backfill = store.backfills.create(
            ctx.org_id, component_id=_job_id(ctx.org_id), start_key="2026-07-01", end_key="2026-07-03", concurrency=1
        )
        t0 = datetime.datetime(2026, 9, 29, 4, 0, tzinfo=datetime.timezone.utc)
        with Session(engine_module.get_engine()) as session:
            row = session.get(Backfill, backfill.id)
            assert row is not None
            row.created_at = t0
            session.add(row)
            runs = session.exec(select(Run).where(Run.backfill_id == backfill.id)).all()
            for i, run in enumerate(sorted(runs, key=lambda r: r.partition_key or "")):
                run.created_at = t0
                run.started_at = t0 + datetime.timedelta(minutes=i)
                run.completed_at = t0 + datetime.timedelta(minutes=10 + i * 10)
                run.status = RunStatus.SUCCESS
                session.add(run)
            session.commit()

        result = scheduling.backfill_timeline(ctx, str(backfill.id), limit=2)

        assert result.status == "success"
        assert result.backfill.concurrency == 1
        assert result.max_concurrent_runs == 3
        assert result.first_start_lag_s == 0.0
        assert (result.duration_p50_s, result.duration_p90_s) == (1140.0, 1680.0)
        assert result.runs_by_status == {"success": 3}
        assert (result.count, result.total) == (2, 3)
        assert [a.partition_key for a in result.attempts] == ["2026-07-01", "2026-07-02"]
        assert result.attempts[1].queue_wait_s == 60.0

    def test_another_orgs_backfill_is_not_found(self, ctx: ToolkitContext, store: Store):
        theirs = uuid4()
        backfill = store.backfills.create(
            theirs, component_id=_job_id(theirs), start_key="2026-07-01", end_key="2026-07-01"
        )

        assert scheduling.backfill_timeline(ctx, str(backfill.id)).status == "error"


class TestWrites:
    def _job(self, ctx: ToolkitContext, store: Store) -> Any:
        connection = store.components.create(
            ctx.org_id, kind="connection", key="demo_connection", config={}, encrypted=False
        )
        source = store.components.create(
            ctx.org_id, kind="source", key="shop_source", relations={"connection": [connection.id], "destinations": []}
        )
        return store.components.create(
            ctx.org_id,
            kind="job",
            key="cron_job",
            name="Daily",
            config={"cron": "0 6 * * *"},
            relations={"targets": [source.id]},
        )

    def test_toggles_a_job_and_an_asset(self, ctx: ToolkitContext, store: Store):
        job = self._job(ctx, store)
        asset = store.components.list(ctx.org_id, ComponentQuery(kind=["asset"], roots_only=False)).items[0]

        off = scheduling.toggle_job(ctx, str(job.id), False)
        asset_off = scheduling.toggle_asset(ctx, str(asset.id), False)

        assert off.status == "success"
        assert asset_off.status == "success"
        assert (off.enabled, off.component.name) == (False, "Daily")
        assert (store.components.get(job.id).config or {})["enabled"] is False
        assert (store.components.get(asset.id).config or {})["enabled"] is False

    def test_queues_a_run_and_a_backfill(self, ctx: ToolkitContext, store: Store):
        job = self._job(ctx, store)

        run = scheduling.trigger_run(ctx, str(job.id), partition_key="2026-07-01")
        backfill = scheduling.trigger_backfill(ctx, str(job.id), "2026-07-01", "2026-07-03", concurrency=2)

        assert run.status == "success"
        assert backfill.status == "success"
        assert run.run.partition_key == "2026-07-01"
        assert (backfill.backfill.partitions, backfill.backfill.concurrency) == (3, 2)
        assert store.runs.list(ctx.org_id, RunQuery(component_id=job.id)).total == 4

    @pytest.mark.parametrize(
        "write",
        [
            lambda ctx, job_id: scheduling.toggle_job(ctx, job_id, False),
            lambda ctx, job_id: scheduling.trigger_run(ctx, job_id),
            lambda ctx, job_id: scheduling.trigger_backfill(ctx, job_id, "2026-07-01", "2026-07-01"),
        ],
    )
    def test_writes_refuse_a_viewer_and_another_orgs_job(self, ctx: ToolkitContext, store: Store, write: Any):
        job = self._job(ctx, store)
        theirs = self._job(dataclasses.replace(ctx, org_id=uuid4()), store)

        assert isinstance(write(dataclasses.replace(ctx, role="viewer"), str(job.id)), ToolError)
        assert write(ctx, str(theirs.id)).status == "error"
        assert store.runs.list(ctx.org_id, RunQuery()).total == 0

    def test_retries_a_failed_run_as_the_next_attempt(self, ctx: ToolkitContext, store: Store):
        job = self._job(ctx, store)
        failed = store.runs.create(ctx.org_id, component_id=job.id)
        store.runs.complete(failed.id, success=False)
        fine = store.runs.create(ctx.org_id, component_id=job.id)
        store.runs.complete(fine.id, success=True)

        result = scheduling.retry_run(ctx, str(failed.id), scope="failed")
        not_failed = scheduling.retry_run(ctx, str(fine.id))

        assert result.status == "success"
        assert (result.run.attempt, result.run.root_run_id, result.run.status) == (2, failed.id, "queued")
        assert not_failed.status == "error"
        assert scheduling.retry_run(ctx, str(failed.id), scope="sometimes").status == "error"

    def test_cancels_a_run_that_has_not_ended(self, ctx: ToolkitContext, store: Store):
        run = store.runs.create(ctx.org_id, component_id=self._job(ctx, store).id)

        result = scheduling.cancel_run(ctx, str(run.id))
        again = scheduling.cancel_run(ctx, str(run.id))

        assert result.status == "success"
        assert result.run.status == "canceled"
        assert again.status == "error"

    def test_cancels_a_backfills_undispatched_runs(self, ctx: ToolkitContext, store: Store):
        backfill = store.backfills.create(
            ctx.org_id, component_id=_job_id(ctx.org_id), start_key="2026-07-01", end_key="2026-07-03", concurrency=1
        )

        result = scheduling.cancel_backfill(ctx, str(backfill.id))
        again = scheduling.cancel_backfill(ctx, str(backfill.id))

        assert result.status == "success"
        assert result.runs_canceled == 3
        assert result.backfill.status == "canceled"
        assert again.status == "error"

    def test_retry_and_cancel_refuse_a_viewer_and_another_org(self, ctx: ToolkitContext, store: Store):
        viewer = dataclasses.replace(ctx, role="viewer")
        theirs = uuid4()
        run = store.runs.create(theirs)
        store.runs.complete(run.id, success=False)
        backfill = store.backfills.create(
            theirs, component_id=_job_id(theirs), start_key="2026-07-01", end_key="2026-07-01"
        )

        assert isinstance(scheduling.retry_run(viewer, str(run.id)), ToolError)
        assert isinstance(scheduling.cancel_run(viewer, str(run.id)), ToolError)
        assert isinstance(scheduling.cancel_backfill(viewer, str(backfill.id)), ToolError)
        assert scheduling.retry_run(ctx, str(run.id)).status == "error"
        assert scheduling.cancel_run(ctx, str(run.id)).status == "error"
        assert scheduling.cancel_backfill(ctx, str(backfill.id)).status == "error"
        assert store.runs.list(theirs, RunQuery(all_attempts=True)).total == 2
        assert store.backfills.get(backfill.id).status == "running"
