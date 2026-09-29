"""Tests for ``interloper_toolkit.scheduling``."""

from __future__ import annotations

import datetime
from typing import Any
from uuid import uuid4

from interloper_db import engine as engine_module
from interloper_db.models import Component, Event, Run
from interloper_db.store import Store
from sqlmodel import Session

from interloper_toolkit import ToolkitContext, scheduling


class TestScheduling:
    def test_get_job_health_computes_success_rate(self, ctx: ToolkitContext):
        job = Component(org_id=ctx.org_id, kind="job", key="daily")
        job_id = job.id
        now = datetime.datetime(2026, 7, 16, 12, 0, tzinfo=datetime.timezone.utc)
        runs = [
            Run(
                id=uuid4(),
                org_id=ctx.org_id,
                component_id=job_id,
                status=status,
                started_at=now,
                completed_at=now + datetime.timedelta(seconds=60),
            )
            for status in ("success", "success", "failed")
        ]
        with Session(engine_module.get_engine()) as session:
            session.add_all([job, *runs])
            session.commit()

        result = scheduling.get_job_health(ctx, str(job_id))

        assert result.status == "success"
        assert result.health.success_rate == 0.67
        assert result.health.avg_duration_seconds == 60.0

    def test_get_job_health_of_another_orgs_job_is_not_found(self, ctx: ToolkitContext):
        job = Component(org_id=uuid4(), kind="job", key="daily")
        job_id = job.id
        with Session(engine_module.get_engine()) as session:
            session.add(job)
            session.commit()

        result = scheduling.get_job_health(ctx, str(job_id))

        assert result.status == "error"
        assert "not found" in result.error

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
            event_id = session.exec(__import__("sqlmodel").select(Event.id).where(Event.run_id == run_id)).first()

        assert scheduling.get_event(ctx, str(event_id)).status == "error"


class TestListingTotals:
    def test_list_jobs_and_runs_and_backfills_page_with_totals(self, ctx: ToolkitContext, store: Store):
        jobs = [Component(org_id=ctx.org_id, kind="job", key=f"job_{i}") for i in range(3)]
        runs = [Run(id=uuid4(), org_id=ctx.org_id, component_id=jobs[0].id, status="success") for _ in range(4)]
        with Session(engine_module.get_engine()) as session:
            session.add_all([*jobs, *runs])
            session.commit()
        for _ in range(2):
            store.runs.create_backfill(ctx.org_id, start_key="2026-07-01", end_key="2026-07-01")

        job_page = scheduling.list_jobs(ctx, limit=2, offset=2)
        run_page = scheduling.list_recent_runs(ctx, limit=3)
        backfill_page = scheduling.list_backfills(ctx, active_only=False, limit=1)

        assert job_page.status == "success"
        assert run_page.status == "success"
        assert backfill_page.status == "success"
        assert (job_page.count, job_page.total) == (1, 3)
        assert (run_page.count, run_page.total) == (3, 6)  # the two backfills add a run each
        assert (backfill_page.count, backfill_page.total) == (1, 2)
