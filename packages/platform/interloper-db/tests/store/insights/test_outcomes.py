"""Tests for ``interloper_db.store.insights.outcomes``."""

from __future__ import annotations

import datetime as dt
from uuid import UUID, uuid4

from interloper_db.models import Run
from interloper_db.store.insights.outcomes import Activity, JobOutcome

_T0 = dt.datetime(2026, 9, 29, 4, tzinfo=dt.timezone.utc)


def _attempt(job: UUID, root: UUID | None, attempt: int, status: str, minutes: int) -> Run:
    run = Run(id=uuid4(), org_id=uuid4(), component_id=job, status=status, attempt=attempt)
    run.root_run_id = root or run.id
    run.started_at, run.completed_at = _T0, _T0 + dt.timedelta(minutes=minutes)
    return run


class TestJobOutcome:
    def test_a_stack_counts_once_by_its_latest_attempt(self):
        job = uuid4()
        healed = _attempt(job, None, 1, "failed", 1)
        stuck = _attempt(job, None, 1, "failed", 2)
        runs = [
            healed,
            _attempt(job, healed.id, 2, "success", 3),
            stuck,
            _attempt(job, stuck.id, 2, "failed", 4),
            _attempt(job, None, 1, "success", 5),
        ]

        [outcome] = JobOutcome.from_runs(runs)

        assert outcome.stacks == {"success": 2, "failed": 1}
        assert (outcome.attempts, outcome.retried, outcome.healed, outcome.still_failing) == (5, 2, 1, 1)
        assert (outcome.duration_p50_seconds, outcome.duration_max_seconds) == (180.0, 300.0)
        assert outcome.last_success_at == _T0 + dt.timedelta(minutes=5)

    def test_the_most_failed_job_comes_first(self):
        quiet, loud = uuid4(), uuid4()

        outcomes = JobOutcome.from_runs([_attempt(quiet, None, 1, "success", 1), _attempt(loud, None, 1, "failed", 1)])

        assert [outcome.job_id for outcome in outcomes] == [loud, quiet]


class TestActivity:
    def test_only_attempts_that_succeeded_or_failed_inside_the_window_count(self):
        now = _T0 + dt.timedelta(minutes=30)
        job = uuid4()
        canceled = _attempt(job, None, 1, "canceled", 10)
        unfinished = _attempt(job, None, 1, "failed", 10)
        unfinished.completed_at = None
        stale = _attempt(job, None, 1, "success", -60 * 25)

        activity = Activity.from_runs(
            completed=[_attempt(job, None, 1, "success", 10), canceled, unfinished, stale],
            running=[],
            queued=0,
            backfills=[],
            backfill_counts={},
            now=now,
        )

        assert (activity.runs.total, activity.runs.succeeded, activity.runs.failed) == (1, 1, 0)
        assert activity.runs.hourly[-1].succeeded == 1
