"""Outcomes: how runs ended, per hour and per job.

A unit of work is a run *stack* (an attempt and its retries), and its outcome
is its latest attempt's status; durations and attempt counts span every
attempt. Timestamps pass through ``assume_utc`` before any arithmetic: SQLite
hands timestamp columns back naive.
"""

from __future__ import annotations

import datetime as dt
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from uuid import UUID

from interloper.utils import assume_utc, percentile

from interloper_db.models import Backfill, Run

TERMINAL_STATUSES = frozenset({"success", "failed", "canceled"})


@dataclass(frozen=True)
class HourBucket:
    """Attempts that finished inside one clock hour, by verdict.

    Attributes:
        hour: The start of the hour, aware UTC.
        succeeded: Attempts that succeeded in it.
        failed: Attempts that failed in it.
    """

    hour: dt.datetime
    succeeded: int
    failed: int


@dataclass(frozen=True)
class FinishedRuns:
    """Attempts that finished over the last 24 clock hours, with their hourly profile.

    Attributes:
        total: Attempts that succeeded or failed.
        succeeded: Attempts that succeeded.
        failed: Attempts that failed.
        hourly: The 24 hourly buckets, oldest first.
    """

    total: int
    succeeded: int
    failed: int
    hourly: list[HourBucket]


@dataclass(frozen=True)
class InFlight:
    """What is executing right now.

    Attributes:
        running: Attempts running now.
        queued: Attempts waiting in the queue.
        longest_running_seconds: The longest a running attempt has run, or
            ``None`` when nothing that started is running.
    """

    running: int
    queued: int
    longest_running_seconds: float | None


@dataclass(frozen=True)
class BackfillProgress:
    """Backfills still queued or running, their partitions rolled up.

    Attributes:
        active: Backfills still queued or running.
        partitions_done: Their partitions in a terminal status.
        partitions_total: Their partitions in total.
    """

    active: int
    partitions_done: int
    partitions_total: int


@dataclass(frozen=True)
class Activity:
    """The last 24 clock hours of attempts, what is executing now, and backfill progress.

    The window starts at the top of the hour 23 hours before *now*'s and ends
    at *now*, so the totals always equal the sum of the buckets.

    Attributes:
        runs: The attempts that finished in the window.
        in_flight: What is executing now.
        backfills: The active backfills' progress.
    """

    runs: FinishedRuns
    in_flight: InFlight
    backfills: BackfillProgress

    @classmethod
    def from_runs(
        cls,
        *,
        completed: Iterable[Run],
        running: list[Run],
        queued: int,
        backfills: list[Backfill],
        backfill_counts: Mapping[UUID, Mapping[str, int]],
        now: dt.datetime,
    ) -> Activity:
        """Bucket the finished attempts by completion hour and roll up what is in flight.

        Args:
            completed: Attempts completed around the window; those that did
                not complete inside it, or ended other than in success or
                failure, are left out.
            running: The attempts running now.
            queued: How many attempts wait in the queue.
            backfills: The backfills still queued or running.
            backfill_counts: Each backfill's partitions per status, by backfill id.
            now: The window's end, aware UTC.

        Returns:
            The activity.
        """
        start = now.replace(minute=0, second=0, microsecond=0) - dt.timedelta(hours=23)
        counts: dict[dt.datetime, Counter[str]] = defaultdict(Counter)
        for run in completed:
            if run.completed_at is None or run.status not in ("success", "failed"):
                continue
            finished = assume_utc(run.completed_at).astimezone(dt.timezone.utc)
            if start <= finished <= now:
                counts[finished.replace(minute=0, second=0, microsecond=0)][run.status] += 1
        hours = (start + dt.timedelta(hours=i) for i in range(24))
        hourly = [HourBucket(hour, counts[hour]["success"], counts[hour]["failed"]) for hour in hours]
        succeeded, failed = sum(bucket.succeeded for bucket in hourly), sum(bucket.failed for bucket in hourly)
        return cls(
            runs=FinishedRuns(succeeded + failed, succeeded, failed, hourly),
            in_flight=InFlight(
                running=len(running),
                queued=queued,
                longest_running_seconds=max(
                    ((now - assume_utc(run.started_at)).total_seconds() for run in running if run.started_at),
                    default=None,
                ),
            ),
            backfills=BackfillProgress(
                active=len(backfills),
                partitions_done=sum(
                    count
                    for backfill in backfills
                    for status, count in backfill_counts.get(backfill.id, {}).items()
                    if status in TERMINAL_STATUSES
                ),
                partitions_total=sum(backfill.partitions for backfill in backfills),
            ),
        )


@dataclass(frozen=True)
class JobOutcome:
    """One job's run outcomes over a window.

    ``stacks`` counts each unit of work once, by its latest attempt's status;
    ``attempts`` counts every attempt. A retried stack is ``healed`` when a
    later attempt succeeded and ``still_failing`` when none has.

    Attributes:
        job_id: The job, or ``None`` for runs whose target was deleted.
        job_name: The job's name at read time.
        stacks: Stacks by their latest attempt's status.
        attempts: Every attempt.
        duration_p50_seconds: Median attempt duration, to a tenth of a second.
        duration_p90_seconds: 90th-percentile attempt duration, likewise.
        duration_max_seconds: Longest attempt duration, likewise.
        retried: Stacks with more than one attempt.
        healed: Retried stacks whose latest attempt succeeded.
        still_failing: Retried stacks whose latest attempt failed.
        last_success_at: The latest successful completion in the window.
    """

    job_id: UUID | None
    job_name: str | None
    stacks: dict[str, int]
    attempts: int
    duration_p50_seconds: float | None
    duration_p90_seconds: float | None
    duration_max_seconds: float | None
    retried: int
    healed: int
    still_failing: int
    last_success_at: dt.datetime | None

    @classmethod
    def from_runs(cls, runs: Iterable[Run]) -> list[JobOutcome]:
        """Group attempts into stacks per job and summarise each job, most failures first.

        Args:
            runs: Every attempt in the window, their targets loaded.

        Returns:
            One outcome per job that ran, the most failed stacks first, then by name.
        """
        stacks: dict[UUID | None, dict[UUID, list[Run]]] = defaultdict(lambda: defaultdict(list))
        names: dict[UUID | None, str | None] = {}
        for run in runs:
            stacks[run.component_id][run.root_run_id].append(run)
            names.setdefault(run.component_id, run.target.name if run.target else None)
        outcomes = []
        for job_id, by_root in stacks.items():
            by_status: Counter[str] = Counter()
            durations: list[float] = []
            attempts = retried = healed = still_failing = 0
            last_success: dt.datetime | None = None
            for chain in by_root.values():
                latest = max(chain, key=lambda run: run.attempt)
                by_status[latest.status] += 1
                attempts += len(chain)
                for run in chain:
                    if run.started_at and run.completed_at:
                        seconds = (assume_utc(run.completed_at) - assume_utc(run.started_at)).total_seconds()
                        durations.append(round(seconds, 1))
                    if run.status == "success" and run.completed_at:
                        finished = assume_utc(run.completed_at)
                        last_success = finished if last_success is None else max(last_success, finished)
                if len(chain) > 1:
                    retried += 1
                    healed += latest.status == "success"
                    still_failing += latest.status == "failed"
            outcomes.append(
                cls(
                    job_id=job_id,
                    job_name=names[job_id],
                    stacks=dict(by_status),
                    attempts=attempts,
                    duration_p50_seconds=percentile(durations, 50),
                    duration_p90_seconds=percentile(durations, 90),
                    duration_max_seconds=max(durations, default=None),
                    retried=retried,
                    healed=healed,
                    still_failing=still_failing,
                    last_success_at=last_success,
                )
            )
        return sorted(outcomes, key=lambda outcome: (-outcome.stacks.get("failed", 0), outcome.job_name or ""))
