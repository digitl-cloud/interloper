"""Cron job: a workload on a cron schedule."""

from __future__ import annotations

from pydantic import Field

from interloper.job.base import Job


class CronJob(Job):
    """A job triggered by a cron expression.

    The trigger is declarative intent the scheduler acts on: ``cron`` sets
    the cadence, and partitioned workloads can backfill a trailing window
    of partitions on every tick.
    """

    cron: str = Field(description="Cron Expression")
    partitioned: bool = Field(default=False)
    backfill_days: int | None = Field(default=1)
    # TODO: add offset
