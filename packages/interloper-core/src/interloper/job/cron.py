"""Cron job: a workload on a cron schedule."""

from __future__ import annotations

from pydantic import Field

from interloper.job.base import Job
from interloper.resource.fields import CronField


class CronJob(Job):
    """A job triggered by a cron expression.

    The trigger is declarative intent the scheduler acts on: ``cron`` sets the
    cadence, and a job whose targets declare time partitioning covers a
    trailing window of partitions on every tick. Whether a job is partitioned
    is derived from its targets' catalog definitions, never stored.

    The window is counted in **partitions**, not days: ``offset`` is how many
    partitions back from the current one it ends, and ``lookback`` how many it
    spans. With daily targets, the defaults (``offset=1``, ``lookback=1``) mean
    "yesterday only"; ``offset=3`` suits a vendor whose data only settles after
    three days.
    """

    cron: str = CronField(title="Cron expression", description="When the job runs (UTC)")
    lookback: int | None = Field(
        default=1,
        ge=1,
        description="How many partitions each run covers",
    )
    offset: int = Field(
        default=1,
        ge=0,
        description="How many partitions back from the current one the window ends",
    )
