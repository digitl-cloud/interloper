"""Cron job: a workload on a cron schedule."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from interloper.job.base import Job


class CronJob(Job):
    """A job triggered by a cron expression.

    The trigger is declarative intent the scheduler acts on: ``cron`` sets the
    cadence, and a partitioned workload covers a trailing window of partitions
    on every tick.

    The window is counted in **partitions**, not days: ``offset`` is how many
    partitions back from the current one it ends, and ``lookback`` how many it
    spans. With daily targets, the defaults (``offset=1``, ``lookback=1``) mean
    "yesterday only"; ``offset=3`` suits a vendor whose data only settles after
    three days.
    """

    cron: str = Field(description="Cron Expression")
    partitioned: bool = Field(default=False)
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

    def __init__(self, /, **data: Any) -> None:
        """Construct the job, accepting the pre-0.60 ``backfill_days`` key.

        Persisted configs written before ``lookback``/``offset`` existed carry
        ``backfill_days``, which :class:`~interloper.component.base.Component`
        would reject as an unknown kwarg. Reading both keys for one release is
        what lets the images roll and the migration run in either order; the
        shim goes away once every deployment has migrated.
        """
        if "backfill_days" in data:
            legacy = data.pop("backfill_days")
            data.setdefault("lookback", legacy)
            data.setdefault("offset", 1)
        super().__init__(**data)
