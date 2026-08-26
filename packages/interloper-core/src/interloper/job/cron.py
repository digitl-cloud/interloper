"""Cron job: a workload on a cron schedule."""

from __future__ import annotations

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, field_validator

from interloper.job.base import Job
from interloper.resource.fields import CronField, TimezoneField


class CronJob(Job):
    """A job triggered by a cron expression.

    The trigger is declarative intent the scheduler acts on: ``cron`` sets the
    cadence on the wall clock of ``timezone``, and a job whose targets declare
    time partitioning covers a trailing window of partitions on every tick.
    Whether a job is partitioned is derived from its targets' catalog
    definitions, never stored.

    The window is counted in **partitions**, not days: ``offset`` is how many
    partitions back from the current one it ends, and ``lookback`` how many it
    spans. With daily targets, the defaults (``offset=1``, ``lookback=1``) mean
    "yesterday only" — the job timezone's yesterday. Hourly windows are always
    UTC-derived regardless of ``timezone`` (hour partition ids are UTC labels,
    see :class:`~interloper.partitioning.time.TimePartitionWindow.lookback`).
    """

    cron: str = CronField(title="Cron expression", description="When the job runs, on the job timezone's clock")
    timezone: str = TimezoneField(
        default="UTC",
        title="Timezone",
        description="IANA timezone the schedule is evaluated in",
    )
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

    @field_validator("timezone")
    @classmethod
    def _known_iana_zone(cls, value: str) -> str:
        """Reject timezone names the runtime's zoneinfo database doesn't know.

        Returns:
            The validated timezone name.

        Raises:
            ValueError: If the name is not a known IANA timezone.
        """
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError):
            raise ValueError(f"Unknown IANA timezone: {value!r}") from None
        return value
