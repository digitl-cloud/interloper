"""Cron job: a workload on a cron schedule."""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, field_validator

from interloper.job.base import Job
from interloper.partitioning.time import TimeGranularity, TimePartitionWindow
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
    see `lookback`).
    Each firing is a backfill over that window, and ``concurrency`` is how many
    of its partitions are in flight at once, newest first.
    """

    cron: str = CronField(
        title="Cron expression",
        description="When the job runs, on the job timezone's clock",
        section="Operation",
    )
    timezone: str = TimezoneField(
        default="UTC",
        title="Timezone",
        description="IANA timezone the schedule is evaluated in",
        section="Operation",
    )
    lookback: int | None = Field(
        default=1,
        ge=1,
        description="How many partitions each run covers",
        json_schema_extra={"x-section": "Partitioning"},
    )
    offset: int = Field(
        default=1,
        ge=0,
        description="How many partitions back from the current one the window ends",
        json_schema_extra={"x-section": "Partitioning"},
    )
    concurrency: int = Field(
        default=1,
        ge=1,
        title="Concurrency",
        description="How many partitions of one firing run at once",
        json_schema_extra={"x-section": "Operation"},
    )

    @field_validator("timezone")
    @classmethod
    def _known_iana_zone(cls, value: str) -> str:
        """Reject timezone names the runtime's zoneinfo database doesn't know.

        Args:
            value: The candidate IANA timezone name (e.g. ``"Europe/Berlin"``).

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

    @staticmethod
    def zone(config: Mapping[str, Any]) -> dt.tzinfo:
        """The timezone a stored job config evaluates its schedule in.

        Stored configs are read as raw payloads, without constructing the job:
        a name that no longer resolves (after a tzdata change) degrades to UTC
        rather than failing whoever reads it.

        Args:
            config: The job's stored config payload.

        Returns:
            The configured zone, or UTC when it is unset or unknown.
        """
        try:
            return ZoneInfo(config.get("timezone") or "UTC")
        except (ZoneInfoNotFoundError, ValueError, TypeError):
            return dt.timezone.utc

    @classmethod
    def window(
        cls, config: Mapping[str, Any], *, fires_at: dt.datetime, granularity: TimeGranularity | None
    ) -> TimePartitionWindow | None:
        """The trailing window of partitions a firing of a stored job config covers.

        The window is read on the job timezone's clock at the firing instant,
        so a daily job's "yesterday" is its timezone's yesterday; hourly
        windows normalize back to UTC (see `TimePartitionWindow.lookback`).

        Args:
            config: The job's stored config payload. A missing ``lookback``
                means the default (1) and an explicit null opts out of windows.
            fires_at: The firing instant, aware.
            granularity: The granularity the job's targets share, from their
                catalog definitions; ``None`` for an unpartitioned job.

        Returns:
            The window, or ``None`` for an unpartitioned job or one whose
            lookback is null.
        """
        lookback = config.get("lookback", 1)
        if not lookback or granularity is None:
            return None
        return TimePartitionWindow.lookback(
            fires_at.astimezone(cls.zone(config)),
            lookback=lookback,
            offset=config.get("offset", 1),
            granularity=granularity,
        )
