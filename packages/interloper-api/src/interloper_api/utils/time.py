"""Time zones and durations as the API presents them."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def job_zone(name: str | None) -> dt.tzinfo:
    """Resolve a job's timezone name, falling back to UTC like the scheduler does.

    Args:
        name: The IANA name from the job's config; ``None`` or an empty name
            means UTC.

    Returns:
        The zone, or UTC when the name is unknown or malformed.
    """
    try:
        return ZoneInfo(name or "UTC")
    except (ZoneInfoNotFoundError, ValueError, TypeError):
        return dt.timezone.utc


def format_duration(delta: dt.timedelta) -> str:
    """Render a duration compactly, keeping its two largest units: ``2d 3h``, ``5h 12m`` or ``12m``.

    Args:
        delta: The duration; seconds below a whole minute are dropped.

    Returns:
        The compact label.
    """
    minutes = int(delta.total_seconds() // 60)
    days, minutes = divmod(minutes, 24 * 60)
    hours, minutes = divmod(minutes, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"
