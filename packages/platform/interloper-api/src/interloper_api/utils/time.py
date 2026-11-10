"""Time zones and durations as the API presents them."""

from __future__ import annotations

import datetime as dt


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
