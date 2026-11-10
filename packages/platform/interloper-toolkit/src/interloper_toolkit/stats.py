"""Pure helpers for the statistics tools: time windows and concurrency."""

from __future__ import annotations

import datetime
from collections.abc import Iterable


def window(
    since: str | None, until: str | None, *, default_days: int | None
) -> tuple[datetime.datetime | None, datetime.datetime | None]:
    """Resolve a tool's ``since``/``until`` arguments to aware instants.

    Args:
        since: ISO date or datetime the window opens at; ``None`` opens it
            *default_days* ago.
        until: ISO date or datetime the window closes before; ``None`` leaves
            it open.
        default_days: Days before now the window opens when *since* is
            omitted; ``None`` leaves it open-ended in the past.

    Returns:
        The ``(since, until)`` instants, UTC when the argument named no zone.
    """
    start = _parse(since)
    if start is None and default_days is not None:
        start = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=default_days)
    return start, _parse(until)


def max_concurrent(spans: Iterable[tuple[datetime.datetime, datetime.datetime | None]]) -> int:
    """The most spans open at one instant, an open-ended span counting until now.

    Args:
        spans: ``(start, end)`` pairs, ``end`` being ``None`` while still open.

    Returns:
        The peak number of overlapping spans.
    """
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    # Ends sort before starts at the same instant, so touching spans don't overlap.
    edges = sorted((t, delta) for start, end in spans for t, delta in ((start, 1), (end or now, -1)))
    peak = open_spans = 0
    for _, delta in edges:
        open_spans += delta
        peak = max(peak, open_spans)
    return peak


def _parse(value: str | None) -> datetime.datetime | None:
    """Parse an ISO date or datetime, defaulting a naive value to UTC.

    Args:
        value: The ISO string, or ``None``.

    Returns:
        The aware instant, or ``None`` for ``None``.
    """
    if value is None:
        return None
    parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=datetime.timezone.utc)
