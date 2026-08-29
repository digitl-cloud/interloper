"""Date and datetime helpers shared across the framework."""

from __future__ import annotations

import calendar
import datetime as dt


def coerce_to_date(value: object) -> dt.date:
    """Coerce a value to a ``datetime.date``.

    Accepts a ``date``, a ``datetime`` (its date part is used), or an ISO-8601
    date string. Anything else raises ``TypeError``.

    Args:
        value: The value to coerce.

    Returns:
        The value as a ``datetime.date``.

    Raises:
        TypeError: If the value cannot be interpreted as a date.
    """
    # `datetime` is a subclass of `date`, so check it first.
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    if isinstance(value, str):
        try:
            return dt.date.fromisoformat(value)
        except ValueError as e:
            raise TypeError(
                f"Could not parse value {value!r} as a date: expected an ISO-8601 date string (YYYY-MM-DD)."
            ) from e
    raise TypeError(
        f"Expected a `datetime.date` or an ISO-8601 date string, got {type(value).__name__}: {value!r}."
    )


def coerce_to_datetime(value: object) -> dt.datetime:
    """Coerce a value to a naive-UTC ``datetime.datetime``.

    Accepts a ``datetime``, a ``date`` (midnight is assumed), or an ISO-8601
    datetime string. Anything else raises ``TypeError``.

    The result is a UTC *label*, not an instant: an aware datetime is
    converted to UTC and stripped of its tzinfo, so comparisons never mix
    aware and naive values.

    Args:
        value: The value to coerce.

    Returns:
        The value as a ``datetime.datetime``.

    Raises:
        TypeError: If the value cannot be interpreted as a datetime.
    """
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None:
            return value.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, dt.date):
        return dt.datetime(value.year, value.month, value.day)  # noqa: DTZ001 — a label, not an instant
    if isinstance(value, str):
        try:
            return coerce_to_datetime(dt.datetime.fromisoformat(value))
        except ValueError as e:
            raise TypeError(
                f"Could not parse value {value!r} as a datetime: expected an ISO-8601 datetime string."
            ) from e
    raise TypeError(
        f"Expected a `datetime.datetime` or an ISO-8601 datetime string, got {type(value).__name__}: {value!r}."
    )


def add_months(value: dt.date, months: int) -> dt.date:
    """Shift a date by *months*, clamping the day to the target month's length.

    Args:
        value: The date to shift.
        months: Number of months to add; negative shifts backwards.

    Returns:
        The shifted date.
    """
    total = (value.year * 12 + value.month - 1) + months
    year, month = divmod(total, 12)
    month += 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def assume_utc(value: dt.datetime) -> dt.datetime:
    """Treat a naive timestamp as UTC, leaving an aware one alone.

    Args:
        value: The timestamp to normalise, aware or naive.

    Returns:
        An aware datetime; an already-aware *ts* is returned unchanged.
    """
    return value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)


def month_start(value: dt.datetime) -> dt.date:
    """The first day of the UTC calendar month a timestamp falls in.

    Naive values are treated as UTC, so a timestamp read back from a database
    that drops tzinfo attributes to the same month it was written in.

    Args:
        value: The timestamp to attribute, aware or naive.

    Returns:
        The first day of that UTC month, as a date.
    """
    return assume_utc(value).astimezone(dt.timezone.utc).date().replace(day=1)
