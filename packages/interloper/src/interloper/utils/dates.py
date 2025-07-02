"""This module contains date utility functions."""
import datetime as dt
from collections.abc import Generator


def date_range(start_date: dt.date, end_date: dt.date, reversed: bool = False) -> Generator[dt.date]:
    """Generate a range of dates.

    Args:
        start_date: The start date of the range.
        end_date: The end date of the range.
        reversed: Whether to reverse the range.

    Yields:
        The dates in the range.
    """
    if reversed:
        while end_date >= start_date:
            yield end_date
            end_date -= dt.timedelta(days=1)
    else:
        while start_date <= end_date:
            yield start_date
            start_date += dt.timedelta(days=1)
