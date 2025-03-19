import datetime as dt
from collections.abc import Generator


def date_range(start_date: dt.date, end_date: dt.date, reversed: bool = False) -> Generator[dt.date]:
    if reversed:
        while end_date >= start_date:
            yield end_date
            end_date -= dt.timedelta(days=1)
    else:
        while start_date <= end_date:
            yield start_date
            start_date += dt.timedelta(days=1)
