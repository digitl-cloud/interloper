"""The Adservice API engine every Adservice request goes through."""

from __future__ import annotations

import datetime as dt
from typing import Any

import interloper as il


class AdserviceApi:
    """The Adservice API: its reports, each read for one day.

    A report is read as one response, whose envelope holds the report under
    ``data``. An HTTP error status is raised as it comes.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, carrying the connection's API key.
        """
        self.client = client

    async def report(self, path: str, day: dt.date, **params: Any) -> Any:
        """Read one day of a report.

        Args:
            path: The report, relative to the API's base URL, such as ``/statistics``.
            day: The report's only day, sent as its ``from_date`` and ``to_date``.
            **params: What else the report is, such as its ``group_by``; a parameter left out is not sent.

        Returns:
            The envelope's ``data`` as Adservice sends it: the report's rows, or an object holding them.
        """
        window = {"from_date": day.isoformat(), "to_date": day.isoformat()}
        response = await self.client.get(path, params=dict(**window, **params))
        response.raise_for_status()
        return response.json()["data"]
