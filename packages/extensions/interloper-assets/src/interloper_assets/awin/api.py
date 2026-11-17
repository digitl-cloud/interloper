"""The Awin API engine every Awin request goes through."""

from __future__ import annotations

import datetime as dt
from typing import Any

import interloper as il


class AwinApi:
    """The Awin API: its responses, its lists over instants and its reports over days.

    Awin answers a list or a report with a bare JSON array and no pages: it
    holds every record of its window at once. It refuses a request with an
    HTTP error status, raised as it comes.

    A transaction list reads its window as instants (``2026-10-01T00:00:00``)
    and a report as days (``2026-10-01``); the methods that take a day encode
    it, so a caller passes the date.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, carrying the connection's access token as its bearer.
        """
        self.client = client

    async def get(self, path: str, **params: Any) -> Any:
        """Read one response.

        Args:
            path: The endpoint, relative to the API's base URL, such as ``/accounts``.
            **params: The query parameters.

        Returns:
            The response's JSON as Awin sends it.
        """
        response = await self.client.get(path, params=params)
        response.raise_for_status()
        return response.json()

    async def rows(self, path: str, day: dt.date, **params: Any) -> list[dict[str, Any]]:
        """Read the records a list holds for one day, from its first second to its last.

        Args:
            path: The list, relative to the API's base URL, such as ``/advertisers/<id>/transactions/``.
            day: The day the list covers.
            **params: What else the list is, such as its ``timezone`` and ``showBasketProducts``.

        Returns:
            The list's records.
        """
        window = {"startDate": f"{day.isoformat()}T00:00:00", "endDate": f"{day.isoformat()}T23:59:59"}
        return await self.get(path, **window, **params)

    async def report(self, path: str, day: dt.date, **params: Any) -> list[dict[str, Any]]:
        """Read one day of a report.

        Args:
            path: The report, relative to the API's base URL, such as ``/advertisers/<id>/reports/publisher``.
            day: The report's only day.
            **params: What else the report is, such as its ``timezone``.

        Returns:
            The report's rows.
        """
        window = {"startDate": day.isoformat(), "endDate": day.isoformat()}
        return await self.get(path, **window, **params)
