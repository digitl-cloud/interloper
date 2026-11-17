"""The AdUp API engine every AdUp request goes through."""

from __future__ import annotations

import datetime as dt
from typing import Any

import interloper as il


class AdupApi:
    """The AdUp API: its reads and its report endpoint.

    AdUp refuses a request with an HTTP error status, raised as is. A report is
    one POST to the report endpoint, whose JSON body names what the report is
    and the days it covers; it is asked for as JSON, and AdUp answers every row
    at once under ``rows``.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, exchanging the connection's client credentials
                for an access token.
        """
        self.client = client

    async def get(self, path: str, **params: Any) -> dict[str, Any]:
        """Read the one response of an endpoint.

        Args:
            path: The endpoint, relative to the API's base URL, such as ``/advertisers/me``.
            **params: The query parameters.

        Returns:
            The response's JSON body.
        """
        response = await self.client.get(path, params=params)
        response.raise_for_status()
        return response.json()

    async def report(self, start: dt.date, end: dt.date, **body: Any) -> list[dict[str, Any]]:
        """Read a report over a window of days.

        Args:
            start: The report's first day.
            end: The report's last day, included.
            **body: What the report is: its ``report_name``, ``report_type``, ``select`` and ``conditions``;
                never the keys this method sets (``download_format``, ``date_range_type``, ``date_range``).

        Returns:
            The report's ``rows``, as AdUp sends them.
        """
        window = {"min": start.isoformat(), "max": end.isoformat()}
        body = dict(**body, download_format="JSON", date_range_type="CUSTOM_DATE", date_range=window)
        response = await self.client.post("/reports/v202101/report", json=body)
        response.raise_for_status()
        return response.json()["rows"]
