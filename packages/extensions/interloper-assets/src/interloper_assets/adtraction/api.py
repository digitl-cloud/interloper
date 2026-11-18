"""The Adtraction API engine every Adtraction request goes through."""

from __future__ import annotations

import datetime as dt
from typing import Any
from zoneinfo import ZoneInfo

import interloper as il

STOCKHOLM = ZoneInfo("Europe/Stockholm")


class AdtractionApi:
    """The Adtraction advertiser API: its responses, its lists over instants and its reports over days.

    Adtraction answers a list or a report with a bare JSON array and no pages:
    it holds every record of its window at once. It refuses a request with an
    HTTP error status, raised as it comes. A transaction list that matches
    nothing is no empty array but an HTTP 200 message object
    (``{"message": ..., "status": 200}``), read as no records.

    Its server keeps Stockholm time. A transaction list reads its window as
    instants with their offset (``2026-10-01T00:00:00+0200``) and a report as
    days (``2026-10-01``); the methods that take a day encode it, so a caller
    passes the date.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, carrying the connection's API token.
        """
        self.client = client

    async def get(self, path: str) -> Any:
        """Read one response.

        Args:
            path: The endpoint, relative to the API's base URL, such as ``/advertiser/account/``.

        Returns:
            The response's JSON as Adtraction sends it.
        """
        response = await self.client.get(path)
        response.raise_for_status()
        return response.json()

    async def rows(self, path: str, day: dt.date, **body: Any) -> list[dict[str, Any]]:
        """Read the records a list holds for one Stockholm day, from its first second to its last.

        Args:
            path: The list, relative to the API's base URL, such as ``/advertiser/transactions/``.
            day: The day the list covers.
            **body: What else the list is, such as its ``transactionStatus``.

        Returns:
            The list's records.
        """
        first = dt.datetime.combine(day, dt.time.min, tzinfo=STOCKHOLM)
        last = dt.datetime.combine(day, dt.time(23, 59, 59), tzinfo=STOCKHOLM)
        window = {"fromDate": first.strftime("%Y-%m-%dT%H:%M:%S%z"), "toDate": last.strftime("%Y-%m-%dT%H:%M:%S%z")}
        return await self._records(path, **window, **body)

    async def report(self, path: str, day: dt.date, **body: Any) -> list[dict[str, Any]]:
        """Read one day of a report.

        Args:
            path: The report, relative to the API's base URL, such as ``/advertiser/statistics/days/``.
            day: The report's only day.
            **body: What else the report is, such as its ``transactionStatus``.

        Returns:
            The report's rows.
        """
        return await self._records(path, fromDate=day.isoformat(), toDate=day.isoformat(), **body)

    async def _records(self, path: str, **body: Any) -> list[dict[str, Any]]:
        """Post a list or report request and read its records.

        Args:
            path: The list or report, relative to the API's base URL.
            **body: The request's JSON body.

        Returns:
            The records, none when Adtraction answers with its HTTP 200 message object.

        Raises:
            RuntimeError: If Adtraction answers with a message object that is not its HTTP 200 one.
        """
        response = await self.client.post(path, json=body)
        response.raise_for_status()
        records = response.json()
        if isinstance(records, dict):
            if records.get("status") != 200:
                raise RuntimeError(f"Adtraction answered {path} with {records}")
            return []
        return records
