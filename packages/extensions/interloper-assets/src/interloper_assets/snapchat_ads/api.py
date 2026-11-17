"""The Snapchat Marketing API engine every Snapchat Ads request goes through."""

from __future__ import annotations

import datetime as dt
from collections.abc import AsyncIterator
from typing import Any

import interloper as il


class SnapchatAdsApi:
    """The Snapchat Marketing API: its paged lists and its stats reports.

    A report here is a ``TOTAL`` over one day broken down to a level: its
    envelope differs from a ``DAY`` or ``HOUR`` report's (``timeseries_stats``)
    and from an unbroken total's, so those would be mechanisms of their own.

    A list wraps each record twice: the page holds the records under their
    plural name, each in a wrapper holding it under the singular
    (``{"campaigns": [{"sub_request_status": ..., "campaign": {...}}]}``). Every
    response links its next page at ``paging.next_link``, and one without it is
    the last. Snapchat refuses a request with an HTTP error status, raised as
    it comes.

    List parameters (``fields``, ``report_dimension``) travel comma-separated;
    every method encodes them, so a caller passes plain values.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, refreshing the connection's access token.
        """
        self.client = client

    async def rows(self, path: str, list_key: str, item_key: str, **params: Any) -> list[dict[str, Any]]:
        """Read every record of a paged list.

        Args:
            path: The endpoint, relative to the API's base URL, such as ``/adaccounts/{id}/campaigns``.
            list_key: The key a page holds its wrapped records under, such as ``campaigns``.
            item_key: The key each wrapper holds its record under, such as ``campaign``.
            **params: The query parameters; a list value is sent comma-separated.

        Returns:
            The records of every page, unwrapped, in page order.
        """
        pages = self._pages(path, **params)
        return [item[item_key] async for page in pages for item in page.get(list_key, [])]

    async def report(self, path: str, day: dt.date, *, breakdown: str, **params: Any) -> list[dict[str, Any]]:
        """Read one day of a stats report, totalled over the day.

        A ``TOTAL`` report answers each total under ``total_stats``, with its
        rows under ``breakdown_stats`` keyed by the breakdown asked for.

        Args:
            path: The stats endpoint, relative to the API's base URL, such as ``/adaccounts/{id}/stats``.
            day: The report's only day.
            breakdown: The level the rows break the total down to, such as ``ad`` or ``campaign``.
            **params: What else the report is: its ``fields`` and any ``report_dimension``; a list
                value is sent comma-separated.

        Returns:
            The breakdown rows of every total on every page, each nesting its metrics.
        """
        window = {
            "granularity": "TOTAL",
            "start_time": day.isoformat(),
            "end_time": (day + dt.timedelta(days=1)).isoformat(),
        }
        pages = self._pages(path, **window, breakdown=breakdown, **params)
        return [
            row
            async for page in pages
            for total in page.get("total_stats", [])
            for row in total["total_stat"]["breakdown_stats"][breakdown]
        ]

    def _pages(self, path: str, **params: Any) -> AsyncIterator[dict[str, Any]]:
        """Walk a paged endpoint by its next links, one page body at a time.

        Each page links the next, so the pages are read one after another;
        their count is not known up front.

        Args:
            path: The endpoint, relative to the API's base URL.
            **params: The first page's query parameters; the next links carry their own.

        Returns:
            The page bodies, in page order.
        """
        paginator = il.JSONLinkPaginator(next_url_path="paging.next_link")
        return self.client.paginate(path, paginator, params=self._query(params))

    @staticmethod
    def _query(params: dict[str, Any]) -> dict[str, Any]:
        """Encode list parameters comma-separated, as Snapchat reads them.

        Args:
            params: The query parameters as the caller passed them.

        Returns:
            The parameters, each list value joined with commas and every other value unchanged.
        """
        return {key: ",".join(value) if isinstance(value, list) else value for key, value in params.items()}
