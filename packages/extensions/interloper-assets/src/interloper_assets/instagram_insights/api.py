"""The Instagram Graph API engine every Instagram Insights request goes through."""

from __future__ import annotations

import datetime as dt
from typing import Any

import httpx2
import interloper as il


class _NextLink(il.JSONLinkPaginator):
    """Graph's ``paging.next`` link, followed without the access token Graph may copy into it.

    The client sends its token as the bearer. Left in the URL, the token would
    reach the HTTP client's request log and, on a refused page, the error's
    message.
    """

    def __init__(self) -> None:
        """Follow the link at ``paging.next``."""
        super().__init__(next_url_path="paging.next")

    def update_request(self, request: httpx2.Request) -> None:
        """Point the request at the next link, its ``access_token`` dropped.

        Args:
            request: The request to advance, mutated in place.
        """
        super().update_request(request)
        request.url = request.url.copy_remove_param("access_token")


class InstagramInsightsApi:
    """The Instagram Graph API: its nodes, its paged edges and their insights.

    Graph refuses a request with an HTTP error status, so every response,
    each page included, is checked as it arrives and raises
    `httpx2.HTTPStatusError`. An edge wraps its records in ``data`` and pages
    by the absolute ``paging.next`` link, which carries the cursor and every
    parameter of the first request.

    List parameters (``fields``, ``metric``, ``breakdown``) travel
    comma-separated; every method encodes them, so a caller passes plain
    values.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, pinned to a Graph version and carrying the
                connection's access token as its bearer.
        """
        self.client = client

    async def get(self, path: str, **params: Any) -> dict[str, Any]:
        """Read one node, such as an account and the fields it is asked for.

        Args:
            path: The node, relative to the versioned base URL, such as ``/<account_id>``.
            **params: The query parameters; a list value is sent comma-separated.

        Returns:
            The node as Graph sends it.
        """
        response = await self.client.get(path, params=self._query(params))
        response.raise_for_status()
        return response.json()

    async def rows(self, path: str, **params: Any) -> list[dict[str, Any]]:
        """Read every record of an edge, following its ``paging.next`` links.

        Each link is followed without the token Graph may copy into it. A
        response without a next link is the last page, so an edge that does
        not page is read once.

        Args:
            path: The edge, relative to the versioned base URL, such as ``/<account_id>/media``.
            **params: The query parameters of the first request; a list value is sent comma-separated.

        Returns:
            The ``data`` records of every page, in page order.
        """
        pages = self.client.paginate(path, _NextLink(), params=self._query(params), data_selector="data")
        return [row async for page in pages for row in page]

    async def report(self, path: str, day: dt.date, **params: Any) -> list[dict[str, Any]]:
        """Read one day of an insights edge.

        Args:
            path: The insights edge, relative to the versioned base URL, such as ``/<account_id>/insights``.
            day: The report's only day, sent as ``since`` and, the day after, as ``until``.
            **params: What the report is: its ``metric``, ``period``, ``metric_type``, ``breakdown``.

        Returns:
            The insight objects of every page, each one metric as Graph sends it
            (``{name, period, values | total_value, ...}``).
        """
        window = {"since": day.isoformat(), "until": (day + dt.timedelta(days=1)).isoformat()}
        return await self.rows(path, **params, **window)

    @staticmethod
    def _query(params: dict[str, Any]) -> dict[str, Any]:
        """Encode list parameters comma-separated, as Graph reads them.

        Args:
            params: The query parameters as the caller passed them.

        Returns:
            The parameters, each list value joined with commas and every other value unchanged.
        """
        return {key: ",".join(value) if isinstance(value, list) else value for key, value in params.items()}
