"""The Facebook Graph API engine every Facebook Insights request goes through."""

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


class FacebookInsightsApi:
    """The Facebook Graph API: its objects, its paged edges, its insights and its refusals.

    Graph refuses a request with an error status and an ``error`` object
    carrying its own ``code``: every method raises the status as
    ``httpx2.HTTPStatusError``, and `error_code` reads Graph's code from it.

    List parameters (``fields``, ``metric``) travel as comma-separated names;
    every method joins them, so a caller passes plain lists.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, carrying a user or a Page access token.
        """
        self.client = client

    async def get(self, path: str, **params: Any) -> dict[str, Any]:
        """Read one object.

        Args:
            path: The object, relative to the versioned base URL, such as ``/{page_id}``.
            **params: The query parameters; a list value is sent comma-separated.

        Returns:
            The object as Graph sends it.
        """
        response = await self.client.get(path, params=self._query(params))
        response.raise_for_status()
        return response.json()

    async def rows(self, path: str, **params: Any) -> list[dict[str, Any]]:
        """Read every row of an edge, following ``paging.next``.

        The ``next`` link is absolute and carries the first request's
        parameters on, so pages are read one after the other, each without
        the token Graph may copy into the link. A response without one is the
        last page.

        Args:
            path: The edge, relative to the versioned base URL, such as ``/{page_id}/published_posts``.
            **params: The first request's query parameters; a list value is sent comma-separated.

        Returns:
            The ``data`` rows of every page, in page order.
        """
        pages = self.client.paginate(path, _NextLink(), params=self._query(params), data_selector="data")
        return [row async for page in pages for row in page]

    async def report(self, path: str, day: dt.date, **params: Any) -> list[dict[str, Any]]:
        """Read one day of an insights edge, as one response.

        An insights edge pages by time window, not by cursor: its
        ``paging.next`` asks the same query for the days after, so following
        it would read past *day*.

        Args:
            path: The insights edge, relative to the versioned base URL, such as ``/{page_id}/insights``.
            day: The report's only day, sent as ``since`` and, the day after, as ``until``.
            **params: What the report is: its ``metric``, ``period``, ``breakdown``; a list value is sent
                comma-separated.

        Returns:
            The insight objects, each one metric as Graph sends it (``{name, period, values, ...}``).
        """
        window = {"since": day.isoformat(), "until": (day + dt.timedelta(days=1)).isoformat()}
        return (await self.get(path, **params, **window))["data"]

    @staticmethod
    def error_code(error: httpx2.HTTPStatusError) -> int | None:
        """Read Graph's own code from a refused request.

        Args:
            error: The error status a method raised.

        Returns:
            The ``error.code`` of the response body, or None when the body is
            not JSON or carries no code.
        """
        try:
            return error.response.json().get("error", {}).get("code")
        except ValueError:
            return None

    @staticmethod
    def _query(params: dict[str, Any]) -> dict[str, Any]:
        """Join list parameters into comma-separated names, as Graph reads them.

        Args:
            params: The query parameters as the caller passed them.

        Returns:
            The parameters, each list value comma-joined and every other value unchanged.
        """
        return {key: ",".join(value) if isinstance(value, list) else value for key, value in params.items()}
