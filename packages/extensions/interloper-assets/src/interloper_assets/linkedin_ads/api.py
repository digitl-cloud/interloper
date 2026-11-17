"""The LinkedIn Marketing API engine every LinkedIn Ads request goes through."""

from __future__ import annotations

import datetime as dt
from typing import Any
from urllib.parse import quote

import interloper as il

from interloper_assets.linkedin_ads.constants import CURSOR_PAGE_SIZE, OFFSET_PAGE_SIZE


class LinkedinAdsApi:
    """The LinkedIn Marketing API: its Rest.li query, its pages and its ad analytics report.

    Rest.li 2.0 reads structured parameters in its own syntax (``List(a,b)``,
    ``(key:value)``) and rejects them percent-encoded, which is what an
    ordinary query encoder makes of every comma and parenthesis. So every
    method writes its query itself, through `_url`, from the plain values a
    caller passes, and sends it as part of the path.

    LinkedIn refuses a request with an HTTP error status, raised as it comes,
    and wraps every list it returns in ``elements``.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, carrying the
                connection's token and the API version headers.
        """
        self.client = client

    async def get(self, path: str, **params: Any) -> dict[str, Any]:
        """Read the one response of a request.

        Args:
            path: The endpoint, relative to the API's base URL, such as ``/adAccounts/123``.
            **params: The query parameters as plain values, written in Rest.li syntax.

        Returns:
            The response's body.
        """
        response = await self.client.get(self._url(path, params))
        response.raise_for_status()
        return response.json()

    async def rows(self, path: str, **params: Any) -> list[dict[str, Any]]:
        """Read every row of a finder paged by cursor.

        Each page names the next one in ``metadata.nextPageToken``, so a page
        without one is the last, and an endpoint that does not page is read
        once.

        Args:
            path: The endpoint, relative to the API's base URL, such as ``/adAccounts/123/adCampaigns``.
            **params: The query parameters as plain values, ``q`` naming the finder.

        Returns:
            The ``elements`` of every page, in page order.
        """
        query = {**params, "pageSize": CURSOR_PAGE_SIZE}
        rows: list[dict[str, Any]] = []
        while True:
            body = await self.get(path, **query)
            rows.extend(self._elements(body))
            token = (body.get("metadata") or {}).get("nextPageToken")
            if not token:
                return rows
            query["pageToken"] = token

    async def offset_rows(self, path: str, **params: Any) -> list[dict[str, Any]]:
        """Read every row of a finder paged by ``start`` and ``count``.

        The walk ends on the first empty page, so a page must carry its
        ``elements``: one without fails rather than cutting the list short. It
        also ends on a page identical to the one before, which is what an
        endpoint that ignores ``start`` answers forever.

        Args:
            path: The endpoint, relative to the API's base URL, such as ``/adAccounts``.
            **params: The query parameters as plain values, ``q`` naming the finder.

        Returns:
            The ``elements`` of every page, in page order.
        """
        rows: list[dict[str, Any]] = []
        previous: list[dict[str, Any]] = []
        start = 0
        while True:
            page = (await self.get(path, **params, count=OFFSET_PAGE_SIZE, start=start))["elements"]
            if not page or page == previous:
                return rows
            rows.extend(page)
            previous = page
            start += OFFSET_PAGE_SIZE

    async def report(self, day: dt.date, **params: Any) -> list[dict[str, Any]]:
        """Read one day of the ad analytics report.

        Args:
            day: The report's only day.
            **params: What the report is: its finder ``q``, pivots, ``timeGranularity``,
                ``accounts`` and ``fields``.

        Returns:
            The report's rows, as LinkedIn sends them.
        """
        body = await self.get("/adAnalytics", **params, dateRange={"start": day, "end": day})
        return self._elements(body)

    @classmethod
    def _url(cls, path: str, params: dict[str, Any]) -> str:
        """Write a request's path with its query in Rest.li 2.0 syntax.

        Args:
            path: The endpoint, relative to the API's base URL.
            params: The query parameters as plain values; ``fields``, the
                projection, is a list of field names.

        Returns:
            The path, followed by its query when there are parameters.
        """
        if not params:
            return path
        query = "&".join(
            f"{key}={','.join(value) if key == 'fields' else cls._encode(value)}" for key, value in params.items()
        )
        return f"{path}?{query}"

    @classmethod
    def _encode(cls, value: Any) -> str:
        """Write one parameter value in Rest.li 2.0 syntax.

        Args:
            value: A list, a record as a dict, a day, a boolean, or a scalar.

        Returns:
            ``List(...)`` for a list, ``(key:value,...)`` for a record or a
            day, ``true``/``false`` for a boolean, and any other value as text
            with every reserved character percent-encoded.
        """
        match value:
            case bool():
                return "true" if value else "false"
            case dt.date():
                return cls._encode({"year": value.year, "month": value.month, "day": value.day})
            case list() | tuple():
                return f"List({','.join(cls._encode(item) for item in value)})"
            case dict():
                return f"({','.join(f'{key}:{cls._encode(item)}' for key, item in value.items())})"
            case _:
                return quote(str(value), safe="")

    @staticmethod
    def _elements(body: dict[str, Any]) -> list[dict[str, Any]]:
        """Unwrap the rows of a list response.

        Args:
            body: A response's body.

        Returns:
            Its ``elements``, or no rows when it carries none.
        """
        return body.get("elements", [])
