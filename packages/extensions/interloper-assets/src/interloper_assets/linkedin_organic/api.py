"""The LinkedIn REST API engine every LinkedIn Organic request goes through."""

from __future__ import annotations

import datetime as dt
from typing import Any
from urllib.parse import quote

import httpx2
import interloper as il


class LinkedinOrganicApi:
    """The LinkedIn REST API as Rest.li 2.0 speaks it: its query syntax, its collections and its daily statistics.

    Rest.li 2.0 reads a query's structure from raw ``(``, ``:`` and ``,`` and
    rejects them percent-encoded, so the engine writes the query string itself
    rather than leaving it to the HTTP client: every value is percent-encoded
    and an object or a list is written around its values in Rest.li syntax
    (``(start:1,end:2)``, ``List(a,b)``). A caller passes plain values.

    A collection answers its ``elements`` and links its next page in
    ``paging.links``. LinkedIn refuses a request with an HTTP error status,
    raised as it is.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, carrying the
                connection's token and LinkedIn's version and protocol headers.
        """
        self.client = client

    async def rows(self, path: str, **params: Any) -> list[dict[str, Any]]:
        """Read every element of a collection, following its ``next`` links.

        LinkedIn may link a next page that turns out empty, so the walk ends at
        a response without a ``next`` link or without elements: a finder that
        does not page is read once. A link is host-relative (``/rest/...``) or
        absolute, and is followed with its query as LinkedIn wrote it.

        Args:
            path: The collection, relative to the API's base URL, such as ``/functions``.
            **params: The query parameters, written in Rest.li syntax by `_url`.

        Returns:
            The ``elements`` of every page, in page order.
        """
        rows: list[dict[str, Any]] = []
        url: str | httpx2.URL | None = self._url(path, params)
        while url is not None:
            response = await self.client.get(url)
            response.raise_for_status()
            body = response.json()
            elements = body.get("elements", [])
            rows.extend(elements)
            url = self._next(body) if elements else None
        return rows

    async def report(self, path: str, day: dt.date, **params: Any) -> list[dict[str, Any]]:
        """Read one UTC day of any time-bound statistics finder.

        Args:
            path: The statistics finder, such as ``/organizationPageStatistics``.
            day: The report's only day, from midnight to midnight UTC.
            **params: What the report is: its finder ``q`` and the finder's own
                parameters, such as the organization it is about.

        Returns:
            The finder's elements for the day, each carrying its epoch-millisecond ``timeRange``.
        """
        start = dt.datetime.combine(day, dt.time(), tzinfo=dt.UTC)
        end = start + dt.timedelta(days=1)
        window = {
            "timeGranularityType": "DAY",
            "timeRange": {"start": int(start.timestamp() * 1000), "end": int(end.timestamp() * 1000)},
        }
        return await self.rows(path, **params, timeIntervals=window)

    @classmethod
    def _url(cls, path: str, params: dict[str, Any]) -> str:
        """Write a path and its query string in Rest.li 2.0 syntax.

        Args:
            path: The endpoint, relative to the API's base URL.
            params: The query parameters as the caller passed them, in order.

        Returns:
            The path, followed by its query when there are parameters.
        """
        if not params:
            return path
        query = "&".join(f"{key}={cls._encode(value)}" for key, value in params.items())
        return f"{path}?{query}"

    @classmethod
    def _encode(cls, value: Any) -> str:
        """Write one parameter value in Rest.li 2.0 syntax.

        Args:
            value: A dict (a Rest.li object keyed by field names), a list (a
                Rest.li array), a string or a number, nested to any depth.

        Returns:
            The value with every string and number percent-encoded, ``:`` and
            ``,`` included, and every structure's own punctuation left raw.
        """
        if isinstance(value, dict):
            return "(" + ",".join(f"{key}:{cls._encode(member)}" for key, member in value.items()) + ")"
        if isinstance(value, list):
            return "List(" + ",".join(cls._encode(member) for member in value) + ")"
        return quote(str(value), safe="")

    def _next(self, body: dict[str, Any]) -> httpx2.URL | None:
        """Resolve a page's ``next`` link against the API's host.

        Args:
            body: A collection page's decoded response.

        Returns:
            The next page's absolute URL, or ``None`` when the page links none.
        """
        links = (body.get("paging") or {}).get("links") or []
        href = next((link.get("href") for link in links if link.get("rel") == "next"), None)
        return self.client.base_url.join(href) if href else None
