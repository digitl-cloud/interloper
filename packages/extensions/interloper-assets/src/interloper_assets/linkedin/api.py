"""LinkedIn's versioned REST API: the engine LinkedIn Ads and LinkedIn Organic read through."""

from __future__ import annotations

import datetime as dt
from typing import Any, ClassVar
from urllib.parse import quote

import httpx2
import interloper as il


class LinkedinApiError(httpx2.HTTPStatusError):
    """LinkedIn refused a request, with its own code and message.

    It is the HTTP error status LinkedIn answered, so whatever reads an
    ``httpx2.HTTPStatusError`` reads it unchanged. Its message is LinkedIn's
    reason rather than httpx's, which names the request URL.
    """

    def __init__(
        self, message: str, *, code: str | None, service_error_code: int | None, response: httpx2.Response
    ) -> None:
        """Hold LinkedIn's codes beside the refused response.

        Args:
            message: What LinkedIn refused and why.
            code: LinkedIn's ``code`` (``REVOKED_ACCESS_TOKEN``), or None when the body carries none.
            service_error_code: LinkedIn's ``serviceErrorCode`` (``65601``), or None when the body carries none.
            response: The refused response.
        """
        super().__init__(message, request=response.request, response=response)
        self.code = code
        self.service_error_code = service_error_code

    @classmethod
    def from_response(cls, response: httpx2.Response) -> LinkedinApiError:
        """Read LinkedIn's refusal from an error response.

        Args:
            response: A response with an error status.

        Returns:
            The refusal, with LinkedIn's ``code``, ``serviceErrorCode`` and
            ``message``, or the status's reason when the body is not LinkedIn's
            error object.
        """
        try:
            body = response.json()
        except ValueError:
            body = None
        error = body if isinstance(body, dict) else {}
        code = error.get("code")
        service_error_code = error.get("serviceErrorCode")
        status = [f"HTTP {response.status_code}"]
        if code is not None:
            status.append(code)
        if service_error_code is not None:
            status.append(f"service error {service_error_code}")
        reason = error.get("message") or response.reason_phrase
        return cls(
            f"LinkedIn refused the request ({', '.join(status)}): {reason}",
            code=code,
            service_error_code=service_error_code,
            response=response,
        )


class LinkedinApi:
    """The LinkedIn REST API as Rest.li 2.0 speaks it: its query, its pagings and its daily reports.

    Rest.li 2.0 reads a query's structure from raw ``(``, ``:`` and ``,`` and
    rejects them percent-encoded, which is what an ordinary query encoder
    makes of them. So every method writes its query itself, through `_url`,
    from the plain values a caller passes, and sends it as part of the path.

    A list response wraps its rows in ``elements``. A collection links its
    next page in ``paging.links`` (`rows`), and a search finder names it by
    cursor (`cursor_rows`). Every request, each page included, goes
    through `_read`, which raises LinkedIn's refusal as `LinkedinApiError`
    with LinkedIn's code and message.
    """

    CURSOR_PAGE_SIZE: ClassVar[int] = 1000

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, carrying the
                connection's token and LinkedIn's version and protocol headers.
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
        return await self._read(self._url(path, params))

    async def rows(self, path: str, **params: Any) -> list[dict[str, Any]]:
        """Read every element of a collection, following its ``next`` links.

        LinkedIn may link a next page that turns out empty, so the walk ends at
        a response without a ``next`` link or without elements: a finder that
        does not page is read once. A link is host-relative (``/rest/...``) or
        absolute, and is followed with its query as LinkedIn wrote it.

        Args:
            path: The collection, relative to the API's base URL, such as ``/functions``.
            **params: The query parameters as plain values, written in Rest.li syntax.

        Returns:
            The ``elements`` of every page, in page order.
        """
        rows: list[dict[str, Any]] = []
        url: str | httpx2.URL | None = self._url(path, params)
        while url is not None:
            body = await self._read(url)
            elements = self._elements(body)
            rows.extend(elements)
            url = self._next(body) if elements else None
        return rows

    async def cursor_rows(self, path: str, **params: Any) -> list[dict[str, Any]]:
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
        query = {**params, "pageSize": self.CURSOR_PAGE_SIZE}
        rows: list[dict[str, Any]] = []
        while True:
            body = await self.get(path, **query)
            rows.extend(self._elements(body))
            token = (body.get("metadata") or {}).get("nextPageToken")
            if not token:
                return rows
            query["pageToken"] = token

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

    async def statistics(self, path: str, day: dt.date, **params: Any) -> list[dict[str, Any]]:
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

    async def _read(self, url: str | httpx2.URL) -> dict[str, Any]:
        """Send one GET and return its body.

        A response with an error status raises as `LinkedinApiError`.

        Args:
            url: The request's path with its Rest.li query, or a page's absolute ``next`` link.

        Returns:
            The response's body.
        """
        response = await self.client.get(url)
        if response.is_error:
            error = LinkedinApiError.from_response(response)
            raise error
        return response.json()

    @classmethod
    def _url(cls, path: str, params: dict[str, Any]) -> str:
        """Write a request's path with its query in Rest.li 2.0 syntax.

        Args:
            path: The endpoint, relative to the API's base URL.
            params: The query parameters as plain values, in order; ``fields``,
                the projection, is a list of field names.

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
            value: A list, a record as a dict, a day, a boolean, or a scalar,
                nested to any depth.

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
