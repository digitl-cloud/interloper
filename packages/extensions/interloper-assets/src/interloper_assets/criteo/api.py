"""The Criteo API engine every Criteo request goes through."""

from __future__ import annotations

import datetime as dt
import logging
from typing import Any

import httpx2
import interloper as il

from interloper_assets.criteo.constants import API_VERSION, PAGE_SIZE

logger = logging.getLogger(__name__)


class CriteoApiError(RuntimeError):
    """Criteo answered a request with a success status and errors beside its data."""

    def __init__(self, errors: list[dict[str, Any]]) -> None:
        """Name every error Criteo reported.

        Args:
            errors: The response's ``errors``, each a Criteo problem with its ``code``, ``title`` and ``detail``.
        """
        super().__init__(f"Criteo answered with errors: {'; '.join(_describe(error) for error in errors)}")
        self.errors = errors


def _describe(problem: dict[str, Any]) -> str:
    """Spell a Criteo problem as its code and detail, falling back on its title.

    Args:
        problem: One entry of a response's ``errors`` or ``warnings``.

    Returns:
        ``code: detail`` when both are present, else whichever of them, or the title, is.
    """
    code = problem.get("code")
    detail = problem.get("detail") or problem.get("title") or ""
    return f"{code}: {detail}" if code and detail else str(code or detail)


class CriteoApi:
    """The Criteo API: its versioned paths, its JSON:API envelope, its pages and its statistics report.

    Every path is under the API version, so a caller names the path after it
    (``/advertisers/me``). Entities come as JSON:API resources under ``data``,
    each ``{id, type, attributes}``; `get`, `post` and `rows` return them as
    rows, the attributes beside the resource's own fields, and a success
    without ``data`` raises rather than reading as nothing to list. Criteo
    refuses a request with an HTTP error status, raised as is. A success can
    also carry ``errors`` for the entities it could not answer: that raises as
    `CriteoApiError`, so no asset writes a partial answer, and its
    ``warnings`` (deprecations) are logged.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, refreshing the connection's access token.
        """
        self.client = client

    async def get(self, path: str, **params: Any) -> list[dict[str, Any]]:
        """Read the resources of one response to a GET.

        Args:
            path: The endpoint, after the API version, such as ``/advertisers/me``.
            **params: The query parameters.

        Returns:
            One row per resource of the response.
        """
        response = await self.client.get(self._versioned(path), params=params)
        response.raise_for_status()
        return self._rows(response)

    async def post(self, path: str, **body: Any) -> list[dict[str, Any]]:
        """Read the resources of one response to a POST, such as a search answering every match at once.

        Args:
            path: The endpoint, after the API version, such as ``/marketing-solutions/campaigns/search``.
            **body: The JSON body's fields, such as ``filters``.

        Returns:
            One row per resource of the response.
        """
        response = await self.client.post(self._versioned(path), json=body)
        response.raise_for_status()
        return self._rows(response)

    async def rows(self, path: str, **params: Any) -> list[dict[str, Any]]:
        """Read every resource of a paged endpoint, ``PAGE_SIZE`` at a time.

        Criteo reports no total, so pages are read in turn until an empty one.
        A page that repeats the one before ends the walk too: an endpoint that
        does not page ignores ``offset`` and would answer the same list forever.

        Args:
            path: The endpoint, after the API version, such as ``/marketing-solutions/advertisers/1/ads``.
            **params: The query parameters, sent beside ``limit`` and ``offset``.

        Returns:
            One row per resource of every page, in page order.
        """
        paginator = il.OffsetPaginator(limit=PAGE_SIZE, data_selector=self._rows)
        pages = self.client.paginate(self._versioned(path), paginator, params=params, data_selector=self._rows)
        rows: list[dict[str, Any]] = []
        previous_page = None
        async for page in pages:
            if page == previous_page:
                break
            rows.extend(page)
            previous_page = page
        return rows

    async def report(self, day: dt.date, **query: Any) -> list[dict[str, Any]]:
        """Read one day of the statistics report.

        Criteo converts every amount to the currency the query names and
        buckets days in the time zone it names; both are the caller's to choose.

        Args:
            day: The report's only day.
            **query: What the report is: its ``advertiserIds``, ``dimensions``,
                ``metrics``, ``currency`` and ``timezone``.

        Returns:
            The report's ``Rows``.
        """
        body = dict(startDate=day.isoformat(), endDate=day.isoformat(), **query, format="json")
        response = await self.client.post(self._versioned("/statistics/report"), json=body)
        response.raise_for_status()
        return response.json()["Rows"]

    @staticmethod
    def _versioned(path: str) -> str:
        """Put a path under the API version every endpoint lives in.

        Args:
            path: The endpoint, after the API version.

        Returns:
            The endpoint relative to the API's base URL.
        """
        return f"/{API_VERSION}{path}"

    @staticmethod
    def _rows(response: httpx2.Response) -> list[dict[str, Any]]:
        """Unwrap a response's resources into rows, the way the paginating client reads each page.

        A row is a resource's attributes beside its own fields (``id``,
        ``type``). Campaigns and ads repeat the resource's id among their
        attributes, with the same value; the resource's own wins.

        Args:
            response: A response with a success status.

        Returns:
            One row per resource of the response's ``data``, none when it is null.

        Raises:
            CriteoApiError: When the response carries ``errors`` beside its data.
        """
        body = response.json()
        for warning in body.get("warnings") or []:
            logger.warning("Criteo warned: %s", _describe(warning))
        if body.get("errors"):
            raise CriteoApiError(body["errors"])
        rows = []
        for resource in body["data"] or []:
            fields = {key: value for key, value in resource.items() if key != "attributes"}
            rows.append({**(resource.get("attributes") or {}), **fields})
        return rows
