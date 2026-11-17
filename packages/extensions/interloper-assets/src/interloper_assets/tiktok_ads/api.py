"""The TikTok Business API engine every TikTok request goes through."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import httpx2
import interloper as il

from interloper_assets.tiktok_ads.constants import PAGE_CONCURRENCY, PAGE_SIZE


class TiktokAdsApiError(RuntimeError):
    """TikTok refused a request, with its own code and message."""


class TiktokAdsApi:
    """The TikTok Business API: its envelope, its pages and its integrated report.

    TikTok answers a refused request with HTTP 200 and a non-zero ``code``, so
    the status proves nothing: every response is read through `_data`, which
    raises TikTok's code and message. The message names the cause (a token
    issued to another app, a revoked token) and carries neither the app secret
    nor the token.

    List and object parameters (``filtering``, ``dimensions``,
    ``advertiser_ids``) travel as JSON strings; every method encodes them, so
    a caller passes plain values.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, carrying the connection's access token.
        """
        self.client = client

    async def get(self, path: str, **params: Any) -> dict[str, Any]:
        """Read the one response of an endpoint that does not page.

        Args:
            path: The endpoint, relative to the API's base URL, such as ``/advertiser/info/``.
            **params: The query parameters; a list or dict value is sent as JSON.

        Returns:
            The response's ``data`` object.
        """
        response = await self.client.get(path, params=self._query(params))
        response.raise_for_status()
        return self._data(response)

    async def rows(self, path: str, **params: Any) -> list[dict[str, Any]]:
        """Read every row of a paged endpoint.

        The first page reports ``data.page_info.total_page``, so the other
        pages are fetched concurrently, at most ``PAGE_CONCURRENCY`` at once.
        A response without one is read as the only page: an endpoint that does
        not page ignores ``page`` and would answer the same list forever.

        Args:
            path: The endpoint, relative to the API's base URL, such as ``/campaign/get/``.
            **params: The query parameters; a list or dict value is sent as JSON.

        Returns:
            The ``data.list`` rows of every page, in page order.
        """
        pages = self.client.paginate(
            path,
            il.PageNumberPaginator(total_path="data.page_info.total_page", maximum_page=1),
            params={**self._query(params), "page_size": PAGE_SIZE},
            data_selector=self._rows,
            concurrency=PAGE_CONCURRENCY,
        )
        return [row async for page in pages for row in page]

    async def report(self, day: dt.date, **params: Any) -> list[dict[str, Any]]:
        """Read one day of any integrated report.

        Args:
            day: The report's only day.
            **params: What the report is: its ``advertiser_id``, ``service_type``,
                ``data_level``, ``report_type``, ``dimensions`` and ``metrics``.

        Returns:
            The report's rows, each nesting its ``dimensions`` and its ``metrics``.
        """
        window = {"start_date": day.isoformat(), "end_date": day.isoformat()}
        return await self.rows("/report/integrated/get/", **window, **params)

    @staticmethod
    def _query(params: dict[str, Any]) -> dict[str, Any]:
        """Encode list and object parameters as JSON strings, as TikTok reads them.

        Args:
            params: The query parameters as the caller passed them.

        Returns:
            The parameters, each list or dict value as JSON and every other value unchanged.
        """
        return {key: json.dumps(value) if isinstance(value, list | dict) else value for key, value in params.items()}

    @staticmethod
    def _data(response: httpx2.Response) -> dict[str, Any]:
        """Unwrap a response's ``data``, raising TikTok's refusal.

        Args:
            response: A response with a success status.

        Returns:
            The response's ``data`` object.

        Raises:
            TiktokAdsApiError: If TikTok answered with a non-zero ``code``.
        """
        body = response.json()
        if body.get("code") != 0:
            raise TiktokAdsApiError(f"TikTok API error {body.get('code')}: {body.get('message')}")
        return body["data"]

    @classmethod
    def _rows(cls, response: httpx2.Response) -> list[dict[str, Any]]:
        """Select a page's rows, the way the paginating client reads each page.

        Args:
            response: A page's response, with a success status.

        Returns:
            The page's ``data.list``.
        """
        return cls._data(response)["list"]
