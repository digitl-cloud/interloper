"""The Search Console API engine every Search Console request goes through."""

from __future__ import annotations

import datetime as dt
from typing import Any
from urllib.parse import quote

import interloper as il

from interloper_assets.search_console.constants import ROW_LIMIT


class SearchConsoleApi:
    """The Search Console API: its JSON reads and its Search Analytics report.

    Google refuses a request by its HTTP status, raised as
    ``httpx2.HTTPStatusError``. A property is addressed by its URL, encoded
    whole into the path: ``https://example.com/`` and ``sc-domain:example.com``
    are one path segment each.

    A Search Analytics row only makes sense against the query that asked for
    it: its dimension values are a positional ``keys`` list, and its search
    type is the query's ``type``, which it does not echo. The engine sent that
    query, so ``report`` names each key after its dimension and tags each row
    with its ``search_type``.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, authenticated as the connection's service account.
        """
        self.client = client

    async def get(self, path: str, **params: Any) -> dict[str, Any]:
        """Read the one response of a GET.

        Args:
            path: The endpoint's path, such as ``/webmasters/v3/sites``.
            **params: The query parameters, under the vendor's names.

        Returns:
            The response's JSON body.
        """
        response = await self.client.get(path, params=params)
        response.raise_for_status()
        return response.json()

    async def report(
        self,
        site_url: str,
        day: dt.date,
        *,
        dimensions: list[str],
        search_types: list[str],
        **body: Any,
    ) -> list[dict[str, Any]]:
        """Read one day of a Search Analytics report, every page of every search type.

        The query's ``type`` takes a single search type, so each is queried in
        turn and every row is tagged with the ``search_type`` it was asked for,
        which the row does not echo. A row carries its dimension values as a
        positional ``keys`` list aligned to ``dimensions``; each value lands on
        a column named after its dimension (``SEARCH_APPEARANCE`` becomes
        ``search_appearance``).

        Args:
            site_url: The property the report reads, such as ``https://example.com/`` or ``sc-domain:example.com``.
            day: The report's only day.
            dimensions: The dimensions to group by, in the order of the rows' ``keys``.
            search_types: The search types to query, one after the other, such as ``WEB`` or ``DISCOVER``.
            **body: The rest of the query in Google's words, such as ``aggregationType`` and ``dataState``;
                never the keys this method sets (``startDate``, ``endDate``, ``dimensions``, ``type``,
                ``rowLimit``, ``startRow``).

        Returns:
            The rows of every search type, in order, each with one column per dimension,
            the metrics as Google sends them, and ``search_type`` in lower case.
        """
        window = {"startDate": day.isoformat(), "endDate": day.isoformat()}
        records: list[dict[str, Any]] = []
        for search_type in search_types:
            query = {**window, "dimensions": dimensions, "type": search_type, **body}
            for row in await self._rows(site_url, query):
                record = {
                    dimension.lower(): key for dimension, key in zip(dimensions, row.get("keys") or [], strict=False)
                }
                record.update({name: value for name, value in row.items() if name != "keys"})
                record["search_type"] = search_type.lower()
                records.append(record)
        return records

    async def _rows(self, site_url: str, query: dict[str, Any]) -> list[dict[str, Any]]:
        """Read every row of one Search Analytics query, page by page.

        A response holds at most ``ROW_LIMIT`` rows, so a full page asks for the
        next one from ``startRow``, and a short or empty page is the last.

        Args:
            site_url: The property the query reads.
            query: The query's body, without its paging.

        Returns:
            The rows of every page, in order, as Google sends them.
        """
        path = f"/webmasters/v3/sites/{quote(site_url, safe='')}/searchAnalytics/query"
        rows: list[dict[str, Any]] = []
        start_row = 0
        while True:
            response = await self.client.post(path, json={**query, "rowLimit": ROW_LIMIT, "startRow": start_row})
            response.raise_for_status()
            page = response.json().get("rows") or []
            rows.extend(page)
            if len(page) < ROW_LIMIT:
                return rows
            start_row += ROW_LIMIT
