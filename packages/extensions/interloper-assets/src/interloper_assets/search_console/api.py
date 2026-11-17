"""The Search Console API engine every Search Console request goes through."""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any

from interloper_assets.search_console.constants import ROW_LIMIT

if TYPE_CHECKING:
    from googleapiclient.discovery import Resource


class SearchConsoleApi:
    """The Search Console API: its discovery methods and its Search Analytics report.

    Every request goes through Google's discovery client, which encodes it from
    the API's discovery document, so a method is named the way Google names it:
    its resources, then its verb (``sites.list``, ``urlInspection.index.inspect``).
    Google refuses a request with an HTTP status, which the client raises as
    ``HttpError`` carrying Google's message; nothing is retried beyond
    google-auth's token refresh on a 401.

    A Search Analytics row only makes sense against the query that asked for
    it: its dimension values are a positional ``keys`` list, and its search
    type is the query's ``type``, which it does not echo. The engine sent that
    query, so ``report`` names each key after its dimension and tags each row
    with its ``search_type``.

    The client is passed in and this module imports nothing from the SDK, so the
    connection that builds the engine loads where the SDK is not installed.
    """

    def __init__(self, client: Resource) -> None:
        """Build the engine on an authenticated discovery client.

        Args:
            client: The ``searchconsole`` v1 discovery client every request goes through,
                carrying the connection's credentials. Its transport is not thread-safe,
                so the engine is never shared across threads.
        """
        self.client = client

    def get(self, method: str, **params: Any) -> dict[str, Any]:
        """Read the one response of a method.

        Args:
            method: The method, by its resources and its verb, such as ``sites.list``.
            **params: The method's parameters as the discovery client takes them: path and
                query parameters by name (``siteUrl``), the request body as ``body``.

        Returns:
            The response's JSON body.
        """
        *resources, verb = method.split(".")
        target = self.client
        for resource in resources:
            target = getattr(target, resource)()
        return getattr(target, verb)(**params).execute()

    def report(
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
            for row in self._rows(site_url, query):
                record = {dimension.lower(): key for dimension, key in zip(dimensions, row.get("keys") or [])}
                record.update({name: value for name, value in row.items() if name != "keys"})
                record["search_type"] = search_type.lower()
                records.append(record)
        return records

    def _rows(self, site_url: str, query: dict[str, Any]) -> list[dict[str, Any]]:
        """Read every row of one Search Analytics query, page by page.

        A response holds at most ``ROW_LIMIT`` rows, so a full page asks for the
        next one from ``startRow``, and a short or empty page is the last.

        Args:
            site_url: The property the query reads.
            query: The query's body, without its paging.

        Returns:
            The rows of every page, in order, as Google sends them.
        """
        rows: list[dict[str, Any]] = []
        start_row = 0
        while True:
            body = {**query, "rowLimit": ROW_LIMIT, "startRow": start_row}
            page = self.get("searchanalytics.query", siteUrl=site_url, body=body).get("rows") or []
            rows.extend(page)
            if len(page) < ROW_LIMIT:
                return rows
            start_row += ROW_LIMIT
