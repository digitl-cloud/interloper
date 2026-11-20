"""The Search Ads 360 Reporting API engine every Search Ads 360 request goes through."""

from __future__ import annotations

import datetime as dt
import logging
from typing import Any

import interloper as il

from interloper_assets.google.errors import GoogleApiError

logger = logging.getLogger(__name__)


class SearchAds360Api:
    """The Search Ads 360 Reporting API: GAQL over a customer's ``searchAds360:search``.

    Every call is scoped to a customer, and to the manager account it is
    reached through, if any, by the ``login-customer-id`` header. Rows come
    back as the REST API sends them: camelCase keys nested per resource, int64
    values as strings, enums by name.

    The endpoint is a POST whose paging token travels in the body, so the
    query-parameter paginators do not apply: `search` walks the pages itself,
    until a page names no ``nextPageToken``, or names one already sent, which
    would read the same pages again. A refusal raises as `GoogleApiError`,
    with Google's reason.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, authenticated as the connection's service account.
        """
        self.client = client

    async def get(self, path: str, **params: Any) -> dict[str, Any]:
        """Read the one response of an endpoint that does not page.

        A refusal raises as `GoogleApiError`, with Google's reason.

        Args:
            path: The endpoint's path, such as ``/customers:listAccessibleCustomers``.
            **params: The query parameters, under the vendor's names.

        Returns:
            The response as the vendor sends it.
        """
        response = await self.client.get(path, params=params)
        if response.is_error:
            refusal = GoogleApiError.from_response(response)
            raise refusal
        return response.json()

    async def search(
        self,
        customer_id: str,
        *,
        resource: str,
        fields: list[str],
        conditions: list[str] | None = None,
        login_customer_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Read every row of a GAQL query for a customer.

        Args:
            customer_id: The customer the query reads.
            resource: The GAQL ``FROM`` resource, such as ``campaign`` or ``customer_client``.
            fields: The GAQL ``SELECT`` field paths.
            conditions: GAQL ``WHERE`` conditions, ANDed; None or an empty list reads the resource unfiltered.
            login_customer_id: The manager account the customer is reached through; None reaches it directly.

        Returns:
            The ``results`` rows of every page, in page order.
        """
        query = f"SELECT {', '.join(fields)} FROM {resource}"
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        headers = {"login-customer-id": login_customer_id} if login_customer_id else {}
        path = f"/customers/{customer_id}/searchAds360:search"
        body = {"query": query}
        rows: list[dict[str, Any]] = []
        sent: set[str] = set()
        while True:
            response = await self.client.post(path, json=body, headers=headers)
            if response.is_error:
                refusal = GoogleApiError.from_response(response)
                raise refusal
            page = response.json()
            rows.extend(page.get("results") or [])
            page_token = page.get("nextPageToken")
            if not page_token:
                return rows
            if page_token in sent:
                logger.warning(f"{path} returned the page token {page_token} again; reading no further")
                return rows
            sent.add(page_token)
            body = {"query": query, "pageToken": page_token}

    async def report(
        self,
        customer_id: str,
        day: dt.date,
        *,
        resource: str,
        fields: list[str],
        conditions: list[str] | None = None,
        login_customer_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Read one day of a GAQL report.

        Args:
            customer_id: The customer the report reads.
            day: The report's only day, applied as the ``segments.date`` filter.
            resource: The GAQL ``FROM`` resource, such as ``campaign``.
            fields: The GAQL ``SELECT`` field paths.
            conditions: GAQL ``WHERE`` conditions ANDed after the day, such as
                ``campaign.status = 'ENABLED'``; None filters on the day alone.
            login_customer_id: The manager account the customer is reached through; None reaches it directly.

        Returns:
            The report's rows, in page order.
        """
        window = f"segments.date BETWEEN '{day.isoformat()}' AND '{day.isoformat()}'"
        return await self.search(
            customer_id,
            resource=resource,
            fields=fields,
            conditions=[window, *(conditions or [])],
            login_customer_id=login_customer_id,
        )
