"""The Google Ads API engine every Google Ads request goes through."""

from __future__ import annotations

import datetime as dt
from typing import Any

import interloper as il

from interloper_assets.google.errors import GoogleApiError


class GoogleAdsApi:
    """The Google Ads REST API: GAQL over a customer's ``googleAds:searchStream``.

    Every call is scoped to a customer, and to the manager account it is
    reached through, if any, by the ``login-customer-id`` header. The stream
    answers with one JSON array of batches, each carrying ``results``; rows
    come back as Google sends them: camelCase keys nested per resource, int64
    values as strings, enums by name, unset fields omitted. The array is read
    whole before it is parsed, so a very large report is held in memory once,
    as text.

    A refusal raises as `GoogleApiError`, with Google's reason, whether Google
    answers it with an error status or, for a stream that fails after it
    started, as an error object closing the array.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every request goes through, carrying the connection's token and developer token.
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

    async def search_stream(
        self, customer_id: str, query: str, *, login_customer_id: str | None = None
    ) -> list[dict[str, Any]]:
        """Read every row a GAQL query streams for a customer.

        A refusal, before the stream or as the error object closing it, raises
        as `GoogleApiError`, with Google's reason.

        Args:
            customer_id: The customer the query reads, without hyphens.
            query: The GAQL query.
            login_customer_id: The manager account the customer is reached through; None reaches it directly.

        Returns:
            The rows of every streamed batch, in order, as Google sends them.
        """
        headers = {"login-customer-id": login_customer_id} if login_customer_id else {}
        path = f"/customers/{customer_id}/googleAds:searchStream"
        response = await self.client.post(path, json={"query": query}, headers=headers)
        if response.is_error:
            refusal = GoogleApiError.from_response(response)
            raise refusal
        rows: list[dict[str, Any]] = []
        for batch in response.json():
            if "error" in batch:
                refusal = GoogleApiError.from_body(batch, response)
                raise refusal
            rows.extend(batch.get("results") or [])
        return rows

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
            customer_id: The customer the report reads, without hyphens.
            day: The report's only day, applied as the ``segments.date`` filter.
            resource: The GAQL ``FROM`` resource, such as ``campaign`` or ``ad_group_ad``.
            fields: The GAQL ``SELECT`` field paths.
            conditions: GAQL ``WHERE`` conditions ANDed after the day, such as
                ``campaign.status != 'REMOVED'``; None filters on the day alone.
            login_customer_id: The manager account the customer is reached through; None reaches it directly.

        Returns:
            The report's rows, as Google sends them.
        """
        window = f"segments.date BETWEEN '{day.isoformat()}' AND '{day.isoformat()}'"
        where = " AND ".join([window, *(conditions or [])])
        query = f"SELECT {', '.join(fields)} FROM {resource} WHERE {where}"
        return await self.search_stream(customer_id, query, login_customer_id=login_customer_id)
