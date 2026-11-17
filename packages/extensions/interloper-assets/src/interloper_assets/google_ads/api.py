"""The Google Ads API engine every Google Ads report goes through."""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from google.ads.googleads.client import GoogleAdsClient


class GoogleAdsApi:
    """The Google Ads API: GAQL over the SDK's search stream.

    Every call is scoped to a customer, and to the manager account it is
    reached through, if any, by the ``login-customer-id`` header. The stream
    answers in batches of proto-plus rows; ``MessageToDict`` over their
    protobuf form gives exactly what the REST API sends: camelCase keys nested
    per resource, int64 values as strings, enums by name, unset fields omitted,
    and ``type`` where proto-plus would say ``type_``.

    Google's refusal is the SDK's ``GoogleAdsException``, carrying its failure
    and request id; it is raised as the SDK raises it.

    The module imports nothing from the SDK or protobuf when it loads (the
    client is passed in, ``json_format`` is imported by ``search_stream``), so
    the connection that builds it loads in the API process, which has neither.
    """

    def __init__(self, client: GoogleAdsClient) -> None:
        """Build the engine on an authenticated SDK client.

        Args:
            client: The SDK client every query goes through, carrying the connection's credentials.
        """
        self.client = client

    def search_stream(
        self, customer_id: str, query: str, *, login_customer_id: str | None = None
    ) -> list[dict[str, Any]]:
        """Read every row a GAQL query streams for a customer.

        Args:
            customer_id: The customer the query reads, without hyphens.
            query: The GAQL query.
            login_customer_id: The manager account the customer is reached through; None reaches it directly.

        Returns:
            The rows of every streamed batch, in order, in the REST JSON shape.
        """
        from google.protobuf import json_format

        metadata = [("login-customer-id", login_customer_id)] if login_customer_id else []
        service = self.client.get_service("GoogleAdsService")
        stream = service.search_stream(customer_id=customer_id, query=query, metadata=metadata)
        return [json_format.MessageToDict(type(row).pb(row)) for batch in stream for row in batch.results]

    def report(
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
            The report's rows, in the REST JSON shape.
        """
        window = f"segments.date BETWEEN '{day.isoformat()}' AND '{day.isoformat()}'"
        where = " AND ".join([window, *(conditions or [])])
        query = f"SELECT {', '.join(fields)} FROM {resource} WHERE {where}"
        return self.search_stream(customer_id, query, login_customer_id=login_customer_id)
