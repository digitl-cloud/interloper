"""Tests for the Google Ads API engine.

The engine runs GAQL through the SDK's ``GoogleAdsService.search_stream``,
scoped to a customer and, through a manager account, to its
``login-customer-id`` header, and returns every streamed row in the REST JSON
shape. These tests drive it against a fake SDK client.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import pytest
from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.v23.services.types.google_ads_service import GoogleAdsRow

from interloper_assets.google_ads.api import GoogleAdsApi

QUERY = "SELECT campaign.id FROM campaign"


def _row(campaign_id: int) -> GoogleAdsRow:
    row = GoogleAdsRow()
    row.campaign.id = campaign_id
    return row


class _FakeClient:
    """The SDK client and its ``GoogleAdsService``: streams ``batches``, raising ``error`` after them."""

    def __init__(self, *batches: list[GoogleAdsRow], error: Exception | None = None):
        self.batches = batches
        self.error = error
        self.service_names: list[str] = []
        self.calls: list[dict[str, Any]] = []

    def get_service(self, name: str) -> _FakeClient:
        self.service_names.append(name)
        return self

    def search_stream(self, **kwargs: Any) -> Iterator[Any]:
        self.calls.append(kwargs)
        return self._stream()

    def _stream(self) -> Iterator[Any]:
        for rows in self.batches:
            yield SimpleNamespace(results=rows)
        if self.error is not None:
            raise self.error


class TestSearchStream:
    def test_streams_the_query_for_the_customer(self):
        client = _FakeClient([_row(1)])

        assert GoogleAdsApi(client).search_stream("123", QUERY) == [{"campaign": {"id": "1"}}]
        assert client.service_names == ["GoogleAdsService"]
        assert client.calls == [{"customer_id": "123", "query": QUERY, "metadata": []}]

    def test_a_manager_account_is_sent_as_the_login_customer_id(self):
        client = _FakeClient([_row(1)])

        GoogleAdsApi(client).search_stream("123", QUERY, login_customer_id="999")

        assert client.calls == [{"customer_id": "123", "query": QUERY, "metadata": [("login-customer-id", "999")]}]

    def test_reads_every_batch_in_order(self):
        client = _FakeClient([_row(1), _row(2)], [], [_row(3)])

        rows = GoogleAdsApi(client).search_stream("123", QUERY)

        assert rows == [{"campaign": {"id": "1"}}, {"campaign": {"id": "2"}}, {"campaign": {"id": "3"}}]

    def test_an_empty_stream_has_no_rows(self):
        assert GoogleAdsApi(_FakeClient()).search_stream("123", QUERY) == []

    def test_rows_take_the_rest_json_shape(self):
        row = GoogleAdsRow()
        row.ad_group_ad.ad.id = 111
        row.ad_group_ad.ad.type_ = "RESPONSIVE_SEARCH_AD"
        row.campaign.network_settings.target_search_network = False
        row.metrics.cost_micros = 12_500_000
        row.metrics.ctr = 0.5

        (record,) = GoogleAdsApi(_FakeClient([row])).search_stream("123", QUERY)

        assert record == {
            "adGroupAd": {"ad": {"id": "111", "type": "RESPONSIVE_SEARCH_AD"}},
            "campaign": {"networkSettings": {"targetSearchNetwork": False}},
            "metrics": {"costMicros": "12500000", "ctr": 0.5},
        }

    def test_a_refusal_mid_stream_raises_the_sdks_exception_unchanged(self):
        refusal = GoogleAdsException(error=None, call=None, failure=None, request_id="request-1")
        client = _FakeClient([_row(1)], error=refusal)

        with pytest.raises(GoogleAdsException) as raised:
            GoogleAdsApi(client).search_stream("123", QUERY)

        assert raised.value is refusal

    def test_a_transport_error_raises_unchanged(self):
        error = ConnectionError("unreachable")

        with pytest.raises(ConnectionError) as raised:
            GoogleAdsApi(_FakeClient(error=error)).search_stream("123", QUERY)

        assert raised.value is error


class TestReport:
    def test_queries_the_resource_over_its_one_day(self):
        client = _FakeClient([_row(1)])

        rows = GoogleAdsApi(client).report(
            "123", dt.date(2026, 6, 14), resource="campaign", fields=["campaign.id", "metrics.clicks"]
        )

        assert rows == [{"campaign": {"id": "1"}}]
        assert client.calls == [
            {
                "customer_id": "123",
                "query": "SELECT campaign.id, metrics.clicks FROM campaign "
                "WHERE segments.date BETWEEN '2026-06-14' AND '2026-06-14'",
                "metadata": [],
            }
        ]

    def test_conditions_narrow_the_day(self):
        client = _FakeClient()

        GoogleAdsApi(client).report(
            "123",
            dt.date(2026, 6, 14),
            resource="campaign",
            fields=["campaign.id"],
            conditions=["campaign.status != 'REMOVED'", "metrics.impressions > 0"],
        )

        assert client.calls[0]["query"] == (
            "SELECT campaign.id FROM campaign WHERE segments.date BETWEEN '2026-06-14' AND '2026-06-14' "
            "AND campaign.status != 'REMOVED' AND metrics.impressions > 0"
        )

    def test_a_manager_account_is_sent_as_the_login_customer_id(self):
        client = _FakeClient()

        GoogleAdsApi(client).report(
            "123", dt.date(2026, 6, 14), resource="ad_group_ad", fields=["ad_group_ad.ad.id"], login_customer_id="999"
        )

        assert client.calls[0]["metadata"] == [("login-customer-id", "999")]
