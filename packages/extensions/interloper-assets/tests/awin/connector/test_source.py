"""Regression tests for the Awin source configuration.

Awin returns camelCase fields, and transactions embed money as nested
``{"amount", "currency"}`` objects (``commissionAmount``, ``saleAmount``, …),
click references as ``{"clickRef": …}``, and transaction parts as a list. A
plain ``DataFrameNormalizer`` snake-cases the publisher report; transactions
need a custom ``AwinTransactionsNormalizer`` that splits the money objects into
``*_amount`` / ``*_currency`` scalars first — otherwise the dict-valued cell
reaches reconcile and fails to cast to ``float`` (observed live). These tests
pin the reshape, that the normalizer survives the host→child spec round-trip,
that a raw API-shaped row reconciles against the schema, and the request each
asset sends for its day and the ``date`` it stamps on the rows.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any

import httpx2
import interloper as il
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData
from interloper_pandas import DataFrameNormalizer

from interloper_assets.awin.connection import AwinConnection
from interloper_assets.awin.connector.schemas import Transactions
from interloper_assets.awin.connector.source import Awin, AwinTransactionsNormalizer, _reshape_transaction
from interloper_assets.awin.constants import BASE_URL

DAY = dt.date(2026, 10, 1)


def _source() -> Any:
    return Awin(id="src-1", advertiser_id="12345", connection=AwinConnection(access_token="x"))


class TestSourceNormalizer:
    def test_transactions_uses_the_custom_normalizer(self):
        asset = next(a for a in _source().assets if type(a).key == "transactions")
        assert isinstance(asset.normalizer, AwinTransactionsNormalizer)

    def test_publishers_uses_the_plain_normalizer(self):
        asset = next(a for a in _source().assets if type(a).key == "publishers_stats")
        assert isinstance(asset.normalizer, DataFrameNormalizer)
        assert not isinstance(asset.normalizer, AwinTransactionsNormalizer)


class TestReshape:
    """Splitting the nested money/click-ref/parts objects is the crux of the port."""

    def test_money_objects_split_into_amount_and_currency(self):
        row = _reshape_transaction(
            {
                "commissionAmount": {"amount": 120.0, "currency": "EUR"},
                "saleAmount": {"amount": 5842.86, "currency": "EUR"},
            }
        )
        assert row["commissionAmount"] == 120.0
        assert row["commissionCurrency"] == "EUR"
        assert row["saleAmount"] == 5842.86
        assert row["saleCurrency"] == "EUR"

    def test_click_ref_is_lifted_and_parts_json_encoded(self):
        row = _reshape_transaction(
            {
                "clickRefs": {"clickRef": "https://ad4mat.com/native-network"},
                "transactionParts": [{"commissionGroupId": 1, "amount": 5842.86}],
            }
        )
        assert row["clickRef"] == "https://ad4mat.com/native-network"
        assert "clickRefs" not in row
        assert isinstance(row["transactionParts"], str)


class TestSpecRoundtripAndReconcile:
    """Normalizer survives the host→child spec round-trip and a raw row reconciles."""

    def _child(self, key: str) -> Any:
        src = _source()
        asset = next(a for a in src.assets if type(a).key == key)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        return next(a for a in child_dag.operations if type(a).key == key)

    def test_transaction_row_reconciles_against_schema(self):
        child = self._child("transactions")
        assert isinstance(child.normalizer, AwinTransactionsNormalizer)

        rows = [
            {
                "id": 123,
                "commissionAmount": {"amount": 120.0, "currency": "EUR"},
                "saleAmount": {"amount": 5842.86, "currency": "EUR"},
                "clickRefs": {"clickRef": "ref-1"},
                "transactionParts": [{"commissionGroupId": 1, "amount": 5842.86}],
                "date": "2026-06-09",
            }
        ]
        normalized = child.normalizer.normalize(rows)
        assert "commission_amount" in normalized.columns
        reconciled = TableData.from_data(normalized).reconcile(Transactions)
        assert float(reconciled.loc[0, "commission_amount"]) == 120.0
        assert reconciled.loc[0, "commission_currency"] == "EUR"


class TestRequests:
    """Each asset sends one GET for its day, with no body, and stamps the day on every row it returns.

    The client is the test's own; ``tests/awin/test_connection.py`` pins the connection's bearer.
    """

    def _run(self, key: str, response: httpx2.Response) -> tuple[list[dict[str, Any]], list[httpx2.Request]]:
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return response

        connection = AwinConnection(id="awin", access_token="token")
        connection.__dict__["client"] = il.AsyncRESTClient(
            BASE_URL, auth=il.HTTPBearerAuth("token"), transport=httpx2.MockTransport(handler)
        )
        source = Awin(id="src-1", advertiser_id="12345", connection=connection)
        asset = next(a for a in source.assets if type(a).key == key)
        context = il.ExecutionContext(asset, il.TimePartition(value=DAY).window)
        return asyncio.run(asset.data(context=context)), requests

    def test_transactions_ask_for_the_whole_day_in_utc_with_basket_products(self):
        transaction = {"id": 1, "commissionAmount": {"amount": 1.5, "currency": "EUR"}}

        rows, requests = self._run("transactions", httpx2.Response(200, json=[transaction]))

        assert rows == [{**transaction, "date": DAY}]
        (request,) = requests
        assert request.method == "GET"
        assert str(request.url) == (
            "https://api.awin.com/advertisers/12345/transactions/"
            "?startDate=2026-10-01T00%3A00%3A00&endDate=2026-10-01T23%3A59%3A59&timezone=UTC&showBasketProducts=true"
        )
        assert request.content == b""

    def test_publishers_stats_ask_the_publisher_report_for_the_day_in_utc(self):
        publisher = {"publisherId": 7, "publisherName": "p", "clicks": 3}

        rows, requests = self._run("publishers_stats", httpx2.Response(200, json=[publisher]))

        assert rows == [{**publisher, "date": DAY}]
        (request,) = requests
        assert request.method == "GET"
        assert str(request.url) == (
            "https://api.awin.com/advertisers/12345/reports/publisher"
            "?startDate=2026-10-01&endDate=2026-10-01&timezone=UTC"
        )
        assert request.content == b""
