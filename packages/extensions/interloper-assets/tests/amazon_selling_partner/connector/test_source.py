"""Regression tests for the AmazonSellingPartner source configuration.

Vendor reports arrive as nested camelCase objects (money as
``{amount, currencyCode}``, inventory cost nested two levels deep). The
source-level ``DataFrameNormalizer`` must reach every asset instance — and
survive the host→child DAG-spec round-trip — so those rows flatten and
snake-case onto the flat schema fields instead of failing validation.

These tests pin the whole chain: live instance → spec JSON → reconstructed
child asset → normalize + validate a camelCase report row. They also pin what
each asset sends and returns against a faked Amazon: a report job (create,
poll, read the document's signed URL, download it gzipped), and a Data Kiosk
query (the same steps, the document plain JSON).
"""

from __future__ import annotations

import datetime as dt
import gzip
import json
from typing import Any

import httpx2
import interloper as il
import pandas as pd
import pytest
from interloper.asset import Asset
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData
from interloper_pandas import DataFrameNormalizer

from interloper_assets.amazon_selling_partner.connection import AmazonSellingPartnerConnection
from interloper_assets.amazon_selling_partner.connector import schemas
from interloper_assets.amazon_selling_partner.connector.source import AmazonSellingPartner

_ASSET_KEY = "vendor_inventory_retail_manufacturing_stats"
MARKETPLACE = "A1PA6795UKMFR9"
DAY = dt.date(2026, 10, 1)
API_URL = "https://sellingpartnerapi-eu.amazon.com"
REPORT_URL = "https://documents.example.com/report"
QUERY_URL = "https://documents.example.com/query"
UNPATCHED_CLIENT = httpx2.AsyncClient


def _source() -> Any:
    return AmazonSellingPartner(
        id="src-1",
        marketplace=MARKETPLACE,
        connection=AmazonSellingPartnerConnection(location="EU", client_id="x", client_secret="x", refresh_token="x"),
    )


class FakeSellingPartner:
    """Amazon's side of a report job and a Data Kiosk query, recording every request.

    Each job is done at its first poll; its document sits at a signed URL
    outside the API, gzipped for a report and plain JSON for a query. How a job
    that fails or waits is read is the engine's, pinned in ``test_api.py``.
    """

    def __init__(self, report: Any = None, query: Any = None) -> None:
        """Serve the given documents.

        Args:
            report: The report job's document, gzipped on download.
            query: The Data Kiosk query's document, served as plain JSON.
        """
        self.report = report
        self.query = query
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        """Answer a request as Amazon would, at the API or at a document's signed URL.

        Args:
            request: The request sent.

        Returns:
            Amazon's answer.
        """
        self.requests.append(request)
        responses: dict[tuple[str, str], Any] = {
            ("POST", f"{API_URL}/reports/2021-06-30/reports"): {"reportId": "R1"},
            ("GET", f"{API_URL}/reports/2021-06-30/reports/R1"): {"processingStatus": "DONE", "reportDocumentId": "D1"},
            ("GET", f"{API_URL}/reports/2021-06-30/documents/D1"): {"reportDocumentId": "D1", "url": REPORT_URL},
            ("POST", f"{API_URL}/dataKiosk/2023-11-15/queries"): {"queryId": "Q1"},
            ("GET", f"{API_URL}/dataKiosk/2023-11-15/queries/Q1"): {
                "processingStatus": "DONE",
                "dataDocumentId": "DD1",
            },
            ("GET", f"{API_URL}/dataKiosk/2023-11-15/documents/DD1"): {"documentId": "DD1", "documentUrl": QUERY_URL},
            ("GET", QUERY_URL): self.query,
        }
        if (request.method, str(request.url)) == ("GET", REPORT_URL):
            return httpx2.Response(200, content=gzip.compress(json.dumps(self.report).encode()))
        return httpx2.Response(200, json=responses[(request.method, str(request.url))])

    @property
    def calls(self) -> list[tuple[str, str]]:
        """List the requests Amazon received.

        Returns:
            Each request's method and URL, in the order they were sent.
        """
        return [(request.method, str(request.url)) for request in self.requests]


async def _run(
    key: str, vendor: FakeSellingPartner, monkeypatch: pytest.MonkeyPatch, day: dt.date = DAY
) -> list[dict[str, Any]]:
    """Run one asset against the faked Amazon, the API through the connection's client and documents outside it.

    Args:
        key: The asset's key.
        vendor: The faked Amazon, recording every request.
        monkeypatch: Routes the document downloads to the fake.
        day: The partition the asset runs for.

    Returns:
        The asset's rows.
    """
    monkeypatch.setattr(
        httpx2, "AsyncClient", lambda **kwargs: UNPATCHED_CLIENT(transport=httpx2.MockTransport(vendor), **kwargs)
    )
    connection = AmazonSellingPartnerConnection(location="EU", client_id="x", client_secret="x", refresh_token="x")
    connection.__dict__["client"] = il.AsyncRESTClient(
        API_URL, headers={"x-amz-access-token": "token"}, transport=httpx2.MockTransport(vendor)
    )
    source = AmazonSellingPartner(id="src-1", marketplace=MARKETPLACE, connection=connection)
    asset = next(asset for asset in source.assets if type(asset).key == key)
    return await asset.data(context=il.ExecutionContext(asset, il.TimePartition(value=day).window))


class TestSourceNormalizer:
    """The decorator-configured normalizer must reach every asset instance."""

    def test_source_instance_has_dataframe_normalizer(self):
        src = _source()
        assert isinstance(src.normalizer, DataFrameNormalizer)
        assert src.normalizer.snake_case_digits is True
        assert src.normalizer.flatten_max_level == 3

    def test_all_assets_inherit_the_normalizer(self):
        src = _source()
        for asset in src.assets:
            assert isinstance(asset.normalizer, DataFrameNormalizer), type(asset).key


class TestSpecRoundtrip:
    """The host→child spec round-trip must preserve the normalizer subclass."""

    def test_child_asset_keeps_dataframe_normalizer(self):
        src = _source()
        asset = next(a for a in src.assets if type(a).key == _ASSET_KEY)

        # Exactly what the k8s runner ships to the child pod.
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.operations if type(a).key == _ASSET_KEY)
        assert isinstance(child_asset, Asset)

        normalizer = child_asset.normalizer
        assert isinstance(normalizer, DataFrameNormalizer)
        assert normalizer.snake_case_digits is True
        assert normalizer.flatten_max_level == 3

    def test_nested_camelcase_report_row_conforms_after_roundtrip(self):
        """A nested API-shaped inventory row must flatten, snake-case, and validate."""
        src = _source()
        asset = next(a for a in src.assets if type(a).key == _ASSET_KEY)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.operations if type(a).key == _ASSET_KEY)
        assert isinstance(child_asset, Asset)

        # Nested money objects (cost two levels deep) and a digit-prefixed key,
        # exactly as GET_VENDOR_INVENTORY_REPORT returns them.
        row: dict[str, Any] = {
            "startDate": "2026-06-10",
            "endDate": "2026-06-10",
            "asin": "B0TEST0001",
            "sellableOnHandInventory": {"cost": {"amount": 500.0, "currencyCode": "EUR"}, "units": 40},
            "aged90PlusDaysSellableInventory": {"cost": {"amount": 10.0, "currencyCode": "EUR"}, "units": 1},
            "sellThroughRate": 0.5,
            "vendorConfirmationRate": 0.9,
            "uft": 3,
        }
        df = pd.DataFrame([row])

        normalizer = child_asset.normalizer
        assert isinstance(normalizer, DataFrameNormalizer)
        normalized = normalizer.normalize(df)

        # The two-level nesting and aged90Plus digit prefix land on schema fields.
        assert "sellable_on_hand_inventory_cost_amount" in normalized.columns
        assert "aged_90_plus_days_sellable_inventory_cost_amount" in normalized.columns

        TableData.from_data(normalized).reconcile(schemas.VendorInventoryRetailManufacturingStats)  # must not raise


class TestForecastingSnapshots:
    """The forecast report is a weekly snapshot: one partition per generation, kept only on its own run."""

    def test_asset_is_enabled_and_partitioned_on_the_generation_date(self):
        asset = next(a for a in _source().assets if type(a).key == "vendor_forecasting_retail_stats")
        assert asset.partitioning is not None and asset.partitioning.column == "forecast_generation_date"
        assert asset.tags == ["Report"]

    async def test_run_keeps_only_the_partitions_generation(self, monkeypatch: pytest.MonkeyPatch):
        rows = [
            {"asin": "A", "forecastGenerationDate": "2026-09-13", "startDate": "2026-09-13"},
            {"asin": "A", "forecastGenerationDate": "2026-09-13", "startDate": "2026-09-20"},
            {"asin": "B", "forecastGenerationDate": "2026-09-06", "startDate": "2026-09-06"},
        ]
        vendor = FakeSellingPartner(report={"forecastByAsin": rows})

        generation_day = await _run("vendor_forecasting_retail_stats", vendor, monkeypatch, dt.date(2026, 9, 13))
        between_generations = await _run("vendor_forecasting_retail_stats", vendor, monkeypatch, dt.date(2026, 9, 14))

        assert [row["startDate"] for row in generation_day] == ["2026-09-13", "2026-09-20"]
        assert between_generations == []

    async def test_asks_for_the_latest_generation_without_a_window(self, monkeypatch: pytest.MonkeyPatch):
        vendor = FakeSellingPartner(report={"forecastByAsin": []})

        await _run("vendor_forecasting_retail_stats", vendor, monkeypatch)

        assert json.loads(vendor.requests[0].content) == {
            "reportType": "GET_VENDOR_FORECASTING_REPORT",
            "marketplaceIds": [MARKETPLACE],
            "reportOptions": {"sellingProgram": "RETAIL"},
        }

    def test_forecast_row_conforms_with_date_typed_columns(self):
        src = _source()
        asset = next(a for a in src.assets if type(a).key == "vendor_forecasting_retail_stats")
        rows = [
            {
                "asin": "B00TEST",
                "startDate": "2026-09-13",
                "endDate": "2026-09-19",
                "forecastGenerationDate": "2026-09-13",
                "meanForecastUnits": 12.5,
                "p70ForecastUnits": 10.0,
                "p80ForecastUnits": 9.0,
                "p90ForecastUnits": 7.0,
            }
        ]
        normalizer = asset.normalizer
        assert normalizer is not None
        out = TableData.from_data(normalizer.normalize(rows)).reconcile(schemas.VendorForecastingRetailStats)
        assert str(out.loc[0, "forecast_generation_date"])[:10] == "2026-09-13"
        assert set(out.columns) == set(schemas.VendorForecastingRetailStats.model_fields)


DAY_WINDOW = {"dataStartTime": "2026-10-01T00:00:00.000Z", "dataEndTime": "2026-10-01T23:59:59.999Z"}
REPORT_CALLS = [
    ("POST", f"{API_URL}/reports/2021-06-30/reports"),
    ("GET", f"{API_URL}/reports/2021-06-30/reports/R1"),
    ("GET", f"{API_URL}/reports/2021-06-30/documents/D1"),
    ("GET", REPORT_URL),
]
QUERY_CALLS = [
    ("POST", f"{API_URL}/dataKiosk/2023-11-15/queries"),
    ("GET", f"{API_URL}/dataKiosk/2023-11-15/queries/Q1"),
    ("GET", f"{API_URL}/dataKiosk/2023-11-15/documents/DD1"),
    ("GET", QUERY_URL),
]
SALES = "GET_VENDOR_SALES_REPORT"
INVENTORY = "GET_VENDOR_INVENTORY_REPORT"
DAILY_REPORTS = [
    ("vendor_traffic_stats", "GET_VENDOR_TRAFFIC_REPORT", {"reportPeriod": "DAY"}, "trafficByAsin"),
    (
        "vendor_sales_retail_manufacturing_stats",
        SALES,
        {"reportPeriod": "DAY", "sellingProgram": "RETAIL", "distributorView": "MANUFACTURING"},
        "salesByAsin",
    ),
    (
        "vendor_sales_retail_sourcing_stats",
        SALES,
        {"reportPeriod": "DAY", "sellingProgram": "RETAIL", "distributorView": "SOURCING"},
        "salesByAsin",
    ),
    (
        "vendor_sales_business_manufacturing_stats",
        SALES,
        {"reportPeriod": "DAY", "sellingProgram": "BUSINESS", "distributorView": "MANUFACTURING"},
        "salesByAsin",
    ),
    (
        "vendor_sales_business_sourcing_stats",
        SALES,
        {"reportPeriod": "DAY", "sellingProgram": "BUSINESS", "distributorView": "SOURCING"},
        "salesByAsin",
    ),
    (
        "vendor_inventory_retail_manufacturing_stats",
        INVENTORY,
        {"reportPeriod": "DAY", "sellingProgram": "RETAIL", "distributorView": "MANUFACTURING"},
        "inventoryByAsin",
    ),
    (
        "vendor_inventory_retail_sourcing_stats",
        INVENTORY,
        {"reportPeriod": "DAY", "sellingProgram": "RETAIL", "distributorView": "SOURCING"},
        "inventoryByAsin",
    ),
    (
        "vendor_net_pure_product_margin_stats",
        "GET_VENDOR_NET_PURE_PRODUCT_MARGIN_REPORT",
        {"reportPeriod": "DAY"},
        "netPureProductMarginByAsin",
    ),
]
QUERY_ASSETS = ("products", "product_fulfillment_stats", "geo_sales_stats_by_country")


class TestReportRequests:
    """Each daily vendor report is one report job for the partition's day, read from one list of its document."""

    def test_every_asset_is_pinned(self):
        pinned = {report[0] for report in DAILY_REPORTS} | {"vendor_forecasting_retail_stats", *QUERY_ASSETS}

        assert {type(asset).key for asset in _source().assets} == pinned

    @pytest.mark.parametrize(("key", "report_type", "options", "records"), DAILY_REPORTS)
    async def test_a_report_runs_one_job_for_its_day(
        self, monkeypatch: pytest.MonkeyPatch, key: str, report_type: str, options: dict[str, str], records: str
    ):
        rows = [{"asin": "B1", "startDate": "2026-10-01"}, {"asin": "B2", "startDate": "2026-10-01"}]
        document = {"reportSpecification": {"reportType": report_type}, records: rows, "aggregate": [{"asin": "X"}]}
        vendor = FakeSellingPartner(report=document)

        assert await _run(key, vendor, monkeypatch) == rows
        assert vendor.calls == REPORT_CALLS
        body = json.loads(vendor.requests[0].content)
        assert body == {
            "reportType": report_type,
            "marketplaceIds": [MARKETPLACE],
            **DAY_WINDOW,
            "reportOptions": options,
        }
        assert list(body) == ["reportType", "marketplaceIds", "reportOptions", "dataStartTime", "dataEndTime"]


class TestQueryRequests:
    """The Data Kiosk assets run one GraphQL query for the partition's day and flatten its metric records."""

    async def test_products_reads_the_identifiers_of_each_asin(self, monkeypatch: pytest.MonkeyPatch):
        metric = {"groupByKey": {"asin": "B1", "parentAsin": "P1", "ean": "E1", "upc": None, "isbn13": "I1"}}
        vendor = FakeSellingPartner(query={"metrics": [metric]})

        rows = await _run("products", vendor, monkeypatch)

        assert rows == [
            {"asin": "B1", "parent_asin": "P1", "ean": "E1", "upc": None, "isbn_13": "I1", "date": DAY},
        ]
        assert vendor.calls == QUERY_CALLS
        query = json.loads(vendor.requests[0].content)["query"]
        assert "analytics_vendorAnalytics_2024_09_30" in query
        assert 'sourcingView(aggregateBy: DAY, startDate: "2026-10-01", endDate: "2026-10-01")' in query
        assert "groupByKey { asin parentAsin ean upc isbn13 }" in query

    async def test_product_fulfillment_stats_flattens_the_sourcing_metrics(self, monkeypatch: pytest.MonkeyPatch):
        metric = {
            "groupByKey": {"asin": "B1"},
            "metrics": {
                "customerSatisfaction": {"customerReturns": 2},
                "sourcing": {
                    "openPurchaseOrderQuantity": 3,
                    "confirmedUnits": 4,
                    "mostRecentSubmitted": ["not", "a", "scalar"],
                    "netReceived": {"units": 5, "value": {"amount": 6.5, "currencyCode": "EUR"}},
                    "overallVendorLeadTime": 7.0,
                    "procurableProductOOS": 0.1,
                    "receivedFillRate": 0.9,
                    "vendorConfirmationRate": 0.8,
                },
            },
        }
        document = {"analytics_vendorAnalytics_2024_09_30": {"sourcingView": {"metrics": [metric]}}}
        vendor = FakeSellingPartner(query=document)

        rows = await _run("product_fulfillment_stats", vendor, monkeypatch)

        assert rows == [
            {
                "asin": "B1",
                "customer_returns": 2,
                "open_purchase_order_quantity": 3,
                "confirmed_units": 4,
                "most_recent_submitted": None,
                "net_received_units": 5,
                "net_received_value_amount": 6.5,
                "net_received_value_currency_code": "EUR",
                "overall_vendor_lead_time": 7.0,
                "procurable_product_oos": 0.1,
                "received_fill_rate": 0.9,
                "vendor_confirmation_rate": 0.8,
                "date": DAY,
            }
        ]
        query = json.loads(vendor.requests[0].content)["query"]
        assert 'sourcingView(aggregateBy: DAY, startDate: "2026-10-01", endDate: "2026-10-01")' in query
        assert "netReceived { units value { amount currencyCode } }" in query

    async def test_geo_sales_stats_by_country_flattens_the_sales_and_costs(self, monkeypatch: pytest.MonkeyPatch):
        money = {"amount": 1.5, "currencyCode": "EUR"}
        metric = {
            "groupByKey": {"asin": "B1", "shipToCountryCode": "DE"},
            "metrics": {
                "shippedOrders": {
                    "shippedUnitsWithRevenue": {"value": {"amount": 9.0, "currencyCode": "EUR"}, "units": 3}
                },
                "costs": {
                    "netPPM": 0.2,
                    "shippedCogs": money,
                    "contraCogsPerUnit": money,
                    "contraCogs": money,
                    "averageSalesDiscount": money,
                },
            },
        }
        vendor = FakeSellingPartner(query={"metrics": [metric]})

        rows = await _run("geo_sales_stats_by_country", vendor, monkeypatch)

        assert rows == [
            {
                "asin": "B1",
                "ship_to_country_code": "DE",
                "shipped_units_with_revenue_value_amount": 9.0,
                "shipped_units_with_revenue_value_currency_code": "EUR",
                "shipped_units_with_revenue_units": 3,
                "net_ppm": 0.2,
                "shipped_cogs_amount": 1.5,
                "shipped_cogs_currency_code": "EUR",
                "contra_cogs_per_unit_amount": 1.5,
                "contra_cogs_per_unit_currency_code": "EUR",
                "contra_cogs_amount": 1.5,
                "contra_cogs_currency_code": "EUR",
                "average_sales_discount_amount": 1.5,
                "average_sales_discount_currency_code": "EUR",
                "date": DAY,
            }
        ]
        query = json.loads(vendor.requests[0].content)["query"]
        assert "groupByKey { asin shipToCountryCode }" in query
