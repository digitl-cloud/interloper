import asyncio
import datetime as dt
import gzip
import json
import logging
from typing import Any

import httpx
import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.amazon_selling_partner import constants, schemas
from interloper_assets.amazon_selling_partner.connection import AmazonSellingPartnerConnection

logger = logging.getLogger(__name__)

_Record = dict[str, Any]

# Polling cadence for async reports and Data Kiosk queries (both queue server-side).
_POLL_INTERVAL = 60.0  # seconds between status polls
_REPORT_TIMEOUT = 7200.0  # 2h — vendor reports can queue for a long time
_QUERY_TIMEOUT = 3600.0  # 1h for Data Kiosk queries


# -- HELPERS — nested-dict access ----------------------------------------------
def _nested(value: Any, *keys: str) -> Any:
    """Walk *keys* through nested dicts, returning ``None`` on any missing hop."""
    for key in keys:
        value = value.get(key) if isinstance(value, dict) else None
    return value


def _scalars(row: dict[str, Any]) -> dict[str, Any]:
    """Keep only scalar (str/int/float) values; blank out anything else with ``None``."""
    return {k: (v if isinstance(v, (str, int, float)) else None) for k, v in row.items()}


# -- HELPERS — async reports ---------------------------------------------------
async def _request_report(
    connection: AmazonSellingPartnerConnection,
    report_type: str,
    marketplace: str,
    start_date: dt.date | None,
    end_date: dt.date | None,
    options: dict[str, str] | None,
) -> str:
    """Create an async report and return its report id."""
    body: dict[str, Any] = {"reportType": report_type, "marketplaceIds": [marketplace]}
    if start_date:
        body["dataStartTime"] = start_date.strftime("%Y-%m-%dT00:00:00.000Z")
    if end_date:
        body["dataEndTime"] = end_date.strftime("%Y-%m-%dT23:59:59.999Z")
    if options:
        body["reportOptions"] = options

    response = await connection.client.post(f"/reports/{constants.REPORT_API_VERSION}/reports", json=body)
    response.raise_for_status()
    return response.json()["reportId"]


async def _wait_for_report(connection: AmazonSellingPartnerConnection, report_id: str) -> dict:
    """Poll a report until it reaches a terminal state; return its final payload.

    Each poll depends on the previous status, so this stays a sequential
    fixed-interval loop bounded by ``_REPORT_TIMEOUT``. A failed report may still
    attach a document carrying error details, so return the payload for the
    caller to inspect rather than raising here.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _REPORT_TIMEOUT
    while True:
        logger.info(f"Waiting for report {report_id}...")
        try:
            response = await connection.client.get(f"/reports/{constants.REPORT_API_VERSION}/reports/{report_id}")
            response.raise_for_status()
            payload = response.json()
            status = payload["processingStatus"]
        except httpx.HTTPError as exc:
            logger.debug(f"Transient error polling report {report_id}: {exc}")
            if loop.time() >= deadline:
                raise RuntimeError(f"Report {report_id} did not complete within {_REPORT_TIMEOUT:.0f}s") from exc
            await asyncio.sleep(_POLL_INTERVAL)
            continue

        if status == "DONE":
            return payload
        if status in ("CANCELLED", "FATAL"):
            if payload.get("reportDocumentId"):
                return payload
            raise RuntimeError(f"Report {report_id} failed with status {status}")

        if loop.time() >= deadline:
            raise RuntimeError(f"Report {report_id} did not complete within {_REPORT_TIMEOUT:.0f}s")
        await asyncio.sleep(_POLL_INTERVAL)


async def _report_document_url(connection: AmazonSellingPartnerConnection, document_id: str) -> str:
    response = await connection.client.get(f"/reports/{constants.REPORT_API_VERSION}/documents/{document_id}")
    response.raise_for_status()
    return response.json()["url"]


async def _download_gzip_json(url: str) -> dict:
    """Download and gzip-decompress a report document from its pre-signed URL."""
    async with httpx.AsyncClient(timeout=None) as client:
        response = await client.get(url)
        response.raise_for_status()
    return json.loads(gzip.decompress(response.content))


async def _get_report(
    connection: AmazonSellingPartnerConnection,
    report_type: str,
    marketplace: str,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
    options: dict[str, str] | None = None,
) -> dict | None:
    """Request, wait for, and download an async report.

    Returns the parsed document, or ``None`` when the report failed only because
    the data for the requested range is not yet available.
    """
    report_id = await _request_report(connection, report_type, marketplace, start_date, end_date, options)
    logger.info(f"Report id: {report_id} ({report_type})")

    payload = await _wait_for_report(connection, report_id)
    status = payload["processingStatus"]

    document_id = payload.get("reportDocumentId")
    if not document_id:
        raise RuntimeError(f"Report {report_id} failed with status {status}")

    url = await _report_document_url(connection, document_id)
    document = await _download_gzip_json(url)

    if status != "DONE":
        details = document.get("errorDetails", "") if isinstance(document, dict) else ""
        if "not yet available" in details:
            logger.warning(f"Report {report_id}: {details}")
            return None
        raise RuntimeError(f"Report {report_id} failed: {details or status}")

    return document


# -- HELPERS — Data Kiosk ------------------------------------------------------
async def _wait_for_query(connection: AmazonSellingPartnerConnection, query_id: str) -> dict:
    """Poll a Data Kiosk query until it reaches a terminal state; return its payload."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _QUERY_TIMEOUT
    while True:
        logger.info(f"Waiting for query {query_id}...")
        try:
            response = await connection.client.get(f"/dataKiosk/{constants.DATAKIOSK_API_VERSION}/queries/{query_id}")
            response.raise_for_status()
            payload = response.json()
            status = payload["processingStatus"]
        except httpx.HTTPError as exc:
            logger.debug(f"Transient error polling query {query_id}: {exc}")
            if loop.time() >= deadline:
                raise RuntimeError(f"Query {query_id} did not complete within {_QUERY_TIMEOUT:.0f}s") from exc
            await asyncio.sleep(_POLL_INTERVAL)
            continue

        if status == "DONE":
            return payload
        if status in ("CANCELLED", "FATAL"):
            raise RuntimeError(f"Query {query_id} failed with status {status}")

        if loop.time() >= deadline:
            raise RuntimeError(f"Query {query_id} did not complete within {_QUERY_TIMEOUT:.0f}s")
        await asyncio.sleep(_POLL_INTERVAL)


async def _download_json(url: str) -> Any:
    """Download a Data Kiosk document (plain JSON) from its pre-signed URL."""
    async with httpx.AsyncClient(timeout=None) as client:
        response = await client.get(url)
        response.raise_for_status()
    return response.json()


def _extract_metrics(document: Any) -> list[dict]:
    """Pull the ``metrics`` record list out of a Data Kiosk result document.

    The list may sit at the top level or nested under the queried view object,
    so fall back to a shallow search of nested dicts.
    """
    if not isinstance(document, dict):
        return []
    if isinstance(document.get("metrics"), list):
        return document["metrics"]
    for value in document.values():
        if isinstance(value, dict):
            found = _extract_metrics(value)
            if found:
                return found
    return []


async def _run_query(connection: AmazonSellingPartnerConnection, query: str) -> list[dict]:
    """Create, wait for, and download a Data Kiosk query; return its metric records."""
    response = await connection.client.post(
        f"/dataKiosk/{constants.DATAKIOSK_API_VERSION}/queries", json={"query": query}
    )
    response.raise_for_status()
    query_id = response.json()["queryId"]
    logger.info(f"Query id: {query_id}")

    payload = await _wait_for_query(connection, query_id)
    document_id = payload.get("dataDocumentId")
    if not document_id:
        # DONE with no document => the query produced no rows.
        return []

    response = await connection.client.get(f"/dataKiosk/{constants.DATAKIOSK_API_VERSION}/documents/{document_id}")
    response.raise_for_status()
    document = await _download_json(response.json()["documentUrl"])
    return _extract_metrics(document)


# -- SOURCE --------------------------------------------------------------------
@il.source(
    resources={"connection": AmazonSellingPartnerConnection},
    tags=["E-Commerce"],
    icon="icon:amazon",
    # Vendor reports return nested camelCase money objects (inventory nests
    # cost two levels deep, e.g. sellableOnHandInventory.cost.amount); flatten and
    # snake-case (splitting digit groups, e.g. aged90Plus -> aged_90_plus) so the
    # columns land on the flat schema fields. A concrete depth (not None) is used
    # deliberately: the spec serializer drops None, degrading the child pod's
    # normalizer to no flattening. Data Kiosk assets already return flat
    # snake_case rows, so this is a no-op for them.
    normalizer=DataFrameNormalizer(flatten_max_level=3, snake_case_digits=True),
)
class AmazonSellingPartner(il.Source):
    """Amazon Selling Partner integration for vendor and seller reporting."""

    marketplace: str = il.FetchField(
        provider="connection.marketplaces",
        label_key="label",
        value_key="value",
        description="Amazon marketplace (scoped to the connection's region)",
        discriminator=True,
    )

    # --- Vendor Analytics — reports API ---

    @il.asset(
        schema=schemas.VendorTrafficStats,
        partitioning=il.TimePartitionConfig(column="start_date"),
        tags=["Report"],
    )
    async def vendor_traffic_stats(
        self, context: il.ExecutionContext, connection: AmazonSellingPartnerConnection
    ) -> list[_Record]:
        """Glance views per ASIN per day."""
        document = await _get_report(
            connection,
            report_type="GET_VENDOR_TRAFFIC_REPORT",
            marketplace=self.marketplace,
            start_date=context.partition_date,
            end_date=context.partition_date,
            options={"reportPeriod": "DAY"},
        )
        return document.get("trafficByAsin", []) if document else []

    @il.asset(
        schema=schemas.VendorSalesRetailManufacturingStats,
        partitioning=il.TimePartitionConfig(column="start_date"),
        tags=["Report"],
    )
    async def vendor_sales_retail_manufacturing_stats(
        self, context: il.ExecutionContext, connection: AmazonSellingPartnerConnection
    ) -> list[_Record]:
        """Retail sales per ASIN per day, manufacturing (distributor) view."""
        document = await _get_report(
            connection,
            report_type="GET_VENDOR_SALES_REPORT",
            marketplace=self.marketplace,
            start_date=context.partition_date,
            end_date=context.partition_date,
            options={"reportPeriod": "DAY", "sellingProgram": "RETAIL", "distributorView": "MANUFACTURING"},
        )
        return document.get("salesByAsin", []) if document else []

    @il.asset(
        schema=schemas.VendorSalesRetailSourcingStats,
        partitioning=il.TimePartitionConfig(column="start_date"),
        tags=["Report"],
    )
    async def vendor_sales_retail_sourcing_stats(
        self, context: il.ExecutionContext, connection: AmazonSellingPartnerConnection
    ) -> list[_Record]:
        """Retail sales per ASIN per day, sourcing (distributor) view."""
        document = await _get_report(
            connection,
            report_type="GET_VENDOR_SALES_REPORT",
            marketplace=self.marketplace,
            start_date=context.partition_date,
            end_date=context.partition_date,
            options={"reportPeriod": "DAY", "sellingProgram": "RETAIL", "distributorView": "SOURCING"},
        )
        return document.get("salesByAsin", []) if document else []

    @il.asset(
        schema=schemas.VendorSalesBusinessManufacturingStats,
        partitioning=il.TimePartitionConfig(column="start_date"),
        tags=["Report"],
    )
    async def vendor_sales_business_manufacturing_stats(
        self, context: il.ExecutionContext, connection: AmazonSellingPartnerConnection
    ) -> list[_Record]:
        """Business (B2B) sales per ASIN per day, manufacturing (distributor) view."""
        document = await _get_report(
            connection,
            report_type="GET_VENDOR_SALES_REPORT",
            marketplace=self.marketplace,
            start_date=context.partition_date,
            end_date=context.partition_date,
            options={"reportPeriod": "DAY", "sellingProgram": "BUSINESS", "distributorView": "MANUFACTURING"},
        )
        return document.get("salesByAsin", []) if document else []

    @il.asset(
        schema=schemas.VendorSalesBusinessSourcingStats,
        partitioning=il.TimePartitionConfig(column="start_date"),
        tags=["Report"],
    )
    async def vendor_sales_business_sourcing_stats(
        self, context: il.ExecutionContext, connection: AmazonSellingPartnerConnection
    ) -> list[_Record]:
        """Business (B2B) sales per ASIN per day, sourcing (distributor) view."""
        document = await _get_report(
            connection,
            report_type="GET_VENDOR_SALES_REPORT",
            marketplace=self.marketplace,
            start_date=context.partition_date,
            end_date=context.partition_date,
            options={"reportPeriod": "DAY", "sellingProgram": "BUSINESS", "distributorView": "SOURCING"},
        )
        return document.get("salesByAsin", []) if document else []

    @il.asset(
        schema=schemas.VendorInventoryRetailManufacturingStats,
        partitioning=il.TimePartitionConfig(column="start_date"),
        tags=["Report"],
    )
    async def vendor_inventory_retail_manufacturing_stats(
        self, context: il.ExecutionContext, connection: AmazonSellingPartnerConnection
    ) -> list[_Record]:
        """Retail inventory health per ASIN per day, manufacturing (distributor) view."""
        document = await _get_report(
            connection,
            report_type="GET_VENDOR_INVENTORY_REPORT",
            marketplace=self.marketplace,
            start_date=context.partition_date,
            end_date=context.partition_date,
            options={"reportPeriod": "DAY", "sellingProgram": "RETAIL", "distributorView": "MANUFACTURING"},
        )
        return document.get("inventoryByAsin", []) if document else []

    @il.asset(
        schema=schemas.VendorInventoryRetailSourcingStats,
        partitioning=il.TimePartitionConfig(column="start_date"),
        tags=["Report"],
    )
    async def vendor_inventory_retail_sourcing_stats(
        self, context: il.ExecutionContext, connection: AmazonSellingPartnerConnection
    ) -> list[_Record]:
        """Retail inventory health per ASIN per day, sourcing (distributor) view."""
        document = await _get_report(
            connection,
            report_type="GET_VENDOR_INVENTORY_REPORT",
            marketplace=self.marketplace,
            start_date=context.partition_date,
            end_date=context.partition_date,
            options={"reportPeriod": "DAY", "sellingProgram": "RETAIL", "distributorView": "SOURCING"},
        )
        return document.get("inventoryByAsin", []) if document else []

    @il.asset(
        schema=schemas.VendorNetPureProductMarginStats,
        partitioning=il.TimePartitionConfig(column="start_date"),
        tags=["Report"],
    )
    async def vendor_net_pure_product_margin_stats(
        self, context: il.ExecutionContext, connection: AmazonSellingPartnerConnection
    ) -> list[_Record]:
        """Net pure product margin per ASIN per day."""
        document = await _get_report(
            connection,
            report_type="GET_VENDOR_NET_PURE_PRODUCT_MARGIN_REPORT",
            marketplace=self.marketplace,
            start_date=context.partition_date,
            end_date=context.partition_date,
            options={"reportPeriod": "DAY"},
        )
        return document.get("netPureProductMarginByAsin", []) if document else []

    # TODO: disabled for now. Review whether this should be partitioned.
    # @il.asset(
    #     schema=schemas.VendorForecastingRetailStats,
    #     tags=["Report"],
    # )
    # async def vendor_forecasting_retail_stats(self, connection: AmazonSellingPartnerConnection) -> list[_Record]:
    #     """Latest forward-looking demand forecast per ASIN (retail, snapshot)."""
    #     document = await _get_report(
    #         connection,
    #         report_type="GET_VENDOR_FORECASTING_REPORT",
    #         marketplace=self.marketplace,
    #         options={"sellingProgram": "RETAIL"},
    #     )
    #     return document.get("forecastByAsin", []) if document else []

    # --- Vendor Analytics — Data Kiosk (GraphQL) ---

    @il.asset(
        schema=schemas.Products,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
    )
    async def products(
        self, context: il.ExecutionContext, connection: AmazonSellingPartnerConnection
    ) -> list[_Record]:
        """Product identifier lookup (ASIN, parent ASIN, EAN, UPC, ISBN)."""
        date = context.partition_date
        query = f"""{{
          {constants.DATAKIOSK_SCHEMA} {{
            sourcingView(aggregateBy: DAY, startDate: "{date:%Y-%m-%d}", endDate: "{date:%Y-%m-%d}") {{
              metrics {{
                groupByKey {{ asin parentAsin ean upc isbn13 }}
              }}
            }}
          }}
        }}"""
        metrics = await _run_query(connection, query)
        return [
            {
                **_scalars(
                    {
                        "asin": _nested(m, "groupByKey", "asin"),
                        "parent_asin": _nested(m, "groupByKey", "parentAsin"),
                        "ean": _nested(m, "groupByKey", "ean"),
                        "upc": _nested(m, "groupByKey", "upc"),
                        "isbn_13": _nested(m, "groupByKey", "isbn13"),
                    }
                ),
                "date": date,
            }
            for m in metrics
        ]

    @il.asset(
        schema=schemas.ProductFulfillmentStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def product_fulfillment_stats(
        self, context: il.ExecutionContext, connection: AmazonSellingPartnerConnection
    ) -> list[_Record]:
        """Vendor sourcing and fulfillment metrics per ASIN per day."""
        date = context.partition_date
        query = f"""{{
          {constants.DATAKIOSK_SCHEMA} {{
            sourcingView(aggregateBy: DAY, startDate: "{date:%Y-%m-%d}", endDate: "{date:%Y-%m-%d}") {{
              metrics {{
                groupByKey {{ asin }}
                metrics {{
                  customerSatisfaction {{ customerReturns }}
                  sourcing {{
                    openPurchaseOrderQuantity
                    confirmedUnits
                    mostRecentSubmitted
                    netReceived {{ units value {{ amount currencyCode }} }}
                    overallVendorLeadTime
                    procurableProductOOS
                    receivedFillRate
                    vendorConfirmationRate
                  }}
                }}
              }}
            }}
          }}
        }}"""
        metrics = await _run_query(connection, query)
        rows: list[_Record] = []
        for m in metrics:
            metric = _nested(m, "metrics")
            sourcing = _nested(metric, "sourcing")
            row = _scalars(
                {
                    "asin": _nested(m, "groupByKey", "asin"),
                    "customer_returns": _nested(metric, "customerSatisfaction", "customerReturns"),
                    "open_purchase_order_quantity": _nested(sourcing, "openPurchaseOrderQuantity"),
                    "confirmed_units": _nested(sourcing, "confirmedUnits"),
                    "most_recent_submitted": _nested(sourcing, "mostRecentSubmitted"),
                    "net_received_units": _nested(sourcing, "netReceived", "units"),
                    "net_received_value_amount": _nested(sourcing, "netReceived", "value", "amount"),
                    "net_received_value_currency_code": _nested(sourcing, "netReceived", "value", "currencyCode"),
                    "overall_vendor_lead_time": _nested(sourcing, "overallVendorLeadTime"),
                    "procurable_product_oos": _nested(sourcing, "procurableProductOOS"),
                    "received_fill_rate": _nested(sourcing, "receivedFillRate"),
                    "vendor_confirmation_rate": _nested(sourcing, "vendorConfirmationRate"),
                }
            )
            row["date"] = date
            rows.append(row)
        return rows

    @il.asset(
        schema=schemas.GeoSalesStatsByCountry,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def geo_sales_stats_by_country(
        self, context: il.ExecutionContext, connection: AmazonSellingPartnerConnection
    ) -> list[_Record]:
        """Shipped sales and cost metrics per ASIN per ship-to country per day."""
        date = context.partition_date
        query = f"""{{
          {constants.DATAKIOSK_SCHEMA} {{
            sourcingView(aggregateBy: DAY, startDate: "{date:%Y-%m-%d}", endDate: "{date:%Y-%m-%d}") {{
              metrics {{
                groupByKey {{ asin shipToCountryCode }}
                metrics {{
                  shippedOrders {{ shippedUnitsWithRevenue {{ value {{ amount currencyCode }} units }} }}
                  costs {{
                    netPPM
                    shippedCogs {{ amount currencyCode }}
                    contraCogsPerUnit {{ amount currencyCode }}
                    contraCogs {{ amount currencyCode }}
                    averageSalesDiscount {{ amount currencyCode }}
                  }}
                }}
              }}
            }}
          }}
        }}"""
        metrics = await _run_query(connection, query)
        rows: list[_Record] = []
        for m in metrics:
            metric = _nested(m, "metrics")
            shipped = _nested(metric, "shippedOrders")
            costs = _nested(metric, "costs")
            row = _scalars(
                {
                    "asin": _nested(m, "groupByKey", "asin"),
                    "ship_to_country_code": _nested(m, "groupByKey", "shipToCountryCode"),
                    "shipped_units_with_revenue_value_amount": _nested(
                        shipped, "shippedUnitsWithRevenue", "value", "amount"
                    ),
                    "shipped_units_with_revenue_value_currency_code": _nested(
                        shipped, "shippedUnitsWithRevenue", "value", "currencyCode"
                    ),
                    "shipped_units_with_revenue_units": _nested(shipped, "shippedUnitsWithRevenue", "units"),
                    "net_ppm": _nested(costs, "netPPM"),
                    "shipped_cogs_amount": _nested(costs, "shippedCogs", "amount"),
                    "shipped_cogs_currency_code": _nested(costs, "shippedCogs", "currencyCode"),
                    "contra_cogs_per_unit_amount": _nested(costs, "contraCogsPerUnit", "amount"),
                    "contra_cogs_per_unit_currency_code": _nested(costs, "contraCogsPerUnit", "currencyCode"),
                    "contra_cogs_amount": _nested(costs, "contraCogs", "amount"),
                    "contra_cogs_currency_code": _nested(costs, "contraCogs", "currencyCode"),
                    "average_sales_discount_amount": _nested(costs, "averageSalesDiscount", "amount"),
                    "average_sales_discount_currency_code": _nested(costs, "averageSalesDiscount", "currencyCode"),
                }
            )
            row["date"] = date
            rows.append(row)
        return rows
