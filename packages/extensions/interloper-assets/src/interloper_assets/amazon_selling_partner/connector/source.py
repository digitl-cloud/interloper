from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.amazon_selling_partner import constants
from interloper_assets.amazon_selling_partner.connection import AmazonSellingPartnerConnection
from interloper_assets.amazon_selling_partner.connector import schemas

_Record = dict[str, Any]


# -- HELPERS: nested-dict access -----------------------------------------------
def _nested(value: Any, *keys: str) -> Any:
    """Walk ``keys`` through nested dicts, for the Data Kiosk assets flattening their metric records.

    Args:
        value: The object to walk, usually a metric record.
        *keys: The keys to follow, outermost first.

    Returns:
        The value at the end of the walk, or ``None`` on any missing hop.
    """
    for key in keys:
        value = value.get(key) if isinstance(value, dict) else None
    return value


def _scalars(row: dict[str, Any]) -> dict[str, Any]:
    """Keep only scalar values of a flattened Data Kiosk row, blanking anything else with ``None``.

    Args:
        row: A row the Data Kiosk assets built from a metric record.

    Returns:
        The row, each str, int or float value kept and every other value ``None``.
    """
    return {key: (value if isinstance(value, (str, int, float)) else None) for key, value in row.items()}


# -- SOURCE --------------------------------------------------------------------
@il.source(
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

    connection: AmazonSellingPartnerConnection

    marketplace: str = il.FetchField(
        provider="connection.marketplaces",
        label_key="label",
        value_key="value",
        description="Amazon marketplace (scoped to the connection's region)",
        discriminator=True,
    )

    # --- Vendor Analytics: reports API ----------------------------------------

    @il.asset(
        schema=schemas.VendorTrafficStats,
        partitioning=il.TimePartitionConfig(column="start_date"),
        tags=["Report"],
    )
    async def vendor_traffic_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Glance views per ASIN per day."""
        return await self.connection.api.report(
            key="trafficByAsin",
            start_date=context.partition_date,
            end_date=context.partition_date,
            reportType="GET_VENDOR_TRAFFIC_REPORT",
            marketplaceIds=[self.marketplace],
            reportOptions={"reportPeriod": "DAY"},
        )

    @il.asset(
        schema=schemas.VendorSalesRetailManufacturingStats,
        partitioning=il.TimePartitionConfig(column="start_date"),
        tags=["Report"],
    )
    async def vendor_sales_retail_manufacturing_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Retail sales per ASIN per day, manufacturing (distributor) view."""
        return await self.connection.api.report(
            key="salesByAsin",
            start_date=context.partition_date,
            end_date=context.partition_date,
            reportType="GET_VENDOR_SALES_REPORT",
            marketplaceIds=[self.marketplace],
            reportOptions={"reportPeriod": "DAY", "sellingProgram": "RETAIL", "distributorView": "MANUFACTURING"},
        )

    @il.asset(
        schema=schemas.VendorSalesRetailSourcingStats,
        partitioning=il.TimePartitionConfig(column="start_date"),
        tags=["Report"],
    )
    async def vendor_sales_retail_sourcing_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Retail sales per ASIN per day, sourcing (distributor) view."""
        return await self.connection.api.report(
            key="salesByAsin",
            start_date=context.partition_date,
            end_date=context.partition_date,
            reportType="GET_VENDOR_SALES_REPORT",
            marketplaceIds=[self.marketplace],
            reportOptions={"reportPeriod": "DAY", "sellingProgram": "RETAIL", "distributorView": "SOURCING"},
        )

    @il.asset(
        schema=schemas.VendorSalesBusinessManufacturingStats,
        partitioning=il.TimePartitionConfig(column="start_date"),
        tags=["Report"],
    )
    async def vendor_sales_business_manufacturing_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Business (B2B) sales per ASIN per day, manufacturing (distributor) view."""
        return await self.connection.api.report(
            key="salesByAsin",
            start_date=context.partition_date,
            end_date=context.partition_date,
            reportType="GET_VENDOR_SALES_REPORT",
            marketplaceIds=[self.marketplace],
            reportOptions={"reportPeriod": "DAY", "sellingProgram": "BUSINESS", "distributorView": "MANUFACTURING"},
        )

    @il.asset(
        schema=schemas.VendorSalesBusinessSourcingStats,
        partitioning=il.TimePartitionConfig(column="start_date"),
        tags=["Report"],
    )
    async def vendor_sales_business_sourcing_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Business (B2B) sales per ASIN per day, sourcing (distributor) view."""
        return await self.connection.api.report(
            key="salesByAsin",
            start_date=context.partition_date,
            end_date=context.partition_date,
            reportType="GET_VENDOR_SALES_REPORT",
            marketplaceIds=[self.marketplace],
            reportOptions={"reportPeriod": "DAY", "sellingProgram": "BUSINESS", "distributorView": "SOURCING"},
        )

    @il.asset(
        schema=schemas.VendorInventoryRetailManufacturingStats,
        partitioning=il.TimePartitionConfig(column="start_date"),
        tags=["Report"],
    )
    async def vendor_inventory_retail_manufacturing_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Retail inventory health per ASIN per day, manufacturing (distributor) view."""
        return await self.connection.api.report(
            key="inventoryByAsin",
            start_date=context.partition_date,
            end_date=context.partition_date,
            reportType="GET_VENDOR_INVENTORY_REPORT",
            marketplaceIds=[self.marketplace],
            reportOptions={"reportPeriod": "DAY", "sellingProgram": "RETAIL", "distributorView": "MANUFACTURING"},
        )

    @il.asset(
        schema=schemas.VendorInventoryRetailSourcingStats,
        partitioning=il.TimePartitionConfig(column="start_date"),
        tags=["Report"],
    )
    async def vendor_inventory_retail_sourcing_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Retail inventory health per ASIN per day, sourcing (distributor) view."""
        return await self.connection.api.report(
            key="inventoryByAsin",
            start_date=context.partition_date,
            end_date=context.partition_date,
            reportType="GET_VENDOR_INVENTORY_REPORT",
            marketplaceIds=[self.marketplace],
            reportOptions={"reportPeriod": "DAY", "sellingProgram": "RETAIL", "distributorView": "SOURCING"},
        )

    @il.asset(
        schema=schemas.VendorNetPureProductMarginStats,
        partitioning=il.TimePartitionConfig(column="start_date"),
        tags=["Report"],
    )
    async def vendor_net_pure_product_margin_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Net pure product margin per ASIN per day."""
        return await self.connection.api.report(
            key="netPureProductMarginByAsin",
            start_date=context.partition_date,
            end_date=context.partition_date,
            reportType="GET_VENDOR_NET_PURE_PRODUCT_MARGIN_REPORT",
            marketplaceIds=[self.marketplace],
            reportOptions={"reportPeriod": "DAY"},
        )

    @il.asset(
        schema=schemas.VendorForecastingRetailStats,
        partitioning=il.TimePartitionConfig(column="forecast_generation_date"),
        tags=["Report"],
    )
    async def vendor_forecasting_retail_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Forward-looking demand forecast per ASIN (retail), one partition per forecast generation.

        Amazon regenerates the forecast weekly and the report only ever returns the
        latest generation, so the rows are kept on the run whose partition is their
        ``forecastGenerationDate`` and every other run writes nothing. Past
        generations cannot be backfilled from the API.
        """
        records = await self.connection.api.report(
            key="forecastByAsin",
            reportType="GET_VENDOR_FORECASTING_REPORT",
            marketplaceIds=[self.marketplace],
            reportOptions={"sellingProgram": "RETAIL"},
        )
        generated_on = context.partition_date.isoformat()
        return [record for record in records if record.get("forecastGenerationDate") == generated_on]

    # --- Vendor Analytics: Data Kiosk (GraphQL) -------------------------------

    def _marketplace_metrics(self, views: list[_Record]) -> list[_Record]:
        """Keep the metric records of the source's marketplace from a ``sourcingView`` query's records.

        The view takes no marketplace argument and answers one record per
        marketplace the vendor sells in, so each query selects the record's
        ``marketplaceId`` and only the source's own are kept, as the report
        assets name it in their ``marketplaceIds``. Used by ``products``,
        ``product_fulfillment_stats`` and ``geo_sales_stats_by_country``.

        Args:
            views: The query's records, one view per marketplace, each with its ``marketplaceId`` and ``metrics``.

        Returns:
            The ``metrics`` of every view of the source's marketplace, in the order Amazon wrote them.
        """
        return [
            metric
            for view in views
            if view.get("marketplaceId") == self.marketplace
            for metric in view.get("metrics") or []
        ]

    @il.asset(
        schema=schemas.Products,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
    )
    async def products(self, context: il.ExecutionContext) -> list[_Record]:
        """Product identifier lookup (ASIN, parent ASIN, EAN, UPC, ISBN)."""
        date = context.partition_date
        query = f"""{{
          {constants.DATAKIOSK_SCHEMA} {{
            sourcingView(aggregateBy: DAY, startDate: "{date:%Y-%m-%d}", endDate: "{date:%Y-%m-%d}") {{
              marketplaceId
              metrics {{
                groupByKey {{ asin parentAsin ean upc isbn13 }}
              }}
            }}
          }}
        }}"""
        views = await self.connection.api.query(query=query)
        metrics = self._marketplace_metrics(views)
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
    async def product_fulfillment_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Vendor sourcing and fulfillment metrics per ASIN per day."""
        date = context.partition_date
        query = f"""{{
          {constants.DATAKIOSK_SCHEMA} {{
            sourcingView(aggregateBy: DAY, startDate: "{date:%Y-%m-%d}", endDate: "{date:%Y-%m-%d}") {{
              marketplaceId
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
        views = await self.connection.api.query(query=query)
        metrics = self._marketplace_metrics(views)
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
    async def geo_sales_stats_by_country(self, context: il.ExecutionContext) -> list[_Record]:
        """Shipped sales and cost metrics per ASIN per ship-to country per day."""
        date = context.partition_date
        query = f"""{{
          {constants.DATAKIOSK_SCHEMA} {{
            sourcingView(aggregateBy: DAY, startDate: "{date:%Y-%m-%d}", endDate: "{date:%Y-%m-%d}") {{
              marketplaceId
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
        views = await self.connection.api.query(query=query)
        metrics = self._marketplace_metrics(views)
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
