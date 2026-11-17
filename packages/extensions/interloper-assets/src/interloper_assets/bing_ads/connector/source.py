from typing import Any

import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.bing_ads import constants
from interloper_assets.bing_ads.connection import BingAdsConnection
from interloper_assets.bing_ads.connector.schemas import Accounts, AdGroups, Ads, AdsStats, Campaigns


@il.source(
    tags=["Advertising"],
    icon="icon:bing",
    normalizer=DataFrameNormalizer(snake_case_digits=True),
)
class BingAds(il.Source):
    """Bing Ads (Microsoft Advertising) platform integration.

    The ad report comes from the Reporting service. The account comes from
    Customer Management, without its business address and tax registration,
    and the campaigns, ad groups and ads from the Bulk API, whose records
    name each entity's parent (an ad group's campaign, an ad's ad group). The
    Bulk columns are those of the bingads SDK version the connector pins
    (13.0.29), so a column Bing added since is not collected yet.
    """

    connection: BingAdsConnection

    account_id: str = il.FetchField(
        label="Account ID",
        description="Bing Ads account",
        provider="connection.accounts",
        label_key="name",
        value_key="account_id",
        discriminator=True,
    )

    @il.asset(
        schema=AdsStats,
        partitioning=il.TimePartitionConfig(column="time_period"),
        tags=["Report"],
    )
    async def ads_stats(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad performance report with impressions, clicks, conversions, and revenue metrics."""
        return await self.connection.api.report(
            self.account_id,
            context.partition_date,
            "AdPerformanceReportRequest",
            scope="AccountThroughAdGroupReportScope",
            scope_fields={"Campaigns": None},
            columns=constants.AD_PERFORMANCE_FIELDS,
            percentages=constants.PERCENTAGE_COLUMNS,
            Aggregation="Daily",
            ReturnOnlyCompleteData=False,
            ReportName="Ad Performance Report",
        )

    @il.asset(schema=Accounts, partitioning=il.SnapshotPartitionConfig(column="date"), tags=["Entity"])
    async def accounts(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """The account with its name, currency, time zone, status and parent customer."""
        accounts = await self.connection.api.call("GetAccount", "Account", AccountId=int(self.account_id))
        if not accounts:
            raise RuntimeError(f"Bing Ads returned no account for {self.account_id}")
        fields = {name: value for name, value in accounts[0].items() if name not in constants.ACCOUNT_EXCLUDED_FIELDS}
        return [{**fields, "date": context.partition_date}]

    @il.asset(schema=Campaigns, partitioning=il.SnapshotPartitionConfig(column="date"), tags=["Entity"])
    async def campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Campaigns with their type, status, budget, bid strategy and time zone."""
        rows = await self.connection.api.bulk(
            self.account_id, data_scope=["EntityData"], download_entities=["Campaigns"]
        )
        return rows.assign(date=context.partition_date)

    @il.asset(schema=AdGroups, partitioning=il.SnapshotPartitionConfig(column="date"), tags=["Entity"])
    async def ad_groups(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad groups with their campaign, status, schedule and bids."""
        rows = await self.connection.api.bulk(
            self.account_id, data_scope=["EntityData"], download_entities=["AdGroups"]
        )
        return rows.assign(date=context.partition_date)

    @il.asset(schema=Ads, partitioning=il.SnapshotPartitionConfig(column="date"), tags=["Entity"])
    async def ads(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ads of every type with their ad group, status, texts and landing pages."""
        rows = await self.connection.api.bulk(self.account_id, data_scope=["EntityData"], download_entities=["Ads"])
        return rows.assign(date=context.partition_date)
