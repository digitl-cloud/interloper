import datetime as dt
import logging
from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.criteo import constants
from interloper_assets.criteo.connection import CriteoConnection
from interloper_assets.criteo.schemas import AdsStats, CampaignsStats

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------
def _statistics_report(
    connection: CriteoConnection,
    advertiser_id: str,
    start_date: dt.date,
    end_date: dt.date,
    dimensions: list[str],
    metrics: list[str] = constants.STATISTICS_METRICS,
    currency: str = "EUR",
) -> list[dict]:
    """POST a Criteo statistics report and return its rows."""
    response = connection.client.post(
        f"/{constants.API_VERSION}/statistics/report",
        json={
            "advertiserIds": advertiser_id,
            "dimensions": dimensions,
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "currency": currency,
            "metrics": metrics,
            "format": "json",
        },
    )
    response.raise_for_status()
    return response.json()["Rows"]


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------
@il.source(
    resources={"connection": CriteoConnection},
    tags=["Advertising"],
    icon="icon:criteo",
    normalizer=DataFrameNormalizer(
        snake_case_digits=True,
        column_overrides={
            "OmnichannelRoasPc30d": "omni_channel_roas_pc_30d",
            "OmnichannelRevenuePc30d": "omni_channel_revenue_pc_30d",
            "OmnichannelSalesPc30d": "omni_channel_sales_pc_30d",
            "OmnichannelsalesClientAttribution": "omnichannel_sales_client_attribution",
        },
    ),
)
class Criteo(il.Source):
    """Criteo advertising platform integration."""

    advertiser_id: str = il.FetchField(
        provider="connection.advertisers",
        label_key="name",
        value_key="id",
        description="Criteo advertiser account",
    )

    @il.asset(
        schema=AdsStats,
        partitioning=il.TimePartitionConfig(column="day"),
        tags=["Report"],
    )
    def ads_stats(self, context: il.ExecutionContext, connection: CriteoConnection) -> list[dict[str, Any]]:
        """Ad-level performance statistics with daily breakdowns."""
        rows = _statistics_report(
            connection,
            advertiser_id=self.advertiser_id,
            start_date=context.partition_date,
            end_date=context.partition_date,
            dimensions=["Day", "AdId", "AdsetId", "CampaignId"],
        )
        return rows

    @il.asset(
        schema=CampaignsStats,
        partitioning=il.TimePartitionConfig(column="day"),
        tags=["Report"],
    )
    def campaigns_stats(self, context: il.ExecutionContext, connection: CriteoConnection) -> list[dict[str, Any]]:
        """Campaign-level performance statistics with daily breakdowns."""
        rows = _statistics_report(
            connection,
            advertiser_id=self.advertiser_id,
            start_date=context.partition_date,
            end_date=context.partition_date,
            dimensions=["Day", "CampaignId"],
        )
        return rows
