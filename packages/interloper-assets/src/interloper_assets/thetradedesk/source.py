import logging

import interloper as il
import pandas as pd

from interloper_assets.fake import fake_data
from interloper_assets.thetradedesk.connection import TheTradeDeskConnection
from interloper_assets.thetradedesk.schemas import (
    AdGroupsStats,
    CampaignsStats,
)

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": TheTradeDeskConnection},
    tags=["Advertising"],
    icon="icon:thetradedesk",
)
class TheTradeDesk(il.Source):
    """The Trade Desk programmatic advertising platform integration."""

    report_template_id: str = il.InputField(
        description="Report template ID for the MyReports schedule",
    )

    @il.asset(
        schema=CampaignsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def campaigns_stats(self, context: il.ExecutionContext, connection: TheTradeDeskConnection) -> pd.DataFrame:
        """Campaign performance metrics including impressions, clicks, conversions, and costs."""
        return fake_data(CampaignsStats, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=AdGroupsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def ad_groups_stats(self, context: il.ExecutionContext, connection: TheTradeDeskConnection) -> pd.DataFrame:
        """Ad group performance metrics including impressions, clicks, conversions, and costs."""
        return fake_data(AdGroupsStats, partition_column="date", partition_date=context.partition_date)
