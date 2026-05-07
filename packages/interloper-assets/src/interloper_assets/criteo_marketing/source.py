import logging

import interloper as il
import pandas as pd

from interloper_assets.criteo_marketing.connection import CriteoMarketingConnection
from interloper_assets.criteo_marketing.schemas import AdsStatistics, CampaignsStatistics
from interloper_assets.fake import fake_data

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": CriteoMarketingConnection},
    tags=["Advertising"],
    icon="icon:criteo",
)
class CriteoMarketing(il.Source):
    """Criteo Marketing advertising platform integration."""

    @il.asset(
        schema=AdsStatistics,
        partitioning=il.TimePartitionConfig(column="day"),
        tags=["Report"],
    )
    def ads(self, context: il.ExecutionContext, connection: CriteoMarketingConnection) -> pd.DataFrame:
        """Ad-level performance statistics with daily breakdowns."""
        return fake_data(AdsStatistics, partition_column="day", partition_date=context.partition_date)

    @il.asset(
        schema=CampaignsStatistics,
        partitioning=il.TimePartitionConfig(column="day"),
        tags=["Report"],
    )
    def campaigns(self, context: il.ExecutionContext, connection: CriteoMarketingConnection) -> pd.DataFrame:
        """Campaign-level performance statistics with daily breakdowns."""
        return fake_data(CampaignsStatistics, partition_column="day", partition_date=context.partition_date)
