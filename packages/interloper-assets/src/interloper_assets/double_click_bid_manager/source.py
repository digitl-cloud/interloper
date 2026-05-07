import logging

import interloper as il
import pandas as pd

from interloper_assets.double_click_bid_manager.connection import DoubleClickBidManagerConnection
from interloper_assets.double_click_bid_manager.schemas import Lineitems, LineitemsByCountry
from interloper_assets.fake import fake_data

logger = logging.getLogger(__name__)


@il.source(
    resources={
        "connection": DoubleClickBidManagerConnection,
    },
    tags=["Advertising"],
    icon="icon:dv360",
)
class DoubleClickBidManager(il.Source):
    """DoubleClick Bid Manager (DBM/DV360) reporting integration."""

    partner_id: str = il.InputField(description="DV360 partner ID for report filtering")
    advertiser_id: str = il.InputField(description="DV360 advertiser ID for report filtering (optional)")

    @il.asset(
        schema=Lineitems,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def lineitems(
        self,
        context: il.ExecutionContext,
        connection: DoubleClickBidManagerConnection,
    ) -> pd.DataFrame:
        """Line item performance report with full metrics and fee breakdown."""
        return fake_data(Lineitems, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=LineitemsByCountry,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def lineitems_by_country(
        self,
        context: il.ExecutionContext,
        connection: DoubleClickBidManagerConnection,
    ) -> pd.DataFrame:
        """Line item performance report segmented by country."""
        return fake_data(LineitemsByCountry, partition_column="date", partition_date=context.partition_date)
