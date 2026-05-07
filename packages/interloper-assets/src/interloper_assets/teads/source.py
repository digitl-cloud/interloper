import logging

import interloper as il
import pandas as pd

from interloper_assets.fake import fake_data
from interloper_assets.teads.connection import TeadsConnection
from interloper_assets.teads.schemas import Campaigns

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": TeadsConnection},
    tags=["Advertising"],
    icon="icon:teads",
)
class Teads(il.Source):
    """Teads advertising platform integration."""

    @il.asset(
        schema=Campaigns,
        partitioning=il.TimePartitionConfig(column="day"),
        tags=["Report"],
    )
    def campaigns(self, context: il.ExecutionContext, connection: TeadsConnection) -> pd.DataFrame:
        """Campaign performance report with delivery, click, and video completion metrics."""
        return fake_data(Campaigns, partition_column="day", partition_date=context.partition_date)
