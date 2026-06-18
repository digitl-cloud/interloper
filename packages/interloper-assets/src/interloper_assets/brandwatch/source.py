
import interloper as il
import pandas as pd

from interloper_assets.brandwatch.connection import BrandwatchConnection
from interloper_assets.brandwatch.schemas import ChannelStats
from interloper_assets.fake import fake_data

# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": BrandwatchConnection},
    tags=["Social Media"],
    icon="fluent:connector-24-filled",
)
class Brandwatch(il.Source):
    """Brandwatch (Falcon.io) social media analytics integration."""

    @il.asset(
        schema=ChannelStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def channel_stats(self, context: il.ExecutionContext, connection: BrandwatchConnection) -> pd.DataFrame:
        """Social media channel insights with engagement metrics for the configured network."""
        return fake_data(ChannelStats, partition_column="date", partition_date=context.partition_date)
