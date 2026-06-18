import logging

import interloper as il
import pandas as pd

from interloper_assets.fake import fake_data
from interloper_assets.usercentrics.connection import UsercentricsConnection
from interloper_assets.usercentrics.schemas import GranularStats, InteractionStats

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": UsercentricsConnection},
    tags=["Privacy & Consent"],
    icon="fluent:connector-24-filled",
)
class Usercentrics(il.Source):
    """Usercentrics consent management analytics integration."""

    @il.asset(
        schema=GranularStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def granular_stats(self, context: il.ExecutionContext, connection: UsercentricsConnection) -> pd.DataFrame:
        """Granular consent analytics data with detailed per-event breakdown."""
        return fake_data(GranularStats, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=InteractionStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def interaction_stats(self, context: il.ExecutionContext, connection: UsercentricsConnection) -> pd.DataFrame:
        """Interaction-level consent analytics with user consent action data."""
        return fake_data(InteractionStats, partition_column="date", partition_date=context.partition_date)
