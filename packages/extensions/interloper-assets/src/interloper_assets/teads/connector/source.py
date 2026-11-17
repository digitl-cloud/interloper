from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.teads import constants
from interloper_assets.teads.connection import TeadsConnection
from interloper_assets.teads.connector import schemas


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    icon="icon:teads",
    normalizer=DataFrameNormalizer(replace_empty_strings=True),
    maturity="beta",
)
class Teads(il.Source):
    """Teads advertising platform integration."""

    connection: TeadsConnection

    advertiser_id: int = il.InputField(
        label="Advertiser ID",
        description="Teads advertiser the reports are filtered to",
        discriminator=True,
    )

    @il.asset(schema=schemas.CreativesStats, partitioning=il.TimePartitionConfig(column="day"), tags=["Report"])
    async def creatives_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily delivery, spend, click and video metrics per creative, campaign and line item."""
        return await self.connection.api.report(
            context.partition_date,
            timezone=constants.REPORT_TIMEZONE,
            filters={"advertisers": [self.advertiser_id]},
            dimensions=constants.STANDARD_DIMENSIONS,
            metrics=constants.STANDARD_METRICS,
        )
