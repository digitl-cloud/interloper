from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.usercentrics.connection import UsercentricsConnection
from interloper_assets.usercentrics.connector import schemas


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Privacy & Consent"],
    icon="fluent:connector-24-filled",
    normalizer=DataFrameNormalizer(replace_empty_strings=True),
    maturity="beta",
)
class Usercentrics(il.Source):
    """Usercentrics consent management analytics integration."""

    connection: UsercentricsConnection

    analytics_id: str = il.InputField(
        label="Analytics ID",
        description="Usercentrics analytics ID the data export is issued for",
        discriminator=True,
    )

    @il.asset(schema=schemas.ConsentsStats, partitioning=il.TimePartitionConfig(column="day"), tags=["Report"])
    async def consents_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily consent decisions per data processing service, settings configuration, country, OS and browser."""
        return await self.connection.api.export(self.analytics_id, "granular", context.partition_date)

    @il.asset(schema=schemas.InteractionsStats, partitioning=il.TimePartitionConfig(column="day"), tags=["Report"])
    async def interactions_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily consent-banner interactions per event type, settings configuration, host, country and device."""
        return await self.connection.api.export(self.analytics_id, "interaction", context.partition_date)
