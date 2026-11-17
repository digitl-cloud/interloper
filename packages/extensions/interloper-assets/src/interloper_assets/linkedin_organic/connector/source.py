from typing import Any, Literal

import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer
from pydantic import Field

from interloper_assets.linkedin_organic.connection import LinkedinOrganicConnection
from interloper_assets.linkedin_organic.connector.schemas import (
    FollowersStats,
    Industries,
    JobFunctions,
    PageStats,
    Seniorities,
    ShareStats,
)


# -- NORMALIZER ----------------------------------------------------------------
class LinkedinOrganicNormalizer(DataFrameNormalizer):
    """Flatten LinkedIn organic statistics and type their epoch instants.

    LinkedIn serializes ``timeRange.start`` / ``timeRange.end`` as milliseconds
    since the epoch. The conformer would read a bare integer as nanoseconds, so
    the listed columns are converted to UTC datetimes here.

    Attributes:
        epoch_columns: Normalized column names holding epoch timestamps; a
            column absent from the frame is skipped.
        epoch_unit: The unit of the epoch timestamps, ``"ms"`` for LinkedIn.
    """

    epoch_columns: list[str] = Field(default_factory=list)
    epoch_unit: Literal["s", "ms"] = "ms"

    def normalize(self, data: Any) -> pd.DataFrame:
        """Normalize the rows, then convert the epoch columns to UTC instants.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame.
        """
        df = super().normalize(data)
        for column in self.epoch_columns:
            if column in df.columns:
                df[column] = pd.to_datetime(df[column], unit=self.epoch_unit, utc=True)
        return df


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Social Media"],
    normalizer=LinkedinOrganicNormalizer(
        flatten_max_level=3,
        epoch_columns=["time_range_start", "time_range_end"],
    ),
    icon="devicon:linkedin",
    maturity="beta",
)
class LinkedinOrganic(il.Source):
    """LinkedIn Organization page organic analytics integration."""

    connection: LinkedinOrganicConnection

    organization_id: str = il.FetchField(
        provider="connection.organizations",
        label_key="name",
        value_key="id",
        description="LinkedIn Organization page ID",
        discriminator=True,
    )

    @property
    def _organization_urn(self) -> str:
        """The organization's URN, which every statistics finder is about.

        Returns:
            The URN, e.g. ``urn:li:organization:123``.
        """
        return f"urn:li:organization:{self.organization_id}"

    # -- STATISTICS ------------------------------------------------------------
    @il.asset(schema=FollowersStats, partitioning=il.SnapshotPartitionConfig(column="date"), tags=["Report"])
    async def followers_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Lifetime follower counts of the organization page by function, geography, industry, seniority and more.

        Each partition holds the counts as they stood when it was loaded, not the followers gained that day.
        """
        rows = await self.connection.api.rows(
            "/organizationalEntityFollowerStatistics",
            q="organizationalEntity",
            organizationalEntity=self._organization_urn,
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(schema=PageStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def page_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily views of the organization page per tab and device, and custom button clicks."""
        rows = await self.connection.api.report(
            "/organizationPageStatistics",
            context.partition_date,
            q="organization",
            organization=self._organization_urn,
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(schema=ShareStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def share_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily organic impressions, clicks, likes, comments, shares and engagement over all the page's posts."""
        rows = await self.connection.api.report(
            "/organizationalEntityShareStatistics",
            context.partition_date,
            q="organizationalEntity",
            organizationalEntity=self._organization_urn,
        )
        return [{**row, "date": context.partition_date} for row in rows]

    # -- TAXONOMIES ------------------------------------------------------------
    @il.asset(schema=Industries, partitioning=il.SnapshotPartitionConfig(column="date"), tags=["Entity"])
    async def industries(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """LinkedIn's industry taxonomy, to resolve the industry URNs of the follower statistics."""
        rows = await self.connection.api.rows("/industryTaxonomyVersions/DEFAULT/industries")
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(schema=JobFunctions, partitioning=il.SnapshotPartitionConfig(column="date"), tags=["Entity"])
    async def job_functions(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """LinkedIn's job function taxonomy, to resolve the function URNs of the follower statistics."""
        rows = await self.connection.api.rows("/functions")
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(schema=Seniorities, partitioning=il.SnapshotPartitionConfig(column="date"), tags=["Entity"])
    async def seniorities(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """LinkedIn's seniority taxonomy, to resolve the seniority URNs of the follower statistics."""
        rows = await self.connection.api.rows("/seniorities")
        return [{**row, "date": context.partition_date} for row in rows]
