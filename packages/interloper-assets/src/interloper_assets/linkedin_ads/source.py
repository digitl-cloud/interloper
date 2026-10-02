import datetime as dt
from typing import Any, Literal
from urllib.parse import quote

import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer
from pydantic import Field

from interloper_assets.linkedin_ads import constants
from interloper_assets.linkedin_ads.connection import LinkedinAdsConnection
from interloper_assets.linkedin_ads.schemas import AdAccounts, AdsStats, CampaignGroups, Campaigns


# -- NORMALIZERS ---------------------------------------------------------------
class LinkedinAdsNormalizer(DataFrameNormalizer):
    """Flatten LinkedIn entities and type their epoch instants.

    LinkedIn serializes instants such as ``runSchedule.start`` as milliseconds
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


class LinkedinAdsStatsNormalizer(DataFrameNormalizer):
    """Type the ``dateRange`` of ad analytics rows as days.

    Each row carries its day as ``{"start": {"year", "month", "day"}, "end": {...}}``.
    Both bounds become ``dt.date`` values before flattening, so they land on
    ``date_range_start`` / ``date_range_end`` under the vendor's own names.
    """

    def normalize(self, data: Any) -> pd.DataFrame:
        """Convert the ``dateRange`` bounds, then run the standard normalization.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame.
        """
        records = data.to_dict("records") if isinstance(data, pd.DataFrame) else data
        rows = []
        for record in records:
            date_range = record.get("dateRange") or {}
            days = {bound: dt.date(**value) for bound, value in date_range.items() if isinstance(value, dict)}
            rows.append({**record, "dateRange": days} if days else record)
        return super().normalize(rows)


# -- HELPERS -------------------------------------------------------------------
def _with_date(rows: list[dict[str, Any]], date: dt.date) -> list[dict[str, Any]]:
    """Stamp the partition date onto entity rows, which LinkedIn returns undated.

    Used by ``ad_accounts``, ``campaign_groups`` and ``campaigns``.

    Args:
        rows: The entity rows as the API returned them.
        date: The partition day to stamp.

    Returns:
        The rows, each with a ``date`` key.
    """
    return [{**row, "date": date} for row in rows]


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    normalizer=LinkedinAdsNormalizer(
        flatten_max_level=1,
        epoch_columns=["run_schedule_start", "run_schedule_end"],
    ),
    icon="devicon:linkedin",
    maturity="beta",
)
class LinkedinAds(il.Source):
    """LinkedIn Ads advertising platform integration."""

    connection: LinkedinAdsConnection

    account_id: str = il.FetchField(
        provider="connection.accounts",
        label_key="name",
        value_key="id",
        description="LinkedIn Ads account",
        discriminator=True,
    )

    async def _search(self, resource: str, fields: list[str]) -> list[dict[str, Any]]:
        """Walk the account's cursor-paginated ``q=search`` finder for *resource*.

        The query string is written out raw because Rest.li 2.0 rejects
        percent-encoded projection commas, which rules out ``client.paginate``
        (its paginators re-encode the whole query).

        Args:
            resource: The account sub-resource to search, e.g. ``adCampaigns``.
            fields: The element fields to project.

        Returns:
            The elements of every page, in page order.
        """
        path = f"/adAccounts/{self.account_id}/{resource}"
        query = f"q=search&pageSize={constants.SEARCH_PAGE_SIZE}&fields={','.join(fields)}"
        elements: list[dict[str, Any]] = []
        page_token = None
        while True:
            url = f"{path}?{query}" if page_token is None else f"{path}?{query}&pageToken={quote(page_token, safe='')}"
            response = await self.connection.client.get(url)
            response.raise_for_status()
            body = response.json()
            elements.extend(body.get("elements", []))
            page_token = (body.get("metadata") or {}).get("nextPageToken")
            if not page_token:
                return elements

    @il.asset(
        schema=AdsStats,
        partitioning=il.TimePartitionConfig(column="date_range_start"),
        tags=["Report"],
        normalizer=LinkedinAdsStatsNormalizer(flatten_max_level=1),
    )
    async def ads_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily cost, delivery, engagement and conversions of each sponsored share per campaign."""
        date = context.partition_date
        day = f"(year:{date.year},month:{date.month},day:{date.day})"
        query = "&".join(
            [
                "q=statistics",
                f"pivots={constants.ANALYTICS_PIVOTS}",
                "timeGranularity=(value:DAILY)",
                f"dateRange=(start:{day},end:{day})",
                f"accounts=List(urn%3Ali%3AsponsoredAccount%3A{self.account_id})",
                f"fields=pivotValues,dateRange,{','.join(constants.ANALYTICS_FIELDS)}",
            ]
        )
        response = await self.connection.client.get(f"/adAnalytics?{query}")
        response.raise_for_status()
        return response.json().get("elements", [])

    @il.asset(schema=AdAccounts, partitioning=il.TimePartitionConfig(column="date"), tags=["Entity"])
    async def ad_accounts(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """The ad account with its name, currency and status."""
        response = await self.connection.client.get(
            f"/adAccounts/{self.account_id}?fields={','.join(constants.ACCOUNT_FIELDS)}"
        )
        response.raise_for_status()
        return _with_date([response.json()], context.partition_date)

    @il.asset(schema=CampaignGroups, partitioning=il.TimePartitionConfig(column="date"), tags=["Entity"])
    async def campaign_groups(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """All campaign groups of the ad account with their status and run schedule."""
        rows = await self._search("adCampaignGroups", constants.CAMPAIGN_GROUP_FIELDS)
        return _with_date(rows, context.partition_date)

    @il.asset(schema=Campaigns, partitioning=il.TimePartitionConfig(column="date"), tags=["Entity"])
    async def campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """All campaigns of the ad account with their type, objective, budget and run schedule."""
        rows = await self._search("adCampaigns", constants.CAMPAIGN_FIELDS)
        return _with_date(rows, context.partition_date)
