import asyncio
import datetime as dt
from typing import Any, Literal

import httpx2
import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer
from pydantic import Field

from interloper_assets.pinterest_ads import constants, schemas
from interloper_assets.pinterest_ads.connection import PinterestAdsConnection

_Record = dict[str, Any]

_POLL_INTERVAL = 10.0
_REPORT_TIMEOUT = 30 * 60
_REPORT_FAILED_STATUSES = frozenset({"DOES_NOT_EXIST", "EXPIRED", "FAILED", "CANCELLED"})


# -- NORMALIZERS ---------------------------------------------------------------
class PinterestEntityNormalizer(DataFrameNormalizer):
    """Flatten Pinterest entities and type their epoch instants.

    Entity responses nest small objects (``owner``, ``targeting_spec``,
    ``tracking_urls``, ``bid_options``, …); one level of flattening mirrors them
    as ``<parent>_<key>`` columns, and anything deeper lands JSON-encoded on
    the ``str`` columns. Pinterest serializes lifecycle and schedule times as
    seconds since the epoch, which the conformer would read as nanoseconds, so
    the listed columns are converted to UTC datetimes here.

    Attributes:
        epoch_columns: Normalized column names holding epoch timestamps; a
            column absent from the frame is skipped.
        epoch_unit: The unit of the epoch timestamps, ``"s"`` for Pinterest.
    """

    epoch_columns: list[str] = Field(default_factory=list)
    epoch_unit: Literal["s", "ms"] = "s"

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


# Report columns come back UPPER_SNAKE; the snake-caser splits "3SEC"/"15SEC" into "3_sec"/"15_sec".
_REPORT_NORMALIZER = DataFrameNormalizer(
    column_overrides={
        "TOTAL_VIDEO_3SEC_VIEWS": "total_video_3sec_views",
        "TOTAL_VIDEO_15SEC_UNIQUE_VIEWS": "total_video_15sec_unique_views",
        "VIDEO_3SEC_VIEWS_1": "video_3sec_views_1",
        "VIDEO_3SEC_VIEWS_2": "video_3sec_views_2",
        "VIDEO_15SEC_UNIQUE_VIEWS_1": "video_15sec_unique_views_1",
        "VIDEO_15SEC_UNIQUE_VIEWS_2": "video_15sec_unique_views_2",
    },
)

_ENTITY_NORMALIZER = PinterestEntityNormalizer(
    flatten_max_level=1,
    epoch_columns=["created_time", "updated_time", "start_time", "end_time"],
    epoch_unit="s",
)


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    icon="logos:pinterest",
    normalizer=_REPORT_NORMALIZER,
)
class PinterestAds(il.Source):
    """Pinterest Ads advertising platform integration."""

    connection: PinterestAdsConnection

    account_id: str = il.FetchField(
        provider="connection.accounts",
        label_key="name",
        value_key="id",
        description="Pinterest Ads account",
        discriminator=True,
    )

    # -- INTERNALS -------------------------------------------------------------
    async def _report(
        self,
        date: dt.date,
        *,
        level: str,
        columns: list[str],
        targeting_types: list[str] | None = None,
    ) -> list[_Record]:
        """Request a one-day async analytics report, wait for it, and return its rows.

        Shared by every report asset. The report is requested at ``DAY``
        granularity in JSON, so each row carries its ``DATE``; the file maps
        each entity id to that entity's rows, which are flattened into one list.

        Args:
            date: The report day, used as both start and end date.
            level: The reporting level (``PIN_PROMOTION``, ``CAMPAIGN``, …).
            columns: The report columns to request.
            targeting_types: The targeting breakdowns, for a ``*_TARGETING``
                level only; ``None`` sends none.

        Returns:
            The report rows, empty when the account had no delivery that day.

        Raises:
            RuntimeError: If the report fails, or is not ready in time.
        """
        path = f"/ad_accounts/{self.account_id}/reports"
        body: dict[str, Any] = {
            "start_date": date.isoformat(),
            "end_date": date.isoformat(),
            "granularity": "DAY",
            "level": level,
            "columns": columns,
            "report_format": "JSON",
        }
        if targeting_types is not None:
            body["targeting_types"] = targeting_types

        response = await self.connection.client.post(path, json=body)
        response.raise_for_status()
        token = response.json()["token"]

        # wait for the report
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _REPORT_TIMEOUT
        while True:
            response = await self.connection.client.get(path, params={"token": token})
            response.raise_for_status()
            status = response.json()
            if status["report_status"] == "FINISHED":
                break
            if status["report_status"] in _REPORT_FAILED_STATUSES:
                raise RuntimeError(f"Pinterest report {token} ended with status {status['report_status']}")
            if loop.time() >= deadline:
                raise RuntimeError(f"Pinterest report {token} was not ready within {_REPORT_TIMEOUT}s")
            await asyncio.sleep(_POLL_INTERVAL)

        if not status.get("url"):
            return []

        # the signed download URL rejects the API's bearer token, so fetch it without auth
        async with httpx2.AsyncClient(timeout=constants.DOWNLOAD_TIMEOUT) as client:
            download = await client.get(status["url"])
        download.raise_for_status()
        if not download.content:
            return []
        return [row for rows in download.json().values() for row in rows]

    async def _list(self, resource: str, date: dt.date) -> list[_Record]:
        """List every object of an ad-account resource, stamped with the snapshot day.

        Shared by the paginated entity assets (``ads``, ``ad_groups``,
        ``campaigns``). Follows the ``bookmark`` cursor; the endpoint's own
        ``entity_statuses`` default (``ACTIVE``, ``PAUSED``) applies.

        Args:
            resource: The resource under the ad account (``ads``, ``ad_groups``,
                ``campaigns``).
            date: The partition day stamped on each row as ``date``.

        Returns:
            Every object of the resource, each carrying ``date``.
        """
        paginator = il.JSONCursorPaginator(cursor_path="bookmark", cursor_param="bookmark")
        pages = self.connection.client.paginate(
            f"/ad_accounts/{self.account_id}/{resource}",
            paginator,
            params={"page_size": constants.PAGE_SIZE},
            data_selector="items",
        )
        return [{**row, "date": date} async for page in pages for row in page]

    # -- REPORTS ---------------------------------------------------------------
    @il.asset(schema=schemas.AdsStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def ads_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Daily ad performance: delivery, engagement, spend and conversion totals per ad."""
        return await self._report(
            context.partition_date,
            level="PIN_PROMOTION",
            columns=[*constants.AD_METRICS, "AD_ID"],
        )

    @il.asset(schema=schemas.CampaignsStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def campaigns_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Daily campaign performance: delivery, engagement, spend and conversion totals per campaign."""
        return await self._report(context.partition_date, level="CAMPAIGN", columns=constants.CAMPAIGN_METRICS)

    @il.asset(
        schema=schemas.AdsConversionsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def ads_conversions_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Daily ad performance with the full conversion breakdown by event, attribution, channel and device."""
        return await self._report(
            context.partition_date,
            level="PIN_PROMOTION",
            columns=constants.ADS_CONVERSIONS_METRICS,
        )

    @il.asset(
        schema=schemas.VideosStatsByTargeting,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def videos_stats_by_targeting(self, context: il.ExecutionContext) -> list[_Record]:
        """Daily video ad performance per ad, broken down by app type and placement."""
        return await self._report(
            context.partition_date,
            level="PIN_PROMOTION_TARGETING",
            columns=constants.VIDEOS_METRICS,
            targeting_types=constants.VIDEO_TARGETING_TYPES,
        )

    # -- ENTITIES --------------------------------------------------------------
    @il.asset(
        schema=schemas.AdAccounts,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def ad_accounts(self, context: il.ExecutionContext) -> list[_Record]:
        """The ad account with its owner, country, currency and permissions."""
        response = await self.connection.client.get(f"/ad_accounts/{self.account_id}")
        response.raise_for_status()
        return [{**response.json(), "date": context.partition_date}]

    @il.asset(
        schema=schemas.Campaigns,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def campaigns(self, context: il.ExecutionContext) -> list[_Record]:
        """All active and paused campaigns in the ad account with their attributes."""
        return await self._list("campaigns", context.partition_date)

    @il.asset(
        schema=schemas.AdGroups,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def ad_groups(self, context: il.ExecutionContext) -> list[_Record]:
        """All active and paused ad groups in the ad account with their attributes."""
        return await self._list("ad_groups", context.partition_date)

    @il.asset(
        schema=schemas.Ads,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def ads(self, context: il.ExecutionContext) -> list[_Record]:
        """All active and paused ads in the ad account with their attributes."""
        return await self._list("ads", context.partition_date)
