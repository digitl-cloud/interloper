import datetime as dt
import json
import logging
from typing import Any

import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.tiktok_ads import constants
from interloper_assets.tiktok_ads.connection import TiktokAdsConnection
from interloper_assets.tiktok_ads.schemas import (
    Ads,
    AdsStats,
    AdsStatsByAgeGender,
    AdsStatsByCountry,
    AdsStatsByPlatform,
    Advertisers,
    Campaigns,
    VideosStatsByPlatform,
)

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# NORMALIZERS
# ------------------------------------------------------------------
class TiktokStatsNormalizer(DataFrameNormalizer):
    """Flatten TikTok integrated-report rows into a flat frame.

    Each row nests its requested dimensions under ``dimensions`` and its metrics
    under ``metrics``; merge both into a single flat record (dropping the parent
    prefix) before the base ``DataFrameNormalizer`` runs. ``stat_time_day`` comes
    back as a midnight datetime string (``2026-01-01 00:00:00``) — coerce it to a
    plain date so it lands on the ``date``-typed schema column.
    """

    def normalize(self, data: Any) -> pd.DataFrame:
        records = data.to_dict("records") if isinstance(data, pd.DataFrame) else data
        flat: list[dict[str, Any]] = []
        for row in records:
            merged = {}
            merged.update(row.get("dimensions") or {})
            merged.update(row.get("metrics") or {})
            for key, value in row.items():
                if key not in ("dimensions", "metrics"):
                    merged[key] = value
            flat.append(merged)

        df = super().normalize(flat)
        if "stat_time_day" in df.columns:
            df["stat_time_day"] = pd.to_datetime(df["stat_time_day"], errors="coerce").dt.date
        return df


# Entity records carry list/dict fields (``ad_texts``, ``image_ids``,
# ``special_industries``, …) that the schemas type as strings; the conformer
# JSON-encodes those nested values when casting to the declared ``str`` type.
_ENTITY_NORMALIZER = DataFrameNormalizer(drop_na_columns=True)


# ------------------------------------------------------------------
# HELPERS — HTTP / pagination
# ------------------------------------------------------------------
def _paginate(connection: TiktokAdsConnection, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    """GET a paginated TikTok endpoint, following ``data.page_info.total_page``."""
    items: list[dict[str, Any]] = []
    page, total_page = 1, 1
    while page <= total_page:
        response = connection.client.get(
            path,
            params={**params, "page": page, "page_size": constants.PAGE_SIZE},
        )
        response.raise_for_status()
        body = response.json()
        if body.get("code") != 0:
            raise RuntimeError(f"TikTok API error {body.get('code')}: {body.get('message')}")
        data = body["data"]
        items.extend(data["list"])
        total_page = data["page_info"]["total_page"]
        page += 1
    return items


def _request_report(
    connection: TiktokAdsConnection,
    advertiser_id: str,
    date: dt.date,
    *,
    report_type: str,
    dimensions: list[str],
    metrics: list[str],
) -> list[dict[str, Any]]:
    """Request an AUCTION_AD integrated report for *date* and return its rows."""
    return _paginate(
        connection,
        "/report/integrated/get/",
        {
            "advertiser_id": advertiser_id,
            "service_type": "AUCTION",
            "report_type": report_type,
            "data_level": "AUCTION_AD",
            "dimensions": json.dumps(dimensions),
            "metrics": json.dumps(metrics),
            "start_date": date.isoformat(),
            "end_date": date.isoformat(),
        },
    )


def _with_date(rows: list[dict[str, Any]], date: dt.date) -> list[dict[str, Any]]:
    """Stamp the partition date onto each stats row (the normalizer reshapes the rest)."""
    return [{**row, "date": date} for row in rows]


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------
@il.source(
    resources={"connection": TiktokAdsConnection},
    tags=["Advertising"],
    icon="logos:tiktok-icon",
    normalizer=TiktokStatsNormalizer(),
)
class TiktokAds(il.Source):
    """TikTok Ads advertising platform integration."""

    advertiser_id: str = il.FetchField(
        provider="connection.advertisers",
        label_key="name",
        value_key="advertiser_id",
        description="TikTok Ads advertiser account",
    )

    # --- Time-series reports (TiktokStatsNormalizer from the source) ---

    @il.asset(schema=AdsStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def ads_stats(self, context: il.ExecutionContext, connection: TiktokAdsConnection) -> list[dict[str, Any]]:
        """Ad-level performance with basic metrics including spend, clicks, and conversions."""
        rows = _request_report(
            connection,
            self.advertiser_id,
            context.partition_date,
            report_type="BASIC",
            dimensions=["ad_id", "stat_time_day"],
            metrics=constants.BASIC_METRICS,
        )
        return _with_date(rows, context.partition_date)

    @il.asset(schema=AdsStatsByCountry, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def ads_stats_by_country(
        self, context: il.ExecutionContext, connection: TiktokAdsConnection
    ) -> list[dict[str, Any]]:
        """Ad performance segmented by country."""
        rows = _request_report(
            connection,
            self.advertiser_id,
            context.partition_date,
            report_type="AUDIENCE",
            dimensions=["ad_id", "country_code", "stat_time_day"],
            metrics=constants.AUDIENCE_METRICS,
        )
        return _with_date(rows, context.partition_date)

    @il.asset(schema=AdsStatsByAgeGender, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def ads_stats_by_age_gender(
        self, context: il.ExecutionContext, connection: TiktokAdsConnection
    ) -> list[dict[str, Any]]:
        """Ad performance segmented by age and gender demographics."""
        rows = _request_report(
            connection,
            self.advertiser_id,
            context.partition_date,
            report_type="AUDIENCE",
            dimensions=["ad_id", "age", "gender", "stat_time_day"],
            metrics=constants.AUDIENCE_METRICS,
        )
        return _with_date(rows, context.partition_date)

    @il.asset(schema=AdsStatsByPlatform, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def ads_stats_by_platform(
        self, context: il.ExecutionContext, connection: TiktokAdsConnection
    ) -> list[dict[str, Any]]:
        """Ad performance segmented by platform."""
        rows = _request_report(
            connection,
            self.advertiser_id,
            context.partition_date,
            report_type="AUDIENCE",
            dimensions=["ad_id", "platform", "stat_time_day"],
            metrics=constants.AUDIENCE_METRICS,
        )
        return _with_date(rows, context.partition_date)

    @il.asset(schema=VideosStatsByPlatform, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def videos_stats_by_platform(
        self, context: il.ExecutionContext, connection: TiktokAdsConnection
    ) -> list[dict[str, Any]]:
        """Video ad performance segmented by platform."""
        rows = _request_report(
            connection,
            self.advertiser_id,
            context.partition_date,
            report_type="AUDIENCE",
            dimensions=["ad_id", "platform", "stat_time_day"],
            metrics=constants.VIDEO_METRICS,
        )
        return _with_date(rows, context.partition_date)

    # --- Entity assets (_ENTITY_NORMALIZER) ---

    @il.asset(schema=Ads, tags=["Entity"], normalizer=_ENTITY_NORMALIZER)
    def ads(self, connection: TiktokAdsConnection) -> list[dict[str, Any]]:
        """All ads in the advertiser account with their attributes."""
        return _paginate(connection, "/ad/get/", {"advertiser_id": self.advertiser_id})

    @il.asset(schema=Campaigns, tags=["Entity"], normalizer=_ENTITY_NORMALIZER)
    def campaigns(self, connection: TiktokAdsConnection) -> list[dict[str, Any]]:
        """All campaigns in the advertiser account with their attributes."""
        return _paginate(connection, "/campaign/get/", {"advertiser_id": self.advertiser_id})

    @il.asset(schema=Advertisers, tags=["Entity"], normalizer=_ENTITY_NORMALIZER)
    def advertisers(self, connection: TiktokAdsConnection) -> list[dict[str, Any]]:
        """The advertiser account with its attributes."""
        response = connection.client.get(
            "/advertiser/info/",
            params={
                "advertiser_ids": json.dumps([self.advertiser_id]),
                "fields": json.dumps(constants.ADVERTISER_FIELDS),
            },
        )
        response.raise_for_status()
        body = response.json()
        if body.get("code") != 0:
            raise RuntimeError(f"TikTok API error {body.get('code')}: {body.get('message')}")
        return body["data"]["list"]
