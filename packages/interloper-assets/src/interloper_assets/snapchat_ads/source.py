import datetime as dt
import logging
from typing import Any

import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.snapchat_ads import constants
from interloper_assets.snapchat_ads.connection import SnapchatAdsConnection
from interloper_assets.snapchat_ads.schemas import (
    AdAccount,
    AdAccounts,
    Ads,
    AdSquads,
    AdsStats,
    AdsStatsByCountry,
    Campaigns,
    CampaignsStats,
    VideosStatsByOs,
)

logger = logging.getLogger(__name__)

_RECORD = dict[str, Any]

# Stats fields requested for the ad/campaign performance reports.
_REPORT_METRICS = constants.CONVERSION_METRICS + constants.CORE_METRICS + constants.ADDITIONAL_METRICS


# ------------------------------------------------------------------
# NORMALIZERS
# ------------------------------------------------------------------
class SnapchatStatsNormalizer(DataFrameNormalizer):
    """Reshape Snapchat stats rows into a flat frame.

    Each row nests its metrics under a ``stats`` dict (flattened to ``stats_*``
    then de-prefixed) or, for dimensioned reports, carries a ``dimension_stats``
    list that is exploded into one row per dimension value. Column-name
    normalization is then applied by the base ``DataFrameNormalizer``.
    """

    def normalize(self, data: Any) -> pd.DataFrame:
        records = data.to_dict("records") if isinstance(data, pd.DataFrame) else data
        df = pd.json_normalize(records, sep="_")
        if df.empty:
            return df

        if "dimension_stats" in df.columns:
            df = df[df["dimension_stats"].map(lambda v: isinstance(v, list) and len(v) > 0)]
            df = df.explode("dimension_stats").reset_index(drop=True)
            expanded = df["dimension_stats"].apply(pd.Series)
            df = pd.concat([df.drop(columns=["dimension_stats"]), expanded], axis=1)
        else:
            df = df.rename(columns={col: col.replace("stats_", "") for col in df.columns})

        return super().normalize(df)


# Entity records are nested dicts; flatten and drop all-null columns.
_ENTITY_NORMALIZER = DataFrameNormalizer(flatten_max_level=3, drop_na_columns=True)


# ------------------------------------------------------------------
# HELPERS — HTTP / pagination
# ------------------------------------------------------------------
def _get_pages(connection: SnapchatAdsConnection, path: str, params: dict | None = None) -> list[_RECORD]:
    """GET a Snapchat endpoint and follow ``paging.next_link``, returning each page's JSON."""
    pages: list[_RECORD] = []
    response = connection.client.get(path, params=params)
    response.raise_for_status()
    page = response.json()
    pages.append(page)
    while page.get("paging", {}).get("next_link"):
        response = connection.client.get(page["paging"]["next_link"])
        response.raise_for_status()
        page = response.json()
        pages.append(page)
    return pages


def _request_report(
    connection: SnapchatAdsConnection,
    account_id: str,
    date: dt.date,
    metrics: list[str],
    breakdown: str = "ad",
    report_dimension: list[str] | None = None,
) -> list[_RECORD]:
    """Request a TOTAL-granularity stats report for *date* and return its breakdown rows."""
    params: dict[str, Any] = {
        "granularity": "TOTAL",
        "start_time": date.isoformat(),
        "end_time": (date + dt.timedelta(days=1)).isoformat(),
        "breakdown": breakdown,
        "fields": ",".join(metrics),
    }
    if report_dimension is not None:
        params["report_dimension"] = ",".join(report_dimension)

    rows: list[_RECORD] = []
    for page in _get_pages(connection, f"/{constants.API_VERSION}/adaccounts/{account_id}/stats", params):
        for total in page.get("total_stats", []):
            rows.extend(total["total_stat"]["breakdown_stats"][breakdown])
    return rows


def _entity_records(connection: SnapchatAdsConnection, path: str, list_key: str, item_key: str) -> list[_RECORD]:
    """Page an entity endpoint and unwrap ``page[list_key][i][item_key]`` records."""
    records: list[_RECORD] = []
    for page in _get_pages(connection, path):
        records.extend(item[item_key] for item in page.get(list_key, []))
    return records


def _with_date(rows: list[_RECORD], date: dt.date) -> list[_RECORD]:
    """Stamp the partition date onto each stats row (the normalizer reshapes the rest)."""
    return [{**row, "date": date} for row in rows]


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------
@il.source(
    resources={"connection": SnapchatAdsConnection},
    tags=["Advertising"],
    icon="mdi:snapchat",
    normalizer=SnapchatStatsNormalizer(),
)
class SnapchatAds(il.Source):
    """Snapchat Ads advertising platform integration."""

    account_id: str = il.FetchField(
        provider="connection.ad_accounts",
        label_key="name",
        value_key="id",
        description="Snapchat Ads ad account",
    )

    # --- Time-series reports (SnapchatStatsNormalizer from the source) ---

    @il.asset(schema=AdsStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def ads_stats(self, context: il.ExecutionContext, connection: SnapchatAdsConnection) -> list[_RECORD]:
        """Ad-level performance with core, additional, and conversion metrics."""
        rows = _request_report(connection, self.account_id, context.partition_date, _REPORT_METRICS, breakdown="ad")
        return _with_date(rows, context.partition_date)

    @il.asset(schema=CampaignsStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def campaigns_stats(self, context: il.ExecutionContext, connection: SnapchatAdsConnection) -> list[_RECORD]:
        """Campaign-level performance with core, additional, and conversion metrics."""
        rows = _request_report(
            connection, self.account_id, context.partition_date, _REPORT_METRICS, breakdown="campaign"
        )
        return _with_date(rows, context.partition_date)

    @il.asset(schema=AdsStatsByCountry, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def ads_stats_by_country(self, context: il.ExecutionContext, connection: SnapchatAdsConnection) -> list[_RECORD]:
        """Ad performance segmented by country with delivery and conversion metrics."""
        rows = _request_report(
            connection,
            self.account_id,
            context.partition_date,
            constants.CONVERSION_METRICS + constants.DELIVERY_METRICS,
            report_dimension=["country"],
        )
        return _with_date(rows, context.partition_date)

    @il.asset(schema=VideosStatsByOs, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def videos_stats_by_os(self, context: il.ExecutionContext, connection: SnapchatAdsConnection) -> list[_RECORD]:
        """Video ad performance segmented by operating system."""
        rows = _request_report(
            connection,
            self.account_id,
            context.partition_date,
            constants.VIDEOS_METRICS,
            report_dimension=["os"],
        )
        return _with_date(rows, context.partition_date)

    # --- Entity assets (flattening normalizer) ---

    @il.asset(schema=AdAccount, tags=["Entity"], normalizer=_ENTITY_NORMALIZER)
    def ad_account(self, connection: SnapchatAdsConnection) -> list[_RECORD]:
        """A single ad account with its attributes."""
        path = f"/{constants.API_VERSION}/adaccounts/{self.account_id}"
        return _entity_records(connection, path, "adaccounts", "adaccount")

    @il.asset(schema=AdAccounts, tags=["Entity"], normalizer=_ENTITY_NORMALIZER)
    def ad_accounts(self, connection: SnapchatAdsConnection) -> list[_RECORD]:
        """All ad accounts in the organization owning this account."""
        account_path = f"/{constants.API_VERSION}/adaccounts/{self.account_id}"
        accounts = _entity_records(connection, account_path, "adaccounts", "adaccount")
        organization_id = accounts[0]["organization_id"] if accounts else None
        if organization_id is None:
            return []
        path = f"/{constants.API_VERSION}/organizations/{organization_id}/adaccounts"
        return _entity_records(connection, path, "adaccounts", "adaccount")

    @il.asset(schema=Ads, tags=["Entity"], normalizer=_ENTITY_NORMALIZER)
    def ads(self, connection: SnapchatAdsConnection) -> list[_RECORD]:
        """All ads in the ad account with their attributes."""
        path = f"/{constants.API_VERSION}/adaccounts/{self.account_id}/ads"
        return _entity_records(connection, path, "ads", "ad")

    @il.asset(schema=AdSquads, tags=["Entity"], normalizer=_ENTITY_NORMALIZER)
    def ad_squads(self, connection: SnapchatAdsConnection) -> list[_RECORD]:
        """All ad squads in the ad account with their attributes."""
        path = f"/{constants.API_VERSION}/adaccounts/{self.account_id}/adsquads"
        return _entity_records(connection, path, "adsquads", "adsquad")

    @il.asset(schema=Campaigns, tags=["Entity"], normalizer=_ENTITY_NORMALIZER)
    def campaigns(self, connection: SnapchatAdsConnection) -> list[_RECORD]:
        """All campaigns in the ad account with their attributes."""
        path = f"/{constants.API_VERSION}/adaccounts/{self.account_id}/campaigns"
        return _entity_records(connection, path, "campaigns", "campaign")
