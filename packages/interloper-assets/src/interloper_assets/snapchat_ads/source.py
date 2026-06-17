import datetime as dt
import logging
from typing import Any

import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.snapchat_ads import constants
from interloper_assets.snapchat_ads.connection import SnapchatAdsConnection
from interloper_assets.snapchat_ads.schemas import (
    AdAccountMetadata,
    AdAccountsMetadata,
    Ads,
    AdsByCountry,
    AdsMetadata,
    AdSquadsMetadata,
    Campaigns,
    CampaignsMetadata,
    VideosByOs,
)

logger = logging.getLogger(__name__)

_RECORD = dict[str, Any]

# Stats fields requested for the ad/campaign performance reports.
_REPORT_METRICS = constants.CONVERSION_METRICS + constants.CORE_METRICS + constants.ADDITIONAL_METRICS


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


def _metadata_records(connection: SnapchatAdsConnection, path: str, list_key: str, item_key: str) -> list[_RECORD]:
    """Page a metadata endpoint and unwrap ``page[list_key][i][item_key]`` records."""
    records: list[_RECORD] = []
    for page in _get_pages(connection, path):
        records.extend(item[item_key] for item in page.get(list_key, []))
    return records


# ------------------------------------------------------------------
# HELPERS — framing
# ------------------------------------------------------------------
def _frame_report(rows: list[_RECORD], date: dt.date) -> pd.DataFrame:
    """Flatten stats rows and stamp the partition date.

    Rows carry a nested ``stats`` dict (flattened to ``stats_*`` then de-prefixed)
    or, for dimensioned reports, a ``dimension_stats`` list that is exploded and
    expanded into one row per dimension value.
    """
    df = pd.json_normalize(rows, sep="_")
    if df.empty:
        df["date"] = pd.Series(dtype="object")
        return df

    if "dimension_stats" in df.columns:
        df = df[df["dimension_stats"].map(lambda v: isinstance(v, list) and len(v) > 0)]
        df = df.explode("dimension_stats")
        expanded = df["dimension_stats"].apply(pd.Series)
        df = pd.concat([df.drop(columns=["dimension_stats"]), expanded], axis=1)
    else:
        df = df.rename(columns={col: col.replace("stats_", "") for col in df.columns})

    df["date"] = date
    return df


def _frame_metadata(records: list[_RECORD]) -> pd.DataFrame:
    """Flatten nested metadata records and drop all-null columns."""
    df = pd.json_normalize(records, sep="_", max_level=3)
    return df.dropna(axis=1, how="all")


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------
@il.source(
    resources={"connection": SnapchatAdsConnection},
    tags=["Advertising"],
    icon="mdi:snapchat",
    normalizer=DataFrameNormalizer(),
)
class SnapchatAds(il.Source):
    """Snapchat Ads advertising platform integration."""

    account_id: str = il.FetchField(
        endpoint="snapchat-ads/ad-accounts",
        depends_on="connection",
        label_key="name",
        value_key="id",
        description="Snapchat Ads ad account",
    )

    # --- Time-series reports ---

    @il.asset(schema=Ads, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def ads(self, context: il.ExecutionContext, connection: SnapchatAdsConnection) -> pd.DataFrame:
        """Ad-level performance with core, additional, and conversion metrics."""
        rows = _request_report(connection, self.account_id, context.partition_date, _REPORT_METRICS, breakdown="ad")
        return _frame_report(rows, context.partition_date)

    @il.asset(schema=Campaigns, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def campaigns(self, context: il.ExecutionContext, connection: SnapchatAdsConnection) -> pd.DataFrame:
        """Campaign-level performance with core, additional, and conversion metrics."""
        rows = _request_report(
            connection, self.account_id, context.partition_date, _REPORT_METRICS, breakdown="campaign"
        )
        return _frame_report(rows, context.partition_date)

    @il.asset(schema=AdsByCountry, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def ads_by_country(self, context: il.ExecutionContext, connection: SnapchatAdsConnection) -> pd.DataFrame:
        """Ad performance segmented by country with delivery and conversion metrics."""
        rows = _request_report(
            connection,
            self.account_id,
            context.partition_date,
            constants.CONVERSION_METRICS + constants.DELIVERY_METRICS,
            report_dimension=["country"],
        )
        return _frame_report(rows, context.partition_date)

    @il.asset(schema=VideosByOs, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def videos_by_os(self, context: il.ExecutionContext, connection: SnapchatAdsConnection) -> pd.DataFrame:
        """Video ad performance segmented by operating system."""
        rows = _request_report(
            connection,
            self.account_id,
            context.partition_date,
            constants.VIDEOS_METRICS,
            report_dimension=["os"],
        )
        return _frame_report(rows, context.partition_date)

    # --- Entity (metadata) assets ---

    @il.asset(schema=AdAccountMetadata, tags=["Entity"])
    def ad_account_metadata(self, connection: SnapchatAdsConnection) -> pd.DataFrame:
        """Metadata for a single ad account."""
        path = f"/{constants.API_VERSION}/adaccounts/{self.account_id}"
        return _frame_metadata(_metadata_records(connection, path, "adaccounts", "adaccount"))

    @il.asset(schema=AdAccountsMetadata, tags=["Entity"])
    def ad_accounts_metadata(self, connection: SnapchatAdsConnection) -> pd.DataFrame:
        """Metadata for all ad accounts in the organization owning this account."""
        account_path = f"/{constants.API_VERSION}/adaccounts/{self.account_id}"
        accounts = _metadata_records(connection, account_path, "adaccounts", "adaccount")
        organization_id = accounts[0]["organization_id"] if accounts else None
        if organization_id is None:
            return pd.DataFrame()
        path = f"/{constants.API_VERSION}/organizations/{organization_id}/adaccounts"
        return _frame_metadata(_metadata_records(connection, path, "adaccounts", "adaccount"))

    @il.asset(schema=AdsMetadata, tags=["Entity"])
    def ads_metadata(self, connection: SnapchatAdsConnection) -> pd.DataFrame:
        """Metadata for all ads in the ad account."""
        path = f"/{constants.API_VERSION}/adaccounts/{self.account_id}/ads"
        return _frame_metadata(_metadata_records(connection, path, "ads", "ad"))

    @il.asset(schema=AdSquadsMetadata, tags=["Entity"])
    def ad_squads_metadata(self, connection: SnapchatAdsConnection) -> pd.DataFrame:
        """Metadata for all ad squads in the ad account."""
        path = f"/{constants.API_VERSION}/adaccounts/{self.account_id}/adsquads"
        return _frame_metadata(_metadata_records(connection, path, "adsquads", "adsquad"))

    @il.asset(schema=CampaignsMetadata, tags=["Entity"])
    def campaigns_metadata(self, connection: SnapchatAdsConnection) -> pd.DataFrame:
        """Metadata for all campaigns in the ad account."""
        path = f"/{constants.API_VERSION}/adaccounts/{self.account_id}/campaigns"
        return _frame_metadata(_metadata_records(connection, path, "campaigns", "campaign"))
