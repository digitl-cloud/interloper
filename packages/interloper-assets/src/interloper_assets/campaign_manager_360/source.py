import datetime as dt
import json
import logging
import time
from io import BytesIO
from typing import Any

import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.campaign_manager_360 import constants, schemas
from interloper_assets.campaign_manager_360.connection import CampaignManager360Connection

logger = logging.getLogger(__name__)

_Record = dict[str, Any]

# Report files queue server-side; poll with backoff until available.
_REPORT_TIMEOUT = 3 * 60 * 60  # 3 hours


# -- NORMALIZER ----------------------------------------------------------------
class CampaignManager360Normalizer(DataFrameNormalizer):
    """Snake-case CM360's CSV report headers onto the schema columns.

    Headers are human-readable ("Active View: % Viewable Impressions",
    "Site (CM360)", "DV360 Cost (USD)"). The generic pass drops ``%`` outright,
    which would collide "% Viewable Impressions" with "Viewable Impressions" —
    so spell it out as ``pct`` first, then defer to snake-casing with digit
    splitting (CM360 → cm_360).
    """

    def column_name(self, name: str) -> str:
        return super().column_name(name.replace("%", "pct"))


# -- HELPERS — report definitions ------------------------------------------------
def _standard_report_body(
    account_id: str, start_date: dt.date, end_date: dt.date, dimensions: list[str], metrics: list[str]
) -> dict:
    return {
        "accountId": account_id,
        "name": f"report_standard_{start_date.isoformat()}_{end_date.isoformat()}",
        "type": "STANDARD",
        "criteria": {
            "dateRange": {"startDate": start_date.isoformat(), "endDate": end_date.isoformat()},
            "dimensions": [{"name": dimension} for dimension in dimensions],
            "metricNames": metrics,
        },
    }


def _reach_report_body(
    account_id: str, start_date: dt.date, end_date: dt.date, dimensions: list[str], metrics: list[str]
) -> dict:
    return {
        "accountId": account_id,
        "name": f"report_reach_{start_date.isoformat()}_{end_date.isoformat()}",
        "type": "REACH",
        "reachCriteria": {
            "dateRange": {"startDate": start_date.isoformat(), "endDate": end_date.isoformat()},
            "dimensions": [{"name": dimension} for dimension in dimensions],
            "metricNames": metrics,
        },
    }


# -- HELPERS — report execution --------------------------------------------------
def _wait_for_file(service: Any, profile_id: str, report_id: int, file_id: int) -> None:
    """Poll a report file until it is available, raising on failure or timeout."""
    deadline = time.monotonic() + _REPORT_TIMEOUT
    interval = float(constants.MIN_RETRY_INTERVAL)
    while True:
        logger.info(f"Waiting for report {report_id} (file {file_id})...")
        status = (
            service.reports().files().get(profileId=profile_id, reportId=report_id, fileId=file_id).execute()
        )["status"]

        if status == "REPORT_AVAILABLE":
            return
        if status in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Report {report_id} failed with status {status}")

        if time.monotonic() >= deadline:
            raise RuntimeError(f"Report {report_id} did not complete within {_REPORT_TIMEOUT}s")
        time.sleep(interval)
        interval = min(interval * 2, constants.MAX_RETRY_INTERVAL)


def _download_file(service: Any, report_id: int, file_id: int) -> BytesIO:
    """Download a report file into memory."""
    from googleapiclient.http import MediaIoBaseDownload

    buffer = BytesIO()
    downloader = MediaIoBaseDownload(buffer, service.files().get_media(reportId=report_id, fileId=file_id))
    done = False
    while not done:
        status, done = downloader.next_chunk()
        logger.info(f"Downloading report {report_id}: {int(status.progress() * 100)}%")
    buffer.seek(0)
    return buffer


def _parse_report_csv(file: BytesIO) -> list[_Record]:
    """Parse a CM360 report file: skip the preamble, drop the trailing totals.

    The file opens with a metadata preamble ending at a "Report Fields" line,
    then the CSV proper, closed by a "Grand Total:" summary row.
    """
    for line in file:
        if b"Report Fields" in line:
            break
    df = pd.read_csv(file, na_values=["-"])
    if df.empty:
        return []
    totals = df[df.iloc[:, 0].astype("string").str.contains("Grand Total:", na=False)].index
    if len(totals):
        df = df.iloc[: totals[-1]]
    return df.to_dict("records")


def _get_report(connection: CampaignManager360Connection, profile_id: str, report_body: dict) -> list[_Record]:
    """Insert, run, download, and parse a report, deleting its definition after."""
    service = connection.client
    report_id = int(service.reports().insert(profileId=profile_id, body=report_body).execute()["id"])
    logger.info(f"Report id: {report_id} ({report_body['type']})")
    try:
        file_id = int(service.reports().run(profileId=profile_id, reportId=report_id).execute()["id"])
        _wait_for_file(service, profile_id, report_id, file_id)
        return _parse_report_csv(_download_file(service, report_id, file_id))
    finally:
        # One-shot report definitions; clean up best-effort so they don't pile up.
        try:
            service.reports().delete(profileId=profile_id, reportId=report_id).execute()
        except Exception as exc:
            logger.warning(f"Failed to delete report {report_id}: {exc}")


def _list_remarketing_lists(service: Any, profile_id: str, advertiser_id: str) -> list[_Record]:
    """Page through the remarketing lists of an advertiser."""
    items: list[_Record] = []
    page_token: str | None = None
    while True:
        response = (
            service.remarketingLists()
            .list(profileId=profile_id, advertiserId=advertiser_id, pageToken=page_token)
            .execute()
        )
        items.extend(response.get("remarketingLists") or [])
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return items


# -- SOURCE --------------------------------------------------------------------
@il.source(
    key="campaign_manager_360",
    resources={
        "connection": CampaignManager360Connection,
    },
    tags=["Advertising"],
    icon="icon:cm360",
    normalizer=CampaignManager360Normalizer(snake_case_digits=True, flatten_max_level=2),
)
class CampaignManager360(il.Source):
    """Campaign Manager 360 advertising platform integration."""

    profile_id: str = il.FetchField(
        title="Profile ID",
        description="CM360 user profile",
        provider="connection.profiles",
        label_key="name",
        value_key="profile_id",
    )
    account_id: str = il.FetchField(
        title="Account ID",
        description="CM360 account",
        provider="connection.profiles",
        label_key="account_name",
        value_key="account_id",
        discriminator=True,
    )
    advertiser_id: str = il.InputField(
        default="",
        title="Advertiser ID",
        description="CM360 advertiser ID (only required for the custom_audiences asset)",
    )

    @il.asset(
        schema=schemas.CampaignsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def campaigns_stats(
        self, context: il.ExecutionContext, connection: CampaignManager360Connection
    ) -> list[_Record]:
        """Campaign performance metrics per day."""
        return _get_report(
            connection,
            self.profile_id,
            _standard_report_body(
                self.account_id,
                context.partition_date,
                context.partition_date,
                dimensions=constants.CAMPAIGN_DIMENSIONS,
                metrics=constants.CAMPAIGN_METRICS,
            ),
        )

    @il.asset(
        schema=schemas.AdsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def ads_stats(self, context: il.ExecutionContext, connection: CampaignManager360Connection) -> list[_Record]:
        """Ad performance metrics per day at placement/creative grain."""
        return _get_report(
            connection,
            self.profile_id,
            _standard_report_body(
                self.account_id,
                context.partition_date,
                context.partition_date,
                dimensions=constants.AD_DIMENSIONS,
                metrics=constants.AD_METRICS,
            ),
        )

    @il.asset(
        schema=schemas.ReachStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def reach_stats(self, context: il.ExecutionContext, connection: CampaignManager360Connection) -> list[_Record]:
        """Unique-reach metrics per campaign and country."""
        rows = _get_report(
            connection,
            self.profile_id,
            _reach_report_body(
                self.account_id,
                context.partition_date,
                context.partition_date,
                dimensions=constants.UNIQUE_REACH_DIMENSIONS,
                metrics=constants.UNIQUE_REACH_METRICS,
            ),
        )
        # The REACH report has no date dimension; stamp the partition day.
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.CustomAudiences,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
    )
    def custom_audiences(
        self, context: il.ExecutionContext, connection: CampaignManager360Connection
    ) -> list[_Record]:
        """Remarketing lists (custom audiences) of the configured advertiser."""
        if not self.advertiser_id:
            raise ValueError("The custom_audiences asset requires the source's advertiser_id to be set")
        items = _list_remarketing_lists(connection.client, self.profile_id, self.advertiser_id)
        for item in items:
            rule = item.get("listPopulationRule")
            # Clauses are a nested list; JSON-encode them onto the string column.
            if isinstance(rule, dict) and isinstance(rule.get("listPopulationClauses"), list):
                rule["listPopulationClauses"] = json.dumps(rule["listPopulationClauses"])
        return [{**item, "date": context.partition_date} for item in items]
