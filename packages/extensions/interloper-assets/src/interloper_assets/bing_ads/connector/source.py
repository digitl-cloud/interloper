import asyncio
import datetime as dt
import logging
import tempfile
from typing import Any

import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.bing_ads import constants
from interloper_assets.bing_ads.connection import BingAdsConnection
from interloper_assets.bing_ads.connector.schemas import Accounts, AdGroups, Ads, AdsStats, Campaigns

logger = logging.getLogger(__name__)

# Rate columns the API returns as percentage strings (e.g. ``"1.50%"``); the
# trailing ``%`` is stripped so the values parse as plain numbers downstream.
PERCENTAGE_COLUMNS = [
    "Ctr",
    "ConversionRate",
    "AllConversionRate",
    "AbsoluteTopImpressionRatePercent",
    "TopImpressionRatePercent",
]


# -- HELPERS -------------------------------------------------------------------
def _build_report_time(
    reporting_service: Any,
    date: dt.date,
    timezone: str = "AmsterdamBerlinBernRomeStockholmVienna",
) -> Any:
    """Build a ``ReportTime`` covering the single day *date*.

    Bing dates each row in its campaign's time zone whatever the request
    names: ``ReportTimeZone`` only decides which zone's today the end date
    may not pass. One fixed zone therefore serves every account.

    Args:
        reporting_service: The Reporting service whose factory builds the request objects.
        date: The day the report covers.
        timezone: The Bing ``ReportTimeZone`` whose today bounds the end date.

    Returns:
        The SDK's ``ReportTime``.
    """
    start_date = reporting_service.factory.create("Date")
    start_date.Day = int(date.day)
    start_date.Month = int(date.month)
    start_date.Year = int(date.year)

    end_date = reporting_service.factory.create("Date")
    end_date.Day = int(date.day)
    end_date.Month = int(date.month)
    end_date.Year = int(date.year)

    report_time = reporting_service.factory.create("ReportTime")
    report_time.CustomDateRangeStart = start_date
    report_time.CustomDateRangeEnd = end_date
    report_time.PredefinedTime = None
    report_time.ReportTimeZone = timezone
    return report_time


def _build_ad_performance_report_request(
    reporting_service: Any,
    report_time: Any,
    account_id: str,
    aggregation: str = "Daily",
) -> Any:
    """Build an ``AdPerformanceReportRequest`` scoped to a single account."""
    scope = reporting_service.factory.create("AccountThroughAdGroupReportScope")
    scope.AccountIds = {"long": [account_id]}
    scope.Campaigns = None

    columns = reporting_service.factory.create("ArrayOfAdPerformanceReportColumn")
    columns.AdPerformanceReportColumn.append(constants.AD_PERFORMANCE_FIELDS)

    request = reporting_service.factory.create("AdPerformanceReportRequest")
    request.Scope = scope
    request.Columns = columns
    request.Aggregation = aggregation
    request.ExcludeColumnHeaders = False
    request.ExcludeReportFooter = True
    request.ExcludeReportHeader = True
    request.Format = "Csv"
    request.ReturnOnlyCompleteData = False
    request.Time = report_time
    request.ReportName = "Ad Performance Report"
    return request


def _translate_soap_fault(error: Exception) -> None:
    """Re-raise a Bing SOAP ``WebFault`` with its precise operation errors.

    The SDK surfaces a generic ``"Invalid client data"`` ``faultstring``; the
    actionable reason (e.g. ``AccountNotAuthorized``) lives in the fault detail.
    Pulls those out into a readable message. Does nothing for other exceptions.

    Raises:
        RuntimeError: Always; the function exists to raise it.

    """
    from suds import WebFault

    if not isinstance(error, WebFault):
        return

    detail = getattr(error.fault, "detail", None)
    messages: list[str] = []
    for fault_name in ("ApiFaultDetail", "AdApiFaultDetail"):
        fault_detail = getattr(detail, fault_name, None)
        if not fault_detail:
            continue
        for group_name, item_name in (
            ("OperationErrors", "OperationError"),
            ("BatchErrors", "BatchError"),
            ("Errors", "AdApiError"),
        ):
            group = getattr(fault_detail, group_name, None)
            if not group or isinstance(group, str):
                continue
            items = getattr(group, item_name, [])
            for item in items if isinstance(items, list) else [items]:
                code = getattr(item, "ErrorCode", None) or getattr(item, "Code", None)
                message = getattr(item, "Message", "")
                messages.append(f"{code}: {message}" if code else str(message))

    if messages:
        raise RuntimeError("Bing Ads API error -- " + "; ".join(messages)) from error


def _download_report(connection: BingAdsConnection, account_id: str, request: Any) -> pd.DataFrame:
    """Download a report request to a temp CSV and return it as a DataFrame."""
    from bingads.v13.reporting.reporting_download_parameters import ReportingDownloadParameters

    with tempfile.TemporaryDirectory() as result_dir:
        download_parameters = ReportingDownloadParameters(
            report_request=request,
            result_file_directory=result_dir,
            result_file_name="report.csv",
            overwrite_result_file=True,
            timeout_in_milliseconds=3600000,
        )
        try:
            result_path = connection.reporting_service_manager(account_id).download_file(download_parameters)
        except Exception as error:
            _translate_soap_fault(error)  # raises RuntimeError for a SOAP fault
            raise

        # The SDK returns ``None`` when the report contains no data for the period.
        if not result_path:
            return pd.DataFrame()

        try:
            df = pd.read_csv(result_path, thousands=",", encoding="utf-8-sig")
        except pd.errors.EmptyDataError:
            return pd.DataFrame()

    for column in PERCENTAGE_COLUMNS:
        if column in df.columns:
            df[column] = df[column].astype("string").str.rstrip("%")

    return df


def _ad_performance_report(connection: BingAdsConnection, account_id: str, date: dt.date) -> pd.DataFrame:
    """Request and download the daily Ad Performance report for *date*."""
    reporting_service = connection.reporting_service(account_id)
    report_time = _build_report_time(reporting_service, date)
    request = _build_ad_performance_report_request(
        reporting_service=reporting_service,
        report_time=report_time,
        account_id=account_id,
    )
    return _download_report(connection, account_id, request)


def _bulk_records(connection: BingAdsConnection, account_id: str, customer_id: str, entities: str) -> pd.DataFrame:
    """Download the account's Bulk file of one entity type and return its records.

    Used by the campaigns, ad groups and ads assets. The file is read as the
    Bulk API writes it, every value as text: an id never passes through a
    float. Its header is shared by every record type, so the columns the
    downloaded records leave empty are dropped, and its file-level rows
    (format version, account) are not records. Three serializations are read
    here, since the conformer would misread them: booleans are ``True`` and
    ``False``, days are month first (``11/05/2026``, or ``11/05/2026 00:00:00``
    on a campaign record, whose day is the part before the time), and
    instants are month first too, in UTC.

    Args:
        connection: The connection the file is downloaded with.
        account_id: The account whose entities are downloaded.
        customer_id: The account's parent customer, which the Bulk service requires.
        entities: The Bulk ``DownloadEntity`` to download: ``Campaigns``, ``AdGroups`` or ``Ads``.

    Returns:
        One row per record, under the Bulk file's column names.
    """
    from bingads.v13.bulk import DownloadParameters

    with tempfile.TemporaryDirectory() as result_dir:
        download_parameters = DownloadParameters(
            result_file_directory=result_dir,
            result_file_name="entities.csv",
            overwrite_result_file=True,
            data_scope=["EntityData"],
            download_entities=[entities],
            file_type="Csv",
            timeout_in_milliseconds=3600000,
        )
        try:
            result_path = connection.bulk_service_manager(account_id, customer_id).download_file(download_parameters)
        except Exception as error:
            _translate_soap_fault(error)  # raises RuntimeError for a SOAP fault
            raise
        df = pd.read_csv(result_path, dtype=str, encoding="utf-8-sig")

    df = df[~df["Type"].isin(constants.BULK_FILE_RECORDS)].dropna(axis="columns", how="all")
    for column in df.columns:
        values = df[column].dropna().str.lower()
        if values.isin(["true", "false"]).all():
            df[column] = df[column].str.lower().map({"true": True, "false": False})
    for column in ("Start Date", "End Date"):
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], format="%m/%d/%Y", exact=False).dt.date
    if "Modified Time" in df.columns:
        df["Modified Time"] = pd.to_datetime(df["Modified Time"], format="mixed", utc=True)
    return df.reset_index(drop=True)


async def _bulk_snapshot(connection: BingAdsConnection, account_id: str, entities: str, date: dt.date) -> pd.DataFrame:
    """Snapshot one entity type of the account, stamped with the snapshot's day.

    Used by the campaigns, ad groups and ads assets. The blocking SDK download
    runs in a thread, after the account's customer is resolved.

    Args:
        connection: The connection the file is downloaded with.
        account_id: The account whose entities are downloaded.
        entities: The Bulk ``DownloadEntity`` to download, see ``_bulk_records``.
        date: The snapshot's day, stamped on every row as ``date``.

    Returns:
        One row per entity.
    """
    customer_id = await connection.customer_id(account_id)
    records = await asyncio.to_thread(_bulk_records, connection, account_id, customer_id, entities)
    return records.assign(date=date)


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    icon="icon:bing",
    normalizer=DataFrameNormalizer(snake_case_digits=True),
)
class BingAds(il.Source):
    """Bing Ads (Microsoft Advertising) platform integration.

    The ad report comes from the Reporting service. The account comes from
    Customer Management, without its business address and tax registration,
    and the campaigns, ad groups and ads from the Bulk API, whose records
    name each entity's parent (an ad group's campaign, an ad's ad group). The
    Bulk columns are those of the bingads SDK version the connector pins
    (13.0.29), so a column Bing added since is not collected yet.
    """

    connection: BingAdsConnection

    account_id: str = il.FetchField(
        label="Account ID",
        description="Bing Ads account",
        provider="connection.accounts",
        label_key="name",
        value_key="account_id",
        discriminator=True,
    )

    @il.asset(
        schema=AdsStats,
        partitioning=il.TimePartitionConfig(column="time_period"),
        tags=["Report"],
    )
    def ads_stats(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad performance report with impressions, clicks, conversions, and revenue metrics."""
        return _ad_performance_report(self.connection, self.account_id, context.partition_date)

    @il.asset(schema=Accounts, partitioning=il.SnapshotPartitionConfig(column="date"), tags=["Entity"])
    async def accounts(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """The account with its name, currency, time zone, status and parent customer."""
        account = await self.connection.account(self.account_id)
        fields = {name: value for name, value in account.items() if name not in constants.ACCOUNT_EXCLUDED_FIELDS}
        return [{**fields, "date": context.partition_date}]

    @il.asset(schema=Campaigns, partitioning=il.SnapshotPartitionConfig(column="date"), tags=["Entity"])
    async def campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Campaigns with their type, status, budget, bid strategy and time zone."""
        return await _bulk_snapshot(self.connection, self.account_id, "Campaigns", context.partition_date)

    @il.asset(schema=AdGroups, partitioning=il.SnapshotPartitionConfig(column="date"), tags=["Entity"])
    async def ad_groups(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad groups with their campaign, status, schedule and bids."""
        return await _bulk_snapshot(self.connection, self.account_id, "AdGroups", context.partition_date)

    @il.asset(schema=Ads, partitioning=il.SnapshotPartitionConfig(column="date"), tags=["Entity"])
    async def ads(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ads of every type with their ad group, status, texts and landing pages."""
        return await _bulk_snapshot(self.connection, self.account_id, "Ads", context.partition_date)
