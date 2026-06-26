import datetime as dt
import logging
import tempfile
from typing import Any

import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.bing_ads import constants
from interloper_assets.bing_ads.connection import BingAdsConnection
from interloper_assets.bing_ads.schemas import AdsStats

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


# ------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------
def _build_report_time(
    reporting_service: Any,
    date: dt.date,
    timezone: str = "AmsterdamBerlinBernRomeStockholmVienna",
) -> Any:
    """Build a ``ReportTime`` covering the single day *date*."""
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


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------
@il.source(
    resources={"connection": BingAdsConnection},
    tags=["Advertising"],
    icon="icon:bing",
    normalizer=DataFrameNormalizer(snake_case_digits=True),
)
class BingAds(il.Source):
    """Bing Ads (Microsoft Advertising) platform integration."""

    account_id: str = il.FetchField(
        title="Account ID",
        description="Bing Ads account",
        provider="connection.accounts",
        label_key="name",
        value_key="account_id",
    )

    @il.asset(
        schema=AdsStats,
        partitioning=il.TimePartitionConfig(column="time_period"),
        tags=["Report"],
    )
    def ads_stats(self, context: il.ExecutionContext, connection: BingAdsConnection) -> pd.DataFrame:
        """Ad performance report with impressions, clicks, conversions, and revenue metrics."""
        return _ad_performance_report(connection, self.account_id, context.partition_date)
