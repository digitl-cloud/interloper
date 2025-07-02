import datetime as dt
import logging
import tempfile
from typing import Any, cast

import interloper as itlp
import pandas as pd
from bingads import AuthorizationData, OAuthAuthorization, OAuthWebAuthCodeGrant, ServiceClient
from bingads.v13.reporting.reporting_download_parameters import ReportingDownloadParameters
from bingads.v13.reporting.reporting_service_manager import ReportingServiceManager
from interloper_pandas import DataframeNormalizer
from suds.sudsobject import Object

from interloper_assets.bing_ads import constants, schemas

logger = logging.getLogger(__name__)


class BingAdsDataframeNormalizer(DataframeNormalizer):
    def __init__(self):
        super().__init__(
            remove_empty_strings=True,
        )

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        data = super().normalize(data)

        columns_to_convert = [
            "ctr",
            "conversion_rate",
            "all_conversion_rate",
            "absolute_top_impression_rate_percent",
            "top_impression_rate_percent",
        ]

        for column in columns_to_convert:
            if column in data.columns:
                data[column] = data[column].str.rstrip("%")

        return data


@itlp.source(
    normalizer=BingAdsDataframeNormalizer(),
)
def bing_ads(
    client_id: str = itlp.Env("BING_ADS_CLIENT_ID"),
    client_secret: str = itlp.Env("BING_ADS_CLIENT_SECRET"),
    developer_token: str = itlp.Env("BING_ADS_DEVELOPER_TOKEN"),
    refresh_token: str = itlp.Env("BING_ADS_REFRESH_TOKEN"),
) -> tuple[itlp.Asset, ...]:
    def authenticate(account_id: str, customer_id: str) -> AuthorizationData:
        oauth_web_auth_code_grant = OAuthWebAuthCodeGrant(
            client_id=client_id,
            client_secret=client_secret,
            redirection_uri=None,
        )

        oauth_tokens = oauth_web_auth_code_grant.request_oauth_tokens_by_refresh_token(refresh_token)

        authorization_data = AuthorizationData(
            developer_token=developer_token,
            authentication=OAuthAuthorization(
                client_id=oauth_web_auth_code_grant.client_id,
                oauth_tokens=oauth_tokens,
            ),
        )
        authorization_data.account_id = account_id
        authorization_data.customer_id = customer_id
        return authorization_data

    def build_report_time(
        service_client: ServiceClient,
        date: dt.date,
        timezone: str = "AmsterdamBerlinBernRomeStockholmVienna",
    ) -> Object:
        start_date: Object = service_client.factory.create("Date")
        start_date.Day = int(date.day)
        start_date.Month = int(date.month)
        start_date.Year = int(date.year)

        end_date: Object = service_client.factory.create("Date")
        end_date.Day = int(date.day)
        end_date.Month = int(date.month)
        end_date.Year = int(date.year)

        report_time: Object = service_client.factory.create("ReportTime")
        report_time.CustomDateRangeStart = start_date
        report_time.CustomDateRangeEnd = end_date
        report_time.PredefinedTime = None
        report_time.ReportTimeZone = timezone

        return report_time

    def build_ad_performance_report_request(
        reporting_service: ServiceClient,
        time: Object,
        account_id: str,
        aggregation: str,
    ) -> Object:
        scope = reporting_service.factory.create("AccountThroughAdGroupReportScope")
        scope.AccountIds = {"long": [account_id]}
        scope.Campaigns = None

        columns: Any = reporting_service.factory.create("ArrayOfAdPerformanceReportColumn")
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
        request.Time = time
        request.ReportName = "Ad Performance Report"
        return request

    def request_report(authorization_data: AuthorizationData, request: Object) -> list[dict]:
        service_manager = ReportingServiceManager(authorization_data)

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=True) as f:
            reporting_download_parameters = ReportingDownloadParameters(
                report_request=request,
                result_file_directory=tempfile.gettempdir(),
                result_file_name=f.name,
                overwrite_result_file=True,
                timeout_in_milliseconds=3600000,
            )

            service_manager.download_file(reporting_download_parameters)

            try:
                df = pd.read_csv(f.name, thousands=",", encoding="utf-8-sig")
                report = df.to_dict(orient="records")
            except pd.errors.EmptyDataError:
                report = []

            return report

    @itlp.asset(
        schema=schemas.Ads,
        partitioning=itlp.TimePartitionConfig(column="time_period"),  # TODO: check
    )
    def ads(
        account_id: str = itlp.Env("BING_ADS_ACCOUNT_ID"),
        customer_id: str = itlp.Env("BING_ADS_CUSTOMER_ID"),
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        authorization_data = authenticate(account_id, customer_id)
        reporting_service = ServiceClient("ReportingService", 13, authorization_data)
        report_time = build_report_time(reporting_service, date)
        request = build_ad_performance_report_request(
            reporting_service=reporting_service,
            time=report_time,
            account_id=cast(str, authorization_data.account_id),
            aggregation="Daily",
        )
        data = request_report(authorization_data, request)
        return pd.DataFrame(data)

    return (ads,)
