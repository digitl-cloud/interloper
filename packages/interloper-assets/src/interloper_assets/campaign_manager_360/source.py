import datetime as dt
import logging
from io import BytesIO

import interloper as itlp
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from interloper_google_cloud import GoogleAuth
from interloper_pandas import DataframeNormalizer
from tenacity import RetryCallState, Retrying, retry_if_result, stop_after_delay, wait_exponential_jitter

from interloper_assets.campaign_manager_360 import constants

logger = logging.getLogger(__name__)

# TODO: review how to handle the service account key as an AssetParam
# TODO: add schemas


@itlp.source(normalizer=DataframeNormalizer())
def campaign_manager_360(
    service_account_key: dict[str, str],
    impersonated_account: str | None = None,
) -> tuple[itlp.Asset, ...]:
    auth = GoogleAuth(service_account_key, impersonated_account)
    service = build(serviceName="dfareporting", version="v4", credentials=auth())

    def build_standard_report(
        account_id: str,
        start_date: dt.date,
        end_date: dt.date,
        metrics: list[str],
        dimensions: list[str],
    ) -> dict:
        return {
            "accountId": account_id,
            "name": f"report_standard_{start_date.isoformat()}_{end_date.isoformat()}",
            "type": "STANDARD",
            "criteria": {
                "dateRange": {
                    "startDate": f"{start_date.isoformat()}",
                    "endDate": f"{end_date.isoformat()}",
                },
                "dimensions": [{"name": dimension} for dimension in dimensions],
                "metricNames": metrics,
            },
        }

    def build_unique_reach_report(
        account_id: str,
        start_date: dt.date,
        end_date: dt.date,
        dimensions: list[str],
        metrics: list[str],
        campaign_id: str | None = None,
    ) -> dict:
        return {
            "accountId": account_id,
            "type": "REACH",
            "name": f"report_reach_{campaign_id}_{start_date.isoformat()}_{end_date.isoformat()}"
            if campaign_id
            else f"report_weekly_reach_{start_date.isoformat()}_{end_date.isoformat()}",
            "reachCriteria": {
                "dimensions": [{"name": dimension} for dimension in dimensions],
                "dateRange": {
                    "startDate": f"{start_date.isoformat()}",
                    "endDate": f"{end_date.isoformat()}",
                },
                "metricNames": metrics,
            },
            "dimensionFilters": [{"dimensionName": "campaign", "value": f"{campaign_id}"}] if campaign_id else [],
        }

    def create_report(profile_id: str, report_definition: dict) -> int:
        response = service.reports().insert(profileId=profile_id, body=report_definition).execute()
        report_id = response["id"]
        assert report_id is not None
        return int(report_id)

    def run_report(profile_id: str, report_id: int, synchronous: bool = False) -> int:
        response = service.reports().run(profileId=profile_id, reportId=report_id, synchronous=synchronous).execute()
        file_id: str = response["id"]
        assert file_id is not None
        return int(file_id)

    def delete_report(profile_id: str, report_id: int) -> None:
        try:
            service.reports().delete(profileId=profile_id, reportId=report_id).execute()
            logger.info("Report deleted successfully")
        except HttpError as error:
            logger.info(f"An error occurred: {error}")

    def is_report_ready(
        profile_id: str,
        report_id: int,
        file_id: int,
        max_request_delay: int | dt.timedelta = dt.timedelta(hours=3),
    ) -> bool:
        retrying = Retrying(
            wait=wait_exponential_jitter(
                initial=constants.MIN_RETRY_INTERVAL,
                max=constants.MAX_RETRY_INTERVAL,
                jitter=60,
            ),
            stop=stop_after_delay(max_request_delay),
            before_sleep=log_retry,
            reraise=True,
            retry=retry_if_result(lambda result: result not in ["REPORT_AVAILABLE", "FAILED", "CANCELLED"]),
        )
        for attempt in retrying:
            with attempt:
                logger.info(
                    f"Polling report: {report_id}, for profile_id: {profile_id}, and file_id: {file_id} "
                    f"(attempt {attempt.retry_state.attempt_number})"
                )
                report_status = (
                    service.reports().files().get(profileId=profile_id, reportId=report_id, fileId=file_id).execute()
                )["status"]

            if attempt.retry_state.outcome and not attempt.retry_state.outcome.failed:
                attempt.retry_state.set_result(report_status)

        if report_status == "REPORT_AVAILABLE":
            logger.info(f"Report completed - {report_id}")
            return True
        else:
            logger.info("Report failed or cancelled.")
            return False

    def remove_report_headers(file: BytesIO) -> list[dict]:
        for line in iter(file.readline, b""):
            if b"Report Fields" in line:
                logger.info("Report Fields found it - Stop it")
                break

        df = pd.read_csv(file, na_values=["-"], delimiter=",", engine="python")
        # Find the index of the last row containing "Grand Total"
        grand_total_idx = df[df.iloc[:, 0].str.contains("Grand Total:")].index[-1]
        # Remove all rows from the grand total row until the end
        df = df.iloc[:grand_total_idx]

        return df.to_dict(orient="records")

    def log_retry(retry_state: RetryCallState) -> None:
        if retry_state.outcome is None:
            raise RuntimeError("log called before outcome was set")

        if retry_state.next_action is None:
            raise RuntimeError("log called before next_action was set")

        if retry_state.outcome.failed:
            ex = retry_state.outcome.exception()
            logger.debug(f"Retrying in {retry_state.next_action.sleep}. Raised {ex.__class__.__name__}: {ex}.")
        else:
            report_status = retry_state.outcome.result()
            logger.debug(f"Retrying in {retry_state.next_action.sleep}. Returned report status: {report_status}")

    def request_standard_report(
        profile_id: str,
        account_id: str,
        start_date: dt.date,
        end_date: dt.date,
        metrics: list[str],
        dimensions: list[str],
    ) -> list[dict]:
        report_def = build_standard_report(account_id, start_date, end_date, metrics, dimensions)
        report_id = create_report(profile_id=profile_id, report_definition=report_def)
        file_id = run_report(profile_id=profile_id, report_id=report_id)
        report_status = is_report_ready(profile_id=profile_id, report_id=report_id, file_id=file_id)

        if not report_status:
            raise RuntimeError("Failed to generate report")

        request = service.files().get_media(reportId=report_id, fileId=file_id)

        with BytesIO() as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                logger.info(f"Download {int(status.progress() * 100)}")

            # The file has been downloaded into RAM, now parse it as a list of dicts
            f.seek(0)
            data = remove_report_headers(file=f)

        delete_report(profile_id=profile_id, report_id=report_id)

        return data

    def request_unique_reach_report(
        profile_id: str,
        account_id: str,
        start_date: dt.date,
        end_date: dt.date,
        campaign_id: str | None = None,
        dimensions: list[str] = constants.UNIQUE_REACH_DIMENSIONS,
        metrics: list[str] = constants.UNIQUE_REACH_METRICS,
    ) -> list[dict] | None:
        report_def = build_unique_reach_report(
            account_id=account_id,
            start_date=start_date,
            end_date=end_date,
            campaign_id=campaign_id,
            dimensions=dimensions,
            metrics=metrics,
        )
        report_id = create_report(profile_id=profile_id, report_definition=report_def)
        file_id = run_report(profile_id=profile_id, report_id=report_id)
        report_status = is_report_ready(
            profile_id=profile_id,
            report_id=report_id,
            file_id=file_id,
        )

        if report_status:
            request = service.files().get_media(reportId=report_id, fileId=file_id)

            with BytesIO() as f:
                downloader = MediaIoBaseDownload(f, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                    logger.info(f"Download {int(status.progress() * 100)}")

                # The file has been downloaded into RAM, now parse it as a list of dicts
                f.seek(0)
                data = remove_report_headers(file=f)

            delete_report(profile_id=profile_id, report_id=report_id)

            return data

    @itlp.asset(
        # schema=...,
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def ads(
        profile_id: str,
        account_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_standard_report(
            profile_id=profile_id,
            account_id=account_id,
            start_date=date,
            end_date=date,
            metrics=constants.AD_METRICS,
            dimensions=constants.AD_DIMENSIONS,
        )

        return pd.DataFrame(data)

    @itlp.asset(
        # schema=...,
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def campaigns(
        profile_id: str,
        account_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_standard_report(
            profile_id=profile_id,
            account_id=account_id,
            start_date=date,
            end_date=date,
            metrics=constants.CAMPAIGN_METRICS,
            dimensions=constants.CAMPAIGN_DIMENSIONS,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        # schema=...,
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def reach(
        profile_id: str,
        account_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_unique_reach_report(
            profile_id=profile_id,
            account_id=account_id,
            start_date=date,
            end_date=date,
            metrics=constants.UNIQUE_REACH_METRICS,
            dimensions=constants.UNIQUE_REACH_DIMENSIONS,
        )
        return pd.DataFrame(data)

    return (ads, campaigns, reach)
