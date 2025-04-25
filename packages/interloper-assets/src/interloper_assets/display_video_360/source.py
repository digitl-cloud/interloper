import datetime as dt
import logging
from io import StringIO
from typing import Any

import interloper as itlp
import pandas as pd
import requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from interloper_google_cloud import GoogleAuth
from interloper_pandas import DataframeNormalizer
from tenacity import Retrying, before_sleep_log, retry_if_result, stop_after_delay, wait_exponential_jitter

from interloper_assets.display_video_360 import constants

logger = logging.getLogger(__name__)

# TODO: add schemas
# TODO: line items report not tested


@itlp.source(normalizer=DataframeNormalizer())
def display_video_360(
    service_account_key: dict[str, str],
    impersonated_account: str | None = None,
) -> tuple[itlp.Asset, ...]:
    auth = GoogleAuth(
        service_account_key,
        impersonated_account,
        scopes=[
            "https://www.googleapis.com/auth/display-video",
            "https://www.googleapis.com/auth/doubleclickbidmanager",
        ],
    )
    credentials = auth()
    dv360_service = build(serviceName="displayvideo", version="v3", credentials=credentials)
    dbm_service = build(serviceName="doubleclickbidmanager", version="v2", credentials=credentials)

    def build_report(
        start_date: dt.date,
        end_date: dt.date,
        metrics: list[str],
        dimensions: list[str],
        filters: list[dict],
        report_type: str = "standard",
        title: str = "dv360_report",
    ) -> dict[str, Any]:
        return {
            "metadata": {
                "title": title,
                "dataRange": {
                    "range": "CUSTOM_DATES",
                    "customStartDate": {"year": start_date.year, "month": start_date.month, "day": start_date.day},
                    "customEndDate": {"year": end_date.year, "month": end_date.month, "day": end_date.day},
                },
                "format": "CSV",
            },
            "params": {
                "type": report_type.capitalize(),
                "groupBys": dimensions,
                "filters": filters,
                "metrics": metrics,
                "options": {},
            },
            "schedule": {"frequency": "ONE_TIME"},
        }

    def delete_report(query_id: str) -> None:
        try:
            dbm_service.queries().delete(queryId=query_id).execute()
            logger.info("Report deleted successfully")
        except (HttpError, TypeError) as error:
            logger.info(f"An error occurred: {error}")

    def read_report(report_path: str, report_type: str) -> pd.DataFrame:
        if report_type not in ["standard", "reach"]:
            raise ValueError(f"Invalid report_type: {report_type}. Valid options are 'standard' and 'reach'.")

        # Read the file line by line
        response = requests.get(report_path)
        response.raise_for_status()
        response.encoding = "utf-8"
        report_lines = response.text.split("\n")
        valid_lines = []
        for line in report_lines:
            # Check if the line matches the pattern of the footer
            if report_type.lower() == "standard" and line.startswith(","):
                break
            elif report_type.lower() == "reach" and line.startswith("Report Time:"):
                break
            valid_lines.append(line)
        csv_content = "\n".join(valid_lines)
        df = pd.read_csv(
            StringIO(csv_content),
            na_values=["-"],
            delimiter=",",
            engine="python",
            on_bad_lines="skip",
            encoding="utf-8-sig",
        )
        if df.iloc[:, 0].str.contains("No data returned by the reporting service.").any():
            return pd.DataFrame(columns=df.columns)
        return df

    def request_report(
        start_date: dt.date,
        end_date: dt.date,
        metrics: list[str],
        dimensions: list[str],
        filters: list[dict],
        report_type: str = "standard",
        title: str = "dv360_report",
        synchronous: bool = False,
    ) -> tuple[str, str]:
        report_definition = build_report(start_date, end_date, metrics, dimensions, filters, report_type, title)
        query_id = dbm_service.queries().create(body=report_definition).execute()["queryId"]
        report_id = dbm_service.queries().run(queryId=query_id, synchronous=synchronous).execute()["key"]["reportId"]
        return query_id, report_id

    def download_report(
        query_id: str,
        report_id: str,
        max_request_delay: int | dt.timedelta = dt.timedelta(hours=3),
        report_type: str = "standard",
    ) -> list[dict]:
        retrying = Retrying(
            wait=wait_exponential_jitter(
                initial=constants.MIN_RETRY_INTERVAL,
                max=constants.MAX_RETRY_INTERVAL,
                jitter=60,
            ),
            stop=stop_after_delay(max_request_delay),
            before_sleep=before_sleep_log(logger, logging.DEBUG),
            reraise=True,
            retry=retry_if_result(lambda result: result["metadata"]["status"]["state"] not in ["DONE", "FAILED"]),
        )
        for attempt in retrying:
            with attempt:
                logger.info(
                    f"Polling report:{report_id} for query {query_id} (attempt {attempt.retry_state.attempt_number})"
                )

                report = dbm_service.queries().reports().get(queryId=query_id, reportId=report_id).execute()

            if attempt.retry_state.outcome and not attempt.retry_state.outcome.failed:
                attempt.retry_state.set_result(report)

        if report["metadata"]["status"]["state"] == "DONE":
            logger.info(f"Report execution completed - {report_id}")
            df = read_report(report_path=report["metadata"]["googleCloudStoragePath"], report_type=report_type)
            logger.info(f"Report downloaded succesfully - {report_id}")
            data = df.to_dict(orient="records")
            delete_report(query_id=query_id)
            return data
        raise RuntimeError(f"Report {report_id}, with query id: {query_id} finished in error")

    def request_and_download_report(
        start_date: dt.date,
        end_date: dt.date,
        metrics: list[str],
        dimensions: list[str],
        filters: list[dict],
        title: str = "dv360_report",
        synchronous: bool = False,
        max_request_delay: int | dt.timedelta = dt.timedelta(hours=3),
        report_type: str = "standard",
    ) -> list[dict]:
        query_id, report_id = request_report(
            start_date, end_date, metrics, dimensions, filters, report_type, title, synchronous
        )
        return download_report(query_id, report_id, max_request_delay, report_type)

    @itlp.asset
    def partners() -> pd.DataFrame:
        response = dv360_service.partners().list().execute()
        return pd.DataFrame(response["partners"])

    @itlp.asset(
        # schema=...,
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def line_items(
        filters: list[dict],
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            start_date=date,
            end_date=date,
            metrics=constants.METRICS,
            dimensions=constants.DIMENSIONS,
            filters=filters,
            title=f"lineitems_report_{date.isoformat()}",
        )
        return pd.DataFrame(data)

    @itlp.asset(
        # schema=...,
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def line_items_by_country(
        filters: list[dict],
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            start_date=date,
            end_date=date,
            metrics=constants.COUNTRY_METRICS,
            dimensions=constants.COUNTRY_DIMENSIONS,
            filters=filters,
            title=f"lineitems_report_{date.isoformat()}",
        )
        return pd.DataFrame(data)

    return (partners, line_items)
