import datetime as dt
import logging
from typing import Any

import httpx
import interloper as il
import pandas as pd
import tenacity as tc
from interloper_pandas import DataFrameNormalizer

from interloper_assets.impact import schemas
from interloper_assets.impact.connection import ImpactConnection

logger = logging.getLogger(__name__)

_RECORD = dict[str, Any]


# ------------------------------------------------------------------
# HELPERS — pagination
# ------------------------------------------------------------------
def _paginated(connection: ImpactConnection, path: str, params: dict, key: str) -> list[_RECORD]:
    """Page through an Impact list endpoint (``@numpages``) and collect *key* records."""
    records: list[_RECORD] = []
    page, num_pages = 1, 1
    while page <= num_pages:
        response = connection.client.get(path, params={**params, "Page": page})
        response.raise_for_status()
        data = response.json()
        num_pages = int(data["@numpages"])
        records.extend(data.get(key) or [])
        page += 1
    return records


def _date_range(date: dt.date) -> dict[str, str]:
    return {
        "StartDate": date.strftime("%Y-%m-%dT00:00:00Z"),
        "EndDate": date.strftime("%Y-%m-%dT23:59:59Z"),
    }


# ------------------------------------------------------------------
# HELPERS — synchronous list endpoints
# ------------------------------------------------------------------
def _get_actions(connection: ImpactConnection, program_id: str, date: dt.date) -> list[_RECORD]:
    return _paginated(connection, "/Actions", {"CampaignId": program_id, **_date_range(date)}, key="Actions")


def _get_action_updates(connection: ImpactConnection, program_id: str, date: dt.date) -> list[_RECORD]:
    return _paginated(
        connection, "/ActionUpdates", {"CampaignId": program_id, **_date_range(date)}, key="ActionUpdates"
    )


def _get_action_inquiries(connection: ImpactConnection, program_id: str, date: dt.date) -> list[_RECORD]:
    return _paginated(
        connection, "/ActionInquiries", {"CampaignId": program_id, **_date_range(date)}, key="ActionInquiries"
    )


# ------------------------------------------------------------------
# HELPERS — async exports (report + clicks) via the Jobs API
# ------------------------------------------------------------------
def _wait_for_job(connection: ImpactConnection, job_id: str) -> None:
    """Poll a queued Impact job until it completes, raising on failure/timeout."""

    def _terminal(response: _RECORD) -> _RECORD:
        if response["Status"] in ("CANCELLED", "ERROR", "FAILED"):
            raise RuntimeError(f"Impact job {job_id} failed with status {response['Status']}")
        return response

    for attempt in tc.Retrying(
        wait=tc.wait_exponential(multiplier=1, min=120, max=180),
        stop=tc.stop_after_delay(dt.timedelta(minutes=15)),
        retry=tc.retry_if_result(lambda r: r["Status"] != "COMPLETED")
        | tc.retry_if_exception_type(httpx.HTTPError),
        before_sleep=tc.before_sleep_log(logger, logging.DEBUG),
        reraise=True,
    ):
        with attempt:
            logger.info(f"Waiting for Impact job {job_id}...")
            response = connection.client.get(f"/Jobs/{job_id}")
            response.raise_for_status()
            response = _terminal(response.json())
        if attempt.retry_state.outcome and not attempt.retry_state.outcome.failed:
            attempt.retry_state.set_result(response)


def _download_job(connection: ImpactConnection, job_id: str, key: str) -> list[_RECORD]:
    """Download a completed job's results, paging through ``@numpages``."""
    records: list[_RECORD] = []
    page, num_pages = 1, 1
    while page <= num_pages:
        response = connection.client.get(
            f"/Jobs/{job_id}/Download",
            headers={"Accept": "application/octet-stream"},
            params={"Mode": "stream", "Page": page},
        )
        response.raise_for_status()
        data = response.json()
        num_pages = int(data["@numpages"])
        records.extend(data.get(key) or [])
        page += 1
    return records


def _report_export(connection: ImpactConnection, report_id: str, program_id: str, date: dt.date) -> list[_RECORD]:
    """Request a report export, wait for the job, and download its ``Records``."""
    response = connection.client.get(
        f"/ReportExport/{report_id}",
        params={"SUBAID": program_id, "ResultFormat": "JSON", **_date_range(date)},
    )
    response.raise_for_status()
    job_id = response.json()["QueuedUri"].split("/")[-1]
    _wait_for_job(connection, job_id)
    return _download_job(connection, job_id, key="Records")


def _clicks_export(connection: ImpactConnection, program_id: str, date: dt.date) -> list[_RECORD]:
    """Request a clicks export, wait for the job, and download its ``Clicks``."""
    response = connection.client.get(
        f"/Programs/{program_id}/ClickExport",
        params={"Date": date.isoformat(), "ResultFormat": "JSON"},
    )
    response.raise_for_status()
    job_id = response.json()["QueuedUri"].split("/")[-1]
    _wait_for_job(connection, job_id)
    return _download_job(connection, job_id, key="Clicks")


# ------------------------------------------------------------------
# HELPERS — framing
# ------------------------------------------------------------------
def _to_df(records: list[_RECORD], date: dt.date) -> pd.DataFrame:
    """Build a DataFrame and stamp the partition date.

    Empty-string blanking and column normalization are handled by the source's
    normalizer (``replace_empty_strings``).
    """
    df = pd.DataFrame(records)
    df["date"] = date
    return df


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------
@il.source(
    resources={"connection": ImpactConnection},
    tags=["Affiliate"],
    icon="fluent:connector-24-filled",
    # Impact returns PascalCase fields with digit groups (SubId1 -> sub_id_1) and
    # blanks as empty strings.
    normalizer=DataFrameNormalizer(snake_case_digits=True, replace_empty_strings=True),
)
class Impact(il.Source):
    """Impact.com affiliate and partnership platform integration."""

    program_id: str = il.FetchField(
        endpoint="impact/programs",
        depends_on="connection",
        label_key="Name",
        value_key="Id",
        description="Impact program (campaign) ID",
    )

    @il.asset(schema=schemas.Actions, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def actions(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Conversion actions including revenue, commissions, and attribution data."""
        return _to_df(_get_actions(connection, self.program_id, context.partition_date), context.partition_date)

    @il.asset(schema=schemas.ActionUpdates, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def action_updates(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Action updates tracking changes to conversion actions over time."""
        return _to_df(_get_action_updates(connection, self.program_id, context.partition_date), context.partition_date)

    @il.asset(schema=schemas.ActionInquiries, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def action_inquiries(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Action inquiries raised against conversion actions."""
        return _to_df(
            _get_action_inquiries(connection, self.program_id, context.partition_date), context.partition_date
        )

    @il.asset(schema=schemas.ActionsBySku, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def actions_by_sku(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Conversion actions broken down by product SKU."""
        records = _report_export(
            connection, "adv_action_list_sku_pm_only", self.program_id, context.partition_date
        )
        return _to_df(records, context.partition_date)

    @il.asset(schema=schemas.Clicks, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def clicks(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Click events for the program."""
        return _to_df(_clicks_export(connection, self.program_id, context.partition_date), context.partition_date)

    @il.asset(schema=schemas.Performance, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def performance(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Daily performance metrics (clicks, actions, revenue)."""
        records = _report_export(
            connection, "att_adv_performance_by_day_pm_only", self.program_id, context.partition_date
        )
        return _to_df(records, context.partition_date)

    @il.asset(schema=schemas.PerformanceByAd, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def performance_by_ad(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Performance metrics broken down by ad."""
        records = _report_export(
            connection, "adv_performance_by_ad_pm_only", self.program_id, context.partition_date
        )
        return _to_df(records, context.partition_date)

    @il.asset(schema=schemas.PerformanceByDomain, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def performance_by_domain(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Performance metrics broken down by referring domain."""
        records = _report_export(
            connection, "att_adv_performance_by_ref_domain_pm_only", self.program_id, context.partition_date
        )
        return _to_df(records, context.partition_date)

    @il.asset(schema=schemas.PerformanceBySharedId, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def performance_by_shared_id(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Performance metrics broken down by shared ID."""
        records = _report_export(
            connection, "att_adv_performance_by_shared_id_pm_only", self.program_id, context.partition_date
        )
        return _to_df(records, context.partition_date)

    @il.asset(schema=schemas.PerformanceByIo, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    def performance_by_io(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Performance metrics broken down by insertion order."""
        records = _report_export(
            connection, "att_adv_performance_by_IO", self.program_id, context.partition_date
        )
        return _to_df(records, context.partition_date)
