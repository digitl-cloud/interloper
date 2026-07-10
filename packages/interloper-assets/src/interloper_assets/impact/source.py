import asyncio
import datetime as dt
import logging
from typing import Any

import httpx
import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.impact import schemas
from interloper_assets.impact.connection import ImpactConnection

logger = logging.getLogger(__name__)

_RECORD = dict[str, Any]

# Max pages fetched concurrently per paginated endpoint / job download.
PAGE_CONCURRENCY = 8

# Job polling cadence (Impact exports queue server-side and complete in minutes).
_JOB_POLL_INTERVAL = 120.0  # seconds between polls
_JOB_POLL_TIMEOUT = 900.0  # 15 minutes


# -- HELPERS — pagination ------------------------------------------------------
async def _paginated(connection: ImpactConnection, path: str, params: dict, key: str) -> list[_RECORD]:
    """Page through an Impact list endpoint (``@numpages``) and collect *key* records.

    The first page reports ``@numpages``, so pages 2..N are fetched concurrently
    (bounded by ``PAGE_CONCURRENCY``) by the paginating client.
    """
    paginator = il.PageNumberPaginator(page_param="Page", total_path="@numpages")
    pages = connection.client.paginate(
        path,
        paginator,
        params=params,
        data_selector=lambda r: r.json().get(key) or [],
        concurrency=PAGE_CONCURRENCY,
    )
    return [row async for page in pages for row in page]


def _date_range(date: dt.date) -> dict[str, str]:
    return {
        "StartDate": date.strftime("%Y-%m-%dT00:00:00Z"),
        "EndDate": date.strftime("%Y-%m-%dT23:59:59Z"),
    }


# -- HELPERS — synchronous list endpoints --------------------------------------
async def _get_actions(connection: ImpactConnection, program_id: str, date: dt.date) -> list[_RECORD]:
    return await _paginated(connection, "/Actions", {"CampaignId": program_id, **_date_range(date)}, key="Actions")


async def _get_action_updates(connection: ImpactConnection, program_id: str, date: dt.date) -> list[_RECORD]:
    return await _paginated(
        connection, "/ActionUpdates", {"CampaignId": program_id, **_date_range(date)}, key="ActionUpdates"
    )


async def _get_action_inquiries(connection: ImpactConnection, program_id: str, date: dt.date) -> list[_RECORD]:
    return await _paginated(
        connection, "/ActionInquiries", {"CampaignId": program_id, **_date_range(date)}, key="ActionInquiries"
    )


# -- HELPERS — async exports (report + clicks) via the Jobs API ----------------
async def _wait_for_job(connection: ImpactConnection, job_id: str) -> None:
    """Poll a queued Impact job until it completes, raising on failure/timeout.

    Each poll depends on the previous one's status, so this stays sequential —
    a fixed-interval async poll loop bounded by ``_JOB_POLL_TIMEOUT``.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _JOB_POLL_TIMEOUT
    while True:
        logger.info(f"Waiting for Impact job {job_id}...")
        try:
            response = await connection.client.get(f"/Jobs/{job_id}")
            response.raise_for_status()
            status = response.json()["Status"]
        except httpx.HTTPError as exc:
            logger.debug(f"Transient error polling Impact job {job_id}: {exc}")
            status = None

        if status == "COMPLETED":
            return
        if status in ("CANCELLED", "ERROR", "FAILED"):
            raise RuntimeError(f"Impact job {job_id} failed with status {status}")

        if loop.time() >= deadline:
            raise RuntimeError(f"Impact job {job_id} did not complete within {_JOB_POLL_TIMEOUT:.0f}s")
        await asyncio.sleep(_JOB_POLL_INTERVAL)


async def _download_job(connection: ImpactConnection, job_id: str, key: str) -> list[_RECORD]:
    """Download a completed job's results, paging through ``@numpages``.

    The first page reports ``@numpages``; pages 2..N download concurrently
    (bounded by ``PAGE_CONCURRENCY``) by the paginating client.
    """
    paginator = il.PageNumberPaginator(page_param="Page", total_path="@numpages")
    pages = connection.client.paginate(
        f"/Jobs/{job_id}/Download",
        paginator,
        params={"Mode": "stream"},
        headers={"Accept": "application/octet-stream"},
        data_selector=lambda r: r.json().get(key) or [],
        concurrency=PAGE_CONCURRENCY,
    )
    return [row async for page in pages for row in page]


async def _report_export(connection: ImpactConnection, report_id: str, program_id: str, date: dt.date) -> list[_RECORD]:
    """Request a report export, wait for the job, and download its ``Records``."""
    response = await connection.client.get(
        f"/ReportExport/{report_id}",
        params={"SUBAID": program_id, "ResultFormat": "JSON", **_date_range(date)},
    )
    response.raise_for_status()
    job_id = response.json()["QueuedUri"].split("/")[-1]
    await _wait_for_job(connection, job_id)
    return await _download_job(connection, job_id, key="Records")


async def _clicks_export(connection: ImpactConnection, program_id: str, date: dt.date) -> list[_RECORD]:
    """Request a clicks export, wait for the job, and download its ``Clicks``."""
    response = await connection.client.get(
        f"/Programs/{program_id}/ClickExport",
        params={"Date": date.isoformat(), "ResultFormat": "JSON"},
    )
    response.raise_for_status()
    job_id = response.json()["QueuedUri"].split("/")[-1]
    await _wait_for_job(connection, job_id)
    return await _download_job(connection, job_id, key="Clicks")


# -- HELPERS — framing ---------------------------------------------------------
def _to_df(records: list[_RECORD], date: dt.date) -> pd.DataFrame:
    """Build a DataFrame and stamp the partition date.

    Empty-string blanking and column normalization are handled by the source's
    normalizer (``replace_empty_strings``).
    """
    df = pd.DataFrame(records)
    df["date"] = date
    return df


# -- SOURCE --------------------------------------------------------------------
@il.source(
    resources={"connection": ImpactConnection},
    tags=["Affiliate"],
    icon="fluent:connector-24-filled",
    normalizer=DataFrameNormalizer(snake_case_digits=True, replace_empty_strings=True),
)
class Impact(il.Source):
    """Impact.com affiliate and partnership platform integration."""

    program_id: str = il.FetchField(
        provider="connection.programs",
        label_key="Name",
        value_key="Id",
        description="Impact program (campaign) ID",
    )

    def asset_table(self, asset: il.Asset) -> str:
        """Suffix tables with the program_id so instances materialize side by side."""
        return f"{asset.key}__{self.program_id}"

    @il.asset(
        schema=schemas.Actions,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def actions(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Conversion actions including revenue, commissions, and attribution data."""
        records = await _get_actions(connection, self.program_id, context.partition_date)
        return _to_df(records, context.partition_date)

    @il.asset(
        schema=schemas.ActionUpdates,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def action_updates(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Action updates tracking changes to conversion actions over time."""
        records = await _get_action_updates(connection, self.program_id, context.partition_date)
        return _to_df(records, context.partition_date)

    @il.asset(
        schema=schemas.ActionInquiries,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def action_inquiries(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Action inquiries raised against conversion actions."""
        records = await _get_action_inquiries(connection, self.program_id, context.partition_date)
        return _to_df(records, context.partition_date)

    @il.asset(
        schema=schemas.ActionsBySku,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def actions_by_sku(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Conversion actions broken down by product SKU."""
        records = await _report_export(
            connection, "adv_action_list_sku_pm_only", self.program_id, context.partition_date
        )
        return _to_df(records, context.partition_date)

    @il.asset(
        schema=schemas.Clicks,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def clicks(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Click events for the program."""
        records = await _clicks_export(connection, self.program_id, context.partition_date)
        return _to_df(records, context.partition_date)

    @il.asset(
        schema=schemas.PerformanceStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def performance_stats(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Daily performance metrics (clicks, actions, revenue)."""
        records = await _report_export(
            connection, "att_adv_performance_by_day_pm_only", self.program_id, context.partition_date
        )
        return _to_df(records, context.partition_date)

    @il.asset(
        schema=schemas.PerformanceStatsByAd,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def performance_stats_by_ad(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Performance metrics broken down by ad."""
        records = await _report_export(
            connection, "adv_performance_by_ad_pm_only", self.program_id, context.partition_date
        )
        return _to_df(records, context.partition_date)

    @il.asset(
        schema=schemas.PerformanceStatsByDomain,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def performance_stats_by_domain(
        self, context: il.ExecutionContext, connection: ImpactConnection
    ) -> pd.DataFrame:
        """Performance metrics broken down by referring domain."""
        records = await _report_export(
            connection, "att_adv_performance_by_ref_domain_pm_only", self.program_id, context.partition_date
        )
        return _to_df(records, context.partition_date)

    @il.asset(
        schema=schemas.PerformanceStatsBySharedId,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def performance_stats_by_shared_id(
        self, context: il.ExecutionContext, connection: ImpactConnection
    ) -> pd.DataFrame:
        """Performance metrics broken down by shared ID."""
        records = await _report_export(
            connection, "att_adv_performance_by_shared_id_pm_only", self.program_id, context.partition_date
        )
        return _to_df(records, context.partition_date)

    @il.asset(
        schema=schemas.PerformanceStatsByIo,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def performance_stats_by_io(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Performance metrics broken down by insertion order."""
        records = await _report_export(connection, "att_adv_performance_by_IO", self.program_id, context.partition_date)
        return _to_df(records, context.partition_date)
