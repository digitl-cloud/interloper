import asyncio
import csv
import datetime as dt
import logging
from typing import Any

import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.thetradedesk import constants, schemas
from interloper_assets.thetradedesk.connection import TheTradeDeskConnection

logger = logging.getLogger(__name__)

_Record = dict[str, Any]

# Report executions queue server-side; poll with backoff until complete.
_POLL_START_INTERVAL = 60.0
_POLL_MAX_INTERVAL = 300.0
_REPORT_TIMEOUT = 3 * 60 * 60  # 3 hours


# -- NORMALIZER ----------------------------------------------------------------
def _decimal_comma_to_float(value: Any) -> Any:
    """Convert a decimal-comma numeric string ("1,5") to a float; pass the rest through."""
    if isinstance(value, str) and "," in value:
        try:
            return float(value.replace(",", "."))
        except ValueError:
            return value
    return value


class TheTradeDeskNormalizer(DataFrameNormalizer):
    """Reshape MyReports CSV rows onto the schema columns.

    Reports are requested in "International" format, so metric values carry
    decimal commas ("1,5") and the date column is DD/MM/YYYY — convert both so
    the generic numeric/date casts succeed. Headers also spell quartiles with
    ``%`` ("Player 25% Complete" → ``player_25pct_complete``), which the generic
    snake-casing would drop.
    """

    def column_name(self, name: str) -> str:
        return super().column_name(name.replace("%", "pct"))

    def normalize(self, data: Any) -> pd.DataFrame:
        df = super().normalize(data)
        if df.empty:
            return df
        object_columns = df.select_dtypes(include="object").columns
        df[object_columns] = df[object_columns].map(_decimal_comma_to_float)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y").dt.date
        return df


# -- HELPERS -------------------------------------------------------------------
async def _create_report_schedule(
    connection: TheTradeDeskConnection, template_id: str, partner_id: str, date: dt.date
) -> str:
    """Create a one-time report schedule and return its id."""
    response = await connection.client.post(
        "/myreports/reportschedule",
        json={
            "RequestedByUserName": "interloper",
            "IncludeHeaders": True,
            "PartnerFilters": [partner_id],
            "ReportDateFormat": "International",
            "ReportDateRange": "Custom",
            "ReportStartDateInclusive": date.isoformat(),
            "ReportEndDateExclusive": (date + dt.timedelta(days=1)).isoformat(),
            "ReportFileFormat": "CSV",
            "ReportFrequency": "Once",
            "ReportNumericFormat": "International",
            "ReportScheduleName": f"interloper_{template_id}_{date.isoformat()}",
            "ReportTemplateId": template_id,
            "TimeZone": "UTC",
        },
    )
    response.raise_for_status()
    return str(response.json()["ReportScheduleId"])


async def _wait_for_execution(connection: TheTradeDeskConnection, partner_id: str, schedule_id: str) -> dict:
    """Poll the schedule's execution until it completes; return the execution."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _REPORT_TIMEOUT
    interval = _POLL_START_INTERVAL
    while True:
        logger.info(f"Waiting for report schedule {schedule_id}...")
        response = await connection.client.post(
            "/myreports/reportexecution/query/partners",
            json={
                "PageStartIndex": 0,
                "PageSize": 100,
                "PartnerIds": [partner_id],
                "ReportScheduleIds": [schedule_id],
            },
        )
        response.raise_for_status()
        executions = response.json().get("Result") or []

        if executions:
            if len(executions) > 1:
                logger.warning(f"Schedule {schedule_id} has multiple executions; using the first")
            execution = executions[0]
            state = execution["ReportExecutionState"]
            if state == "Complete":
                return execution
            if state in ("Failed", "Error"):
                raise RuntimeError(f"Report schedule {schedule_id} failed with state {state}")

        if loop.time() >= deadline:
            raise RuntimeError(f"Report schedule {schedule_id} did not complete within {_REPORT_TIMEOUT:.0f}s")
        await asyncio.sleep(interval)
        interval = min(interval * 2, _POLL_MAX_INTERVAL)


async def _download_report(connection: TheTradeDeskConnection, url: str) -> list[_Record]:
    """Download a report delivery and parse its CSV rows (values stay strings)."""
    response = await connection.client.get(url, follow_redirects=True)
    response.raise_for_status()
    return list(csv.DictReader(response.text.splitlines()))


async def _get_report(
    connection: TheTradeDeskConnection, template_id: str, partner_id: str, date: dt.date
) -> list[_Record]:
    """Schedule, wait for, and download a one-time report, deleting the schedule after."""
    schedule_id = await _create_report_schedule(connection, template_id, partner_id, date)
    logger.info(f"Report schedule id: {schedule_id} (template {template_id})")
    try:
        execution = await _wait_for_execution(connection, partner_id, schedule_id)
        deliveries = execution["ReportDeliveries"]
        if len(deliveries) > 1:
            logger.warning(f"Schedule {schedule_id} has multiple deliveries; using the first")
        return await _download_report(connection, deliveries[0]["DownloadURL"])
    finally:
        # One-shot schedules; clean up best-effort so they don't pile up.
        try:
            response = await connection.client.delete(f"/myreports/reportschedule/{schedule_id}")
            response.raise_for_status()
        except Exception as exc:
            logger.warning(f"Failed to delete report schedule {schedule_id}: {exc}")


# -- SOURCE --------------------------------------------------------------------
@il.source(
    resources={"connection": TheTradeDeskConnection},
    tags=["Advertising"],
    icon="icon:thetradedesk",
    normalizer=TheTradeDeskNormalizer(replace_empty_strings=True),
)
class TheTradeDesk(il.Source):
    """The Trade Desk programmatic advertising platform integration."""

    partner_id: str = il.FetchField(
        label="Partner ID",
        description="The Trade Desk partner",
        provider="connection.partners",
        label_key="name",
        value_key="partner_id",
        discriminator=True,
    )

    @il.asset(
        schema=schemas.AdGroupsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def ad_groups_stats(
        self, context: il.ExecutionContext, connection: TheTradeDeskConnection
    ) -> list[_Record]:
        """Ad-group performance metrics per day, including video-player and conversion metrics."""
        return await _get_report(
            connection,
            template_id=constants.ADGROUPS_REPORT_TEMPLATE_ID,
            partner_id=self.partner_id,
            date=context.partition_date,
        )
