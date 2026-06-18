import datetime as dt
from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.adup import constants
from interloper_assets.adup.connection import AdupConnection
from interloper_assets.adup.schemas import Account, AdsStats


def get_report(client: il.RESTClient, report_type: str, start_date: dt.date, end_date: dt.date) -> dict:
    """Fetch a report from the Adup API.

    Returns:
        The parsed JSON response.
    """
    response = client.post(
        "/reports/v202101/report",
        json={
            "report_name": report_type,
            "report_type": report_type,
            "select": constants.FIELDS[report_type],
            "conditions": [],
            "download_format": "JSON",
            "date_range_type": "CUSTOM_DATE",
            "date_range": {
                "min": start_date.isoformat(),
                "max": end_date.isoformat(),
            },
        },
    )
    response.raise_for_status()
    return response.json()


@il.source(
    resources={"connection": AdupConnection},
    tags=["Advertising"],
    icon="icon:adup",
    # The AdWords-style report returns PascalCase columns (Date, AdgroupId, …);
    # snake-case them onto the schema.
    normalizer=DataFrameNormalizer(),
)
class Adup(il.Source):
    """Adup advertising platform integration."""

    @il.asset(
        tags=["Entity"],
        schema=Account,
    )
    def account(self, connection: AdupConnection) -> list[dict[str, Any]]:
        """Advertiser account information."""
        response = connection.client.get("/advertisers/me")
        response.raise_for_status()
        return [response.json()]

    @il.asset(
        partitioning=il.TimePartitionConfig(column="date", allow_window=True),
        tags=["Report"],
        schema=AdsStats,
    )
    def ads_stats(self, context: il.ExecutionContext, connection: AdupConnection) -> list[dict[str, Any]]:
        """Ad performance insights with metrics like impressions, clicks, conversions, and cost."""
        start_date, end_date = context.partition_date_window
        data = get_report(connection.client, "AD_PERFORMANCE_REPORT", start_date, end_date)
        return data["rows"]
