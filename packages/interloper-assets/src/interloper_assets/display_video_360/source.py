import datetime as dt
import json
import logging
import time
from io import StringIO
from typing import Any

import httpx
import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.display_video_360 import constants, schemas
from interloper_assets.display_video_360.connection import DisplayVideo360Connection

logger = logging.getLogger(__name__)

_Record = dict[str, Any]

# Report queries queue server-side; poll with backoff until done.
_REPORT_TIMEOUT = 3 * 60 * 60  # 3 hours


# -- NORMALIZER ----------------------------------------------------------------
class DisplayVideo360Normalizer(DataFrameNormalizer):
    """Snake-case Bid Manager CSV report headers onto the schema columns.

    Report headers are human-readable ("Active View: % Viewable Impressions",
    "CM360 Placement ID"). The generic pass drops ``%`` outright, which would
    collide "% Viewable Impressions" with "Viewable Impressions" — so spell it
    out as ``pct`` first, then defer to snake-casing with digit splitting
    (CM360 → cm_360). A no-op for the entity assets' camelCase JSON payloads.
    """

    def column_name(self, name: str) -> str:
        return super().column_name(name.replace("%", "pct"))


# -- HELPERS — reports (Bid Manager API) -----------------------------------------
def _report_body(title: str, date: dt.date, dimensions: list[str], metrics: list[str], filters: list[dict]) -> dict:
    return {
        "metadata": {
            "title": title,
            "dataRange": {
                "range": "CUSTOM_DATES",
                "customStartDate": {"year": date.year, "month": date.month, "day": date.day},
                "customEndDate": {"year": date.year, "month": date.month, "day": date.day},
            },
            "format": "CSV",
        },
        "params": {
            "type": "STANDARD",
            "groupBys": dimensions,
            "filters": filters,
            "metrics": metrics,
            "options": {},
        },
        "schedule": {"frequency": "ONE_TIME"},
    }


def _wait_for_report(dbm_client: Any, query_id: str, report_id: str) -> dict:
    """Poll a report until it reaches a terminal state; return its final payload."""
    deadline = time.monotonic() + _REPORT_TIMEOUT
    interval = float(constants.MIN_RETRY_INTERVAL)
    while True:
        logger.info(f"Waiting for report {report_id} (query {query_id})...")
        report = dbm_client.queries().reports().get(queryId=query_id, reportId=report_id).execute()
        state = report["metadata"]["status"]["state"]

        if state == "DONE":
            return report
        if state == "FAILED":
            raise RuntimeError(f"Report {report_id} (query {query_id}) failed")

        if time.monotonic() >= deadline:
            raise RuntimeError(f"Report {report_id} did not complete within {_REPORT_TIMEOUT}s")
        time.sleep(interval)
        interval = min(interval * 2, constants.MAX_RETRY_INTERVAL)


def _parse_report_csv(text: str) -> list[_Record]:
    """Parse a Bid Manager CSV report, dropping the trailing summary footer.

    The footer (totals plus query metadata) starts at the first line whose
    leading field is empty. Empty reports carry a "No data returned by the
    reporting service." marker row instead of data.
    """
    lines = []
    for line in text.split("\n"):
        if line.startswith(","):
            break
        lines.append(line)
    df = pd.read_csv(StringIO("\n".join(lines)), na_values=["-"], on_bad_lines="skip")
    if df.empty:
        return []
    first = df.iloc[:, 0].astype("string")
    if first.str.contains("No data returned by the reporting service.", na=False).any():
        return []
    return df.to_dict("records")


def _get_report(
    connection: DisplayVideo360Connection,
    title: str,
    date: dt.date,
    dimensions: list[str],
    metrics: list[str],
    filters: list[dict],
) -> list[_Record]:
    """Create, run, download, and parse a report query, deleting it after."""
    dbm_client = connection.dbm_client
    query_id = dbm_client.queries().create(body=_report_body(title, date, dimensions, metrics, filters)).execute()[
        "queryId"
    ]
    logger.info(f"Query id: {query_id} ({title})")
    try:
        report_id = dbm_client.queries().run(queryId=query_id, synchronous=False).execute()["key"]["reportId"]
        report = _wait_for_report(dbm_client, query_id, report_id)
        url = report["metadata"]["googleCloudStoragePath"]
        response = httpx.get(url, timeout=None)
        response.raise_for_status()
        return _parse_report_csv(response.content.decode("utf-8-sig"))
    finally:
        # One-shot queries; clean up best-effort so they don't pile up.
        try:
            dbm_client.queries().delete(queryId=query_id).execute()
        except Exception as exc:
            logger.warning(f"Failed to delete query {query_id}: {exc}")


# -- HELPERS — entities (Display & Video API) ------------------------------------
def _list_audiences(dv_client: Any, scope: dict[str, str]) -> list[_Record]:
    """Page through the first-party/partner audiences of a partner or advertiser."""
    audiences: list[_Record] = []
    page_token: str | None = None
    while True:
        response = dv_client.firstPartyAndPartnerAudiences().list(**scope, pageToken=page_token).execute()
        audiences.extend(response.get("firstPartyAndPartnerAudiences") or [])
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return audiences


# -- SOURCE --------------------------------------------------------------------
@il.source(
    key="display_video_360",
    resources={
        "connection": DisplayVideo360Connection,
    },
    tags=["Advertising"],
    icon="icon:dv360",
    # Partner objects nest config two levels deep (dataAccessConfig.sdfConfig.*);
    # digit splitting maps dv360ToCm... -> dv_360_to_cm_... onto the schema fields.
    normalizer=DisplayVideo360Normalizer(snake_case_digits=True, flatten_max_level=2),
)
class DisplayVideo360(il.Source):
    """Display & Video 360 advertising platform integration."""

    partner_id: str = il.FetchField(
        label="Partner ID",
        description="DV360 partner",
        provider="connection.partners",
        label_key="name",
        value_key="partner_id",
        discriminator=True,
    )
    advertiser_id: str = il.InputField(
        default="",
        label="Advertiser ID",
        description=(
            "DV360 advertiser ID — narrows report assets to one advertiser and "
            "scopes audience assets by advertiser instead of partner"
        ),
    )
    audience_id: str = il.InputField(
        default="",
        label="Audience ID",
        description="Audience ID (only required for the custom_audiences asset)",
    )

    @property
    def _audience_scope(self) -> dict[str, str]:
        """Audience calls take a partner XOR advertiser scope; advertiser wins when set."""
        if self.advertiser_id:
            return {"advertiserId": self.advertiser_id}
        return {"partnerId": self.partner_id}

    @property
    def _report_filters(self) -> list[dict[str, str]]:
        """Report filters: always the partner, narrowed by advertiser when set."""
        filters = [{"type": "FILTER_PARTNER", "value": self.partner_id}]
        if self.advertiser_id:
            filters.append({"type": "FILTER_ADVERTISER", "value": self.advertiser_id})
        return filters

    # --- Reports (Bid Manager API) ---

    @il.asset(
        schema=schemas.LineItemsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def line_items_stats(
        self, context: il.ExecutionContext, connection: DisplayVideo360Connection
    ) -> list[_Record]:
        """Line-item performance, cost and fee metrics per day."""
        return _get_report(
            connection,
            title=f"line_items_stats_{context.partition_date.isoformat()}",
            date=context.partition_date,
            dimensions=constants.LINE_ITEM_DIMENSIONS,
            metrics=constants.LINE_ITEM_METRICS,
            filters=self._report_filters,
        )

    @il.asset(
        schema=schemas.LineItemsStatsByCountry,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def line_items_stats_by_country(
        self, context: il.ExecutionContext, connection: DisplayVideo360Connection
    ) -> list[_Record]:
        """Line-item performance and cost metrics per day, broken down by country."""
        return _get_report(
            connection,
            title=f"line_items_stats_by_country_{context.partition_date.isoformat()}",
            date=context.partition_date,
            dimensions=constants.LINE_ITEM_COUNTRY_DIMENSIONS,
            metrics=constants.LINE_ITEM_COUNTRY_METRICS,
            filters=self._report_filters,
        )

    # --- Entities (Display & Video API) ---

    @il.asset(
        schema=schemas.Partners,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
    )
    def partners(self, context: il.ExecutionContext, connection: DisplayVideo360Connection) -> list[_Record]:
        """DV360 partners and their configuration."""
        items = connection._list_partners()
        for item in items:
            exchange_config = item.get("exchangeConfig")
            # Enabled exchanges are a nested list; JSON-encode onto the string column.
            if isinstance(exchange_config, dict) and isinstance(exchange_config.get("enabledExchanges"), list):
                exchange_config["enabledExchanges"] = json.dumps(exchange_config["enabledExchanges"])
        return [{**item, "date": context.partition_date} for item in items]

    @il.asset(
        schema=schemas.Audiences,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
    )
    def audiences(self, context: il.ExecutionContext, connection: DisplayVideo360Connection) -> list[_Record]:
        """First-party and partner audiences of the configured partner or advertiser."""
        rows = _list_audiences(connection.dv_client, self._audience_scope)
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.CustomAudiences,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
    )
    def custom_audiences(
        self, context: il.ExecutionContext, connection: DisplayVideo360Connection
    ) -> list[_Record]:
        """Detail of the configured audience, including per-surface audience sizes."""
        if not self.audience_id:
            raise ValueError("The custom_audiences asset requires the source's audience_id to be set")
        audience = (
            connection.dv_client.firstPartyAndPartnerAudiences()
            .get(**self._audience_scope, firstPartyAndPartnerAudienceId=self.audience_id)
            .execute()
        )
        return [{**audience, "date": context.partition_date}]
