import datetime as dt
import json
import logging
import re
from collections.abc import Callable
from time import sleep
from typing import Any

import interloper as il
import pandas as pd
import tenacity as tc
from interloper_pandas import DataFrameNormalizer

from interloper_assets.facebook_ads import constants
from interloper_assets.facebook_ads.connection import FacebookAdsConnection
from interloper_assets.facebook_ads.connector import schemas

logger = logging.getLogger(__name__)

# Insight columns whose cells are lists of ``{action_type, value}``; each is
# pivoted into one column per action type (``actions`` -> ``actions_link_click``).
PIVOT_COLUMNS = [
    "action_values",
    "actions",
    "conversions",
    "cost_per_action_type",
    "cost_per_conversion",
    "cost_per_outbound_click",
    "cost_per_unique_action_type",
    "cost_per_unique_outbound_click",
    "outbound_clicks_ctr",
    "outbound_clicks",
    "unique_actions",
    "unique_outbound_clicks_ctr",
    "unique_outbound_clicks",
    "video_30_sec_watched_actions",
    "video_avg_time_watched_actions",
    "video_p100_watched_actions",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p95_watched_actions",
    "video_play_actions",
    "website_ctr",
]

# Shared insights breakdowns for the ad/campaign/video reports.
_PLATFORM_BREAKDOWNS = ["publisher_platform", "platform_position", "impression_device"]
_ACTION_BREAKDOWNS = ["action_device", "action_type"]

# Facebook rate-limit error codes (user request limit / app throttling).
_RATE_LIMIT_CODES = (17, 80004)

# Async insights reports can run long (they often sit at "Job Running 99%" for
# minutes before completing), so poll generously before giving up.
INSIGHTS_TIMEOUT = dt.timedelta(hours=1)


# -- HELPERS: flattening -------------------------------------------------------
def _sanitize(name: str) -> str:
    """Lower-case a column name and collapse non-alphanumeric runs to ``_``."""
    return re.sub(r"[^0-9a-zA-Z]+", "_", str(name)).strip("_").lower()


def _pivot_cell(column: str, cell: Any) -> dict[str, float]:
    """Pivot a list of ``{action_type, value}`` into ``{column_actiontype: value}``.

    Values are summed across the action-device breakdown so each action type
    yields a single column, mirroring the reference reporter.
    """
    out: dict[str, float] = {}
    if isinstance(cell, list):
        for entry in cell:
            action_type = entry.get("action_type")
            if action_type is None:
                continue
            value = pd.to_numeric(entry.get("value"), errors="coerce")
            if pd.isna(value):
                continue
            key = f"{column}_{action_type}"
            out[key] = out.get(key, 0.0) + float(value)
    return out


class FacebookActionsNormalizer(DataFrameNormalizer):
    """Pivot Facebook insight action-lists into per-action-type columns.

    Each ``actions``/``action_values``/… cell is a list of ``{action_type,
    value}``; pivot it into one ``<column>_<action_type>`` column (summed across
    the action-device breakdown), sanitize the names, coerce custom-conversion
    columns to float, then defer to the base ``DataFrameNormalizer`` to flatten
    nested entity dicts (``creative``) and snake-case the rest.
    """

    def normalize(self, data: Any) -> pd.DataFrame:
        """Reshape the vendor payload before the standard normalization.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame.
        """
        df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
        if df.empty:
            return df

        for column in PIVOT_COLUMNS:
            if column not in df.columns:
                continue
            cells = df[column].map(lambda cell, column=column: _pivot_cell(column, cell))
            pivoted = pd.DataFrame(list(cells), index=df.index)
            df = pd.concat([df.drop(columns=[column]), pivoted], axis=1)

        df.columns = [_sanitize(c) for c in df.columns]

        # Custom-conversion columns are not in the schema; coerce to float so
        # mixed int/NaN values don't surface as objects.
        for column in df.columns:
            if re.match(r".*_custom(_.+)?$", column):
                df[column] = pd.to_numeric(df[column], errors="coerce").astype(float)

        return super().normalize(df)


# -- HELPERS: SDK calls --------------------------------------------------------
def _account(connection: FacebookAdsConnection, account_id: str) -> Any:
    from facebook_business.adobjects.adaccount import AdAccount

    return AdAccount(f"act_{account_id}", api=connection.api)


def _is_rate_limit_error(exc: BaseException) -> bool:
    from facebook_business.exceptions import FacebookRequestError

    return isinstance(exc, FacebookRequestError) and exc.api_error_code() in _RATE_LIMIT_CODES


_rate_limit_retry = tc.retry(
    retry=tc.retry_if_exception(_is_rate_limit_error),
    wait=tc.wait_random_exponential(multiplier=2, min=5, max=300),
    stop=tc.stop_after_attempt(5),
    reraise=True,
)


def _insights_usage(headers: dict[str, str], account_id: str) -> int:
    """Read the worst-case rate-limit usage percentage from insights headers."""
    if "x-app-usage" in headers:
        business_limit = json.loads(headers["x-app-usage"])
    elif "x-business-use-case-usage" in headers:
        business_limit = json.loads(headers["x-business-use-case-usage"])[account_id][0]
    else:
        logger.warning("Rate limit headers are missing; cannot check rate limits.")
        return 0

    throttle = json.loads(headers["x-fb-ads-insights-throttle"])
    return max(
        business_limit["call_count"],
        business_limit["total_cputime"],
        business_limit["total_time"],
        throttle["app_id_util_pct"],
    )


def _insights_headers(account: Any) -> dict[str, str]:
    from facebook_business.exceptions import FacebookRequestError

    @tc.retry(
        retry=tc.retry_if_exception_type(FacebookRequestError),
        wait=tc.wait_fixed(30),
        stop=tc.stop_after_attempt(3),
        reraise=True,
    )
    def _fetch() -> dict[str, str]:
        return account.get_insights().headers()

    return _fetch()


def _wait_for_job(job: Any, timeout: dt.timedelta = INSIGHTS_TIMEOUT) -> None:
    """Poll an async report run until it completes.

    Raises:
        FacebookError: If the report reports a failed status.
        TimeoutError: If it does not complete within *timeout*.
    """
    from facebook_business.adobjects.adreportrun import AdReportRun
    from facebook_business.exceptions import FacebookError

    last_status: dict[str, Any] = {}
    try:
        for attempt in tc.Retrying(
            wait=tc.wait_fixed(10),
            stop=tc.stop_after_delay(timeout),
            before_sleep=tc.before_sleep_log(logger, logging.DEBUG),
            reraise=True,
            retry=tc.retry_if_result(lambda s: s[AdReportRun.Field.async_status] != "Job Completed"),
        ):
            with attempt:
                last_status = job.api_get()._json
                logger.info(
                    f"Waiting for report... ({last_status[AdReportRun.Field.async_status]} "
                    f"{last_status[AdReportRun.Field.async_percent_completion]}%)"
                )
                if last_status[AdReportRun.Field.async_status] == "Job Failed":
                    raise FacebookError("Facebook insights report failed")
            if attempt.retry_state.outcome and not attempt.retry_state.outcome.failed:
                attempt.retry_state.set_result(last_status)
    except tc.RetryError as exc:
        raise TimeoutError(
            f"Facebook insights report did not complete within {timeout} "
            f"(last status: {last_status.get(AdReportRun.Field.async_status)} "
            f"{last_status.get(AdReportRun.Field.async_percent_completion)}%)"
        ) from exc


def _get_ads_insights(
    connection: FacebookAdsConnection,
    account_id: str,
    fields: list[str],
    params: dict,
) -> list[dict]:
    """Run an async insights report, waiting out rate limits, and return rows."""
    account = _account(connection, account_id)

    usage = _insights_usage(_insights_headers(account), account_id)
    while usage > 80:
        wait_time = max(usage * 2, 150)
        logger.info(f"Rate-limit usage at {usage}%; sleeping {wait_time}s before requesting insights...")
        sleep(wait_time)
        usage = _insights_usage(_insights_headers(account), account_id)

    job = account.get_insights(fields=fields, params=params, is_async=True)
    _wait_for_job(job)
    return [item._json for item in job.get_result()]


def _get_custom_audiences(
    connection: FacebookAdsConnection,
    account_id: str,
    fields: list[str],
    params: dict,
) -> list[dict]:
    account = _account(connection, account_id)
    return [audience._json for audience in account.get_custom_audiences(fields=fields, params=params)]


def _get_campaigns(connection: FacebookAdsConnection, account_id: str) -> list[dict]:
    account = _account(connection, account_id)
    return [campaign._json for campaign in account.get_campaigns(fields=constants.CAMPAIGN_ENTITY_FIELDS)]


def _asks_for_less(exc: BaseException) -> bool:
    from facebook_business.exceptions import FacebookRequestError

    return (
        isinstance(exc, FacebookRequestError)
        and exc.api_error_code() == 1
        and "reduce the amount of data" in (exc.api_error_message() or "")
    )


def _paginate(request: Callable[..., Any], fields: list[str], limit: int = 25) -> list[dict]:
    """Read every page of an account edge, retrying each page call on rate limits.

    Used by the ``ads`` and ``adsets`` entity assets, the two edges large
    enough to page and to hit the rate limit. Indexing an SDK cursor reads its
    current page without consuming it, and ``load_next_page`` replaces the
    page, so each page is read by position.

    Facebook refuses a page whose objects carry too much data with a 500
    asking for less; the edge is then read again from the start with half the
    page, since an entity read is a snapshot and a restart loses nothing.

    Args:
        request: The account's edge method, such as ``AdAccount.get_ads``.
        fields: The fields to request on each object.
        limit: Objects per page; halved on every refusal down to one.

    Returns:
        Every object's data, in the order the API returned them.
    """
    try:
        cursor = _rate_limit_retry(request)(fields=fields, params={"limit": limit})
        rows: list[dict] = []
        while True:
            rows.extend(cursor[i].export_all_data() for i in range(len(cursor)))
            if not _rate_limit_retry(cursor.load_next_page)():
                return rows
    except Exception as exc:
        if limit == 1 or not _asks_for_less(exc):
            raise
        logger.warning(f"Facebook asked for less data at {limit} objects per page; retrying with {limit // 2}")
        return _paginate(request, fields, limit // 2)


def _get_ads(connection: FacebookAdsConnection, account_id: str) -> list[dict]:
    return _paginate(_account(connection, account_id).get_ads, constants.AD_ENTITY_FIELDS)


def _get_adsets(connection: FacebookAdsConnection, account_id: str) -> list[dict]:
    return _paginate(_account(connection, account_id).get_ad_sets, constants.ADSET_ENTITY_FIELDS)


def _get_ad_account(connection: FacebookAdsConnection, account_id: str) -> dict:
    return _read_fields(_account(connection, account_id), constants.AD_ACCOUNT_ENTITY_FIELDS)


def _read_fields(node: Any, fields: list[str]) -> dict:
    """Read a single Graph API object's fields, retrying on rate limits.

    Used by the ``ad_accounts`` entity asset. Facebook refuses an object whose
    fields carry too much data with the same 500 asking for less that it
    gives a heavy page, and an object has no page to shrink: its fields are
    read again in two halves and the answers merged, halving until each read
    fits, so every field is still read.

    Args:
        node: The SDK object to read, such as an ``AdAccount``.
        fields: The fields to request; split in halves on every refusal down to one.

    Returns:
        The object's data for every requested field it carries.
    """
    try:
        return _rate_limit_retry(node.api_get)(fields=fields).export_all_data()
    except Exception as exc:
        if len(fields) == 1 or not _asks_for_less(exc):
            raise
    middle = len(fields) // 2
    logger.warning(f"Facebook asked for less data reading {len(fields)} fields; reading them in two halves")
    return {**_read_fields(node, fields[:middle]), **_read_fields(node, fields[middle:])}


def _time_range(date: dt.date) -> dict[str, str]:
    return {"since": date.isoformat(), "until": date.isoformat()}


# Entity nodes nest objects (targeting, creative, business) that the source normalizer would
# flatten into open-ended columns, and flattening business.name would overwrite the vendor's own
# business_name; the entity assets keep every object whole as JSON under the vendor's name.
_ENTITY_NORMALIZER = FacebookActionsNormalizer()


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    icon="logos:facebook",
    normalizer=FacebookActionsNormalizer(flatten_max_level=1),
)
class FacebookAds(il.Source):
    """Facebook Ads (Meta Marketing) advertising platform integration."""

    connection: FacebookAdsConnection

    account_id: str = il.FetchField(
        provider="connection.accounts",
        label_key="name",
        value_key="account_id",
        description="Facebook Ads account ID",
        discriminator=True,
    )

    @il.asset(
        schema=schemas.CampaignsStats,
        partitioning=il.TimePartitionConfig(column="date_start"),
        tags=["Report"],
    )
    def campaigns_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Campaign-level performance insights with metrics, engagement, and cost breakdowns."""
        rows = _get_ads_insights(
            self.connection,
            self.account_id,
            fields=constants.CAMPAIGNS_FIELDS,
            params={
                "level": "campaign",
                "time_range": _time_range(context.partition_date),
                "breakdowns": _PLATFORM_BREAKDOWNS,
                "action_breakdown": _ACTION_BREAKDOWNS,
            },
        )
        return rows

    @il.asset(
        schema=schemas.AdsStats,
        partitioning=il.TimePartitionConfig(column="date_start"),
        tags=["Report"],
    )
    def ads_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad-level performance insights with platform, device, and action breakdowns."""
        rows = _get_ads_insights(
            self.connection,
            self.account_id,
            fields=constants.ADS_INSIGHT_FIELDS,
            params={
                "level": "ad",
                "time_range": _time_range(context.partition_date),
                "breakdowns": _PLATFORM_BREAKDOWNS,
                "action_breakdown": _ACTION_BREAKDOWNS,
            },
        )
        return rows

    @il.asset(
        schema=schemas.AdsStatsByAgeGender,
        partitioning=il.TimePartitionConfig(column="date_start"),
        tags=["Report"],
    )
    def ads_stats_by_age_gender(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad-level performance insights broken down by age and gender demographics."""
        rows = _get_ads_insights(
            self.connection,
            self.account_id,
            fields=constants.GENDER_AGE_FIELDS,
            params={
                "level": "ad",
                "time_range": _time_range(context.partition_date),
                "breakdowns": ["gender", "age"],
                "action_breakdown": _ACTION_BREAKDOWNS,
            },
        )
        return rows

    @il.asset(
        schema=schemas.AdsStatsByCountry,
        partitioning=il.TimePartitionConfig(column="date_start"),
        tags=["Report"],
    )
    def ads_stats_by_country(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad-level performance insights broken down by country and region."""
        rows = _get_ads_insights(
            self.connection,
            self.account_id,
            fields=constants.GEO_FIELDS,
            params={
                "level": "ad",
                "time_range": _time_range(context.partition_date),
                "breakdowns": ["country", "region"],
                "action_breakdown": _ACTION_BREAKDOWNS,
            },
        )
        return rows

    @il.asset(
        schema=schemas.VideosStats,
        partitioning=il.TimePartitionConfig(column="date_start"),
        tags=["Report"],
    )
    def videos_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Video ad performance with view retention, engagement, and conversion metrics."""
        rows = _get_ads_insights(
            self.connection,
            self.account_id,
            fields=constants.VIDEOS_FIELDS,
            params={
                "level": "ad",
                "time_range": _time_range(context.partition_date),
                "breakdowns": _PLATFORM_BREAKDOWNS,
                "action_breakdown": _ACTION_BREAKDOWNS,
            },
        )
        return rows

    @il.asset(
        schema=schemas.CustomAudiences,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
    )
    def custom_audiences(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Custom audiences with approximate size bounds for the account."""
        rows = _get_custom_audiences(
            self.connection,
            self.account_id,
            fields=constants.CUSTOM_AUDIENCES_FIELDS,
            params={},
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.Ads,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    def ads(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad entities including creative, status, bidding and targeting."""
        return [{**row, "date": context.partition_date} for row in _get_ads(self.connection, self.account_id)]

    @il.asset(
        schema=schemas.Campaigns,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    def campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Campaign entities including objective, budget, and status configuration."""
        return [{**row, "date": context.partition_date} for row in _get_campaigns(self.connection, self.account_id)]

    @il.asset(
        schema=schemas.AdAccounts,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    def ad_accounts(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """The ad account with its name, currency, timezone, status and billing settings."""
        return [{**_get_ad_account(self.connection, self.account_id), "date": context.partition_date}]

    @il.asset(
        schema=schemas.Adsets,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    def adsets(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad set entities including schedule, budget, bidding, learning stage and targeting."""
        return [{**row, "date": context.partition_date} for row in _get_adsets(self.connection, self.account_id)]
