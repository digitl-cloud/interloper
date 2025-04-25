import datetime as dt
import json
import logging
import re
from time import sleep
from typing import Any

import interloper as itlp
import pandas as pd
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.api import FacebookAdsApi
from facebook_business.exceptions import FacebookError, FacebookRequestError
from interloper_pandas import DataframeNormalizer
from tenacity import (
    Retrying,
    before_sleep_log,
    retry,
    retry_if_exception_type,
    retry_if_result,
    stop_after_attempt,
    stop_after_delay,
    wait_fixed,
)

from interloper_assets.facebook_ads import constants

logger = logging.getLogger(__name__)


class FacebookAdsDataframeNormalizer(DataframeNormalizer):
    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        pivot_columns = [
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
        for column in pivot_columns:
            if column in df.columns:
                df = self.unnest_and_pivot(df=df, column=column, pivot="action_type", values="value")

        df = super().normalize(df)

        # Convert columns with "_custom" suffix to numeric, which are not included in the schema
        # Example matches:
        # - actions_offsite_conversion_fb_pixel_custom
        # - cost_per_action_type_offsite_conversion_custom_856937251479437
        # - conversions_offsite_conversion_fb_pixel_custom_microdata
        for column in df.columns:
            if re.match(r".*_custom(_.+)?$", column):
                df[column] = pd.to_numeric(df[column], errors="coerce")

        return df

    # TODO: move this to DataframeNormalizer?
    def unnest_and_pivot(
        self,
        df: pd.DataFrame,
        column: str,
        pivot: str,
        values: str | list[str],
        sep: str = "_",
        fill_value: Any | None = None,
        sort_pivoted_columns: bool = False,
    ) -> pd.DataFrame:
        # Unnest & pivot
        pivoted = df[column].explode().apply(pd.Series).pivot(columns=pivot, values=values)  # type: ignore

        # Clean up
        pivoted = pivoted.dropna(axis=1, how="all")
        if fill_value is not None:
            pivoted = pivoted.fillna(fill_value)

        # Rename columns
        if pivoted.columns.nlevels > 1:
            pivoted.columns = pivoted.columns.map(lambda c: sep.join(reversed(c)))
        pivoted = pivoted.add_prefix(f"{column}{sep}")

        # Sort columns
        if sort_pivoted_columns:
            pivoted = pivoted.reindex(sorted(pivoted.columns), axis=1)

        # Merge
        index = df.columns.get_loc(column)
        df = pd.concat([df.iloc[:, :index], pivoted, df.iloc[:, index:]], axis=1)
        df = df.drop(columns=column)

        return df


@itlp.source(normalizer=FacebookAdsDataframeNormalizer())
def facebook_ads(
    access_token: str = itlp.Env("FACEBOOK_ADS_ACCESS_TOKEN"),
    app_id: str = itlp.Env("FACEBOOK_ADS_APP_ID"),
    app_secret: str = itlp.Env("FACEBOOK_ADS_APP_SECRET"),
) -> tuple[itlp.Asset, ...]:
    FacebookAdsApi.init(app_id, app_secret, access_token)

    def _check_rate_limit(headers: dict[str, str], account_id: str) -> int:
        business_limit = json.loads(headers["x-business-use-case-usage"])[account_id][0]
        rate_limit = json.loads(headers["x-fb-ads-insights-throttle"])
        logger.debug(f"Call count for {account_id}: {business_limit['call_count']}")
        logger.debug(f"Total CPU Time for {account_id}: {business_limit['total_cputime']}")
        logger.debug(f"Total Time for {account_id}: {business_limit['total_time']}")
        logger.debug(f"Allocated capacity for app_id: {rate_limit['app_id_util_pct']}")
        # Seems to be obsolete after the facebook update
        # logger.info(f"Allocated capacity for account_id: {rate_limit['acc_id_util_pct']}")
        usage = max(
            business_limit["call_count"],
            business_limit["total_cputime"],
            business_limit["total_time"],
            rate_limit["app_id_util_pct"],
            # rate_limit["acc_id_util_pct"],
        )
        return usage

    def _wait_for_job_completion(job: AdReportRun) -> None:
        for attempt in Retrying(
            wait=wait_fixed(10),
            stop=stop_after_delay(dt.timedelta(minutes=10)),
            before_sleep=before_sleep_log(logger, logging.DEBUG),
            reraise=True,
            retry=retry_if_result(lambda job_status: job_status[AdReportRun.Field.async_status] != "Job Completed"),
        ):
            with attempt:
                job_status = job.api_get()._json  # type: ignore
                logger.info(
                    f"Waiting... ({job_status[AdReportRun.Field.async_status]} "
                    f"{job_status[AdReportRun.Field.async_percent_completion]}%)"
                )

                if job_status[AdReportRun.Field.async_status] == "Job Failed":
                    logger.info("Report has failed")
                    raise FacebookError("Report has failed")

            if attempt.retry_state.outcome and not attempt.retry_state.outcome.failed:
                attempt.retry_state.set_result(job_status)

    def _get_headers(account: AdAccount) -> dict[str, str]:
        @retry(
            retry=retry_if_exception_type(FacebookRequestError),
            wait=wait_fixed(30),
            stop=stop_after_attempt(3),
        )
        def get_insights_headers() -> dict[str, str]:
            insights = account.get_insights()
            return insights.headers()  # type: ignore

        try:
            return get_insights_headers()  # type: ignore
        except FacebookRequestError as e:
            logger.error(f"Facebook API request failed: {e}")
            raise e

    def get_ads_insights(account_id: str, fields: list[str], params: dict = {}) -> list[dict]:
        logger.info(f"Fetching report for account {account_id}...")
        account = AdAccount(f"act_{account_id}")
        headers = _get_headers(account=account)
        job: AdReportRun = account.get_insights(fields=fields, params=params, is_async=True)  # type: ignore

        usage = _check_rate_limit(headers=headers, account_id=account_id)
        while usage > 80:
            wait_time = max(usage * 2, 150)
            logger.info(f"Sleep for {wait_time}s to avoid throttling...")
            sleep(wait_time)
            headers = _get_headers(account=account)
            usage = _check_rate_limit(headers=headers, account_id=account_id)

        _wait_for_job_completion(job)
        logger.info(
            f"Report done ({job[AdReportRun.Field.async_status]} {job[AdReportRun.Field.async_percent_completion]}%)"
        )

        # TODO: This can take a long time. Improve logging to show progress.
        logger.info("Fetching report results...")
        return [item._json for item in job.get_result()]

    @itlp.asset(
        # schema=...,
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def ads(
        account_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = get_ads_insights(
            account_id=account_id,
            fields=constants.ADS_FIELDS,
            params={
                "level": "ad",
                "time_range": {"since": date.isoformat(), "until": date.isoformat()},
                "breakdowns": ["publisher_platform", "platform_position", "impression_device"],
                "action_breakdown": ["action_device", "action_type"],
            },
        )
        return pd.DataFrame(data)

    @itlp.asset(
        # schema=...,
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def ads_by_age_gender(
        account_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = get_ads_insights(
            account_id=account_id,
            fields=constants.GENDER_AGE_FIELDS,
            params={
                "level": "ad",
                "time_range": {"since": date.isoformat(), "until": date.isoformat()},
                "breakdowns": ["gender", "age"],
                "action_breakdown": ["action_device", "action_type"],
            },
        )
        return pd.DataFrame(data)

    @itlp.asset(
        # schema=...,
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def ads_by_country(
        account_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = get_ads_insights(
            account_id=account_id,
            fields=constants.GEO_FIELDS,
            params={
                "level": "ad",
                "time_range": {"since": date.isoformat(), "until": date.isoformat()},
                "breakdowns": ["country", "region"],
                "action_breakdown": ["action_device", "action_type"],
            },
        )
        return pd.DataFrame(data)

    @itlp.asset(
        # schema=...,
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def campaigns(
        account_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = get_ads_insights(
            account_id=account_id,
            fields=constants.CAMPAINGS_FIELDS,
            params={
                "level": "campaign",
                "time_range": {"since": date.isoformat(), "until": date.isoformat()},
                "breakdowns": ["publisher_platform", "platform_position", "impression_device"],
                "action_breakdown": ["action_device", "action_type"],
            },
        )
        return pd.DataFrame(data)

    @itlp.asset(
        # schema=...,
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def videos(
        account_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = get_ads_insights(
            account_id=account_id,
            fields=constants.VIDEOS_FIELDS,
            params={
                "level": "ad",
                "time_range": {"since": date.isoformat(), "until": date.isoformat()},
                "breakdowns": ["publisher_platform", "platform_position", "impression_device"],
                "action_breakdown": ["action_device", "action_type"],
            },
        )
        return pd.DataFrame(data)

    return (ads, ads_by_age_gender, ads_by_country, campaigns, videos)
