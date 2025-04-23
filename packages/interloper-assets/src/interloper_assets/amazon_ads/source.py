import datetime as dt
import enum
import gzip
import json
import logging
from functools import partial

import httpx
import interloper as itlp
import pandas as pd
from interloper_pandas.normalizer import DataframeNormalizer
from tenacity import (
    RetryCallState,
    Retrying,
    retry,
    retry_if_exception,
    retry_if_result,
    stop_after_attempt,
    stop_after_delay,
    wait_fixed,
    wait_incrementing,
)

from interloper_assets.amazon_ads import constants

logger = logging.getLogger(__name__)


# Notes:
# - Even though the API supports start and end date, the data is aggregated over the entire date range,
#   so we don't use DateWindow to partition the data.
# - TODO: should we set the profile ID at the source level?


class AmazonAdsAPILocation(enum.Enum):
    NORTH_AMERICA = "NA"
    EUROPE = "EU"
    FAR_EAST = "FE"

    @property
    def api_url(self) -> str:
        return {
            AmazonAdsAPILocation.EUROPE: "https://advertising-api-eu.amazon.com",
            AmazonAdsAPILocation.FAR_EAST: "https://advertising-api-fe.amazon.com",
            AmazonAdsAPILocation.NORTH_AMERICA: "https://advertising-api.amazon.com",
        }[self]

    @property
    def auth_url(self) -> str:
        return {
            AmazonAdsAPILocation.EUROPE: "https://api.amazon.co.uk",
            AmazonAdsAPILocation.FAR_EAST: "https://api.amazon.co.jp",
            AmazonAdsAPILocation.NORTH_AMERICA: "https://api.amazon.com",
        }[self]


@itlp.source(
    normalizer=DataframeNormalizer(),
)
def amazon_ads(
    location: AmazonAdsAPILocation | str = itlp.Env("AMAZON_ADS_API_LOCATION"),
    client_id: str = itlp.Env("AMAZON_ADS_CLIENT_ID"),
    client_secret: str = itlp.Env("AMAZON_ADS_CLIENT_SECRET"),
    refresh_token: str = itlp.Env("AMAZON_ADS_REFRESH_TOKEN"),
) -> tuple[itlp.Asset, ...]:
    location = AmazonAdsAPILocation(location) if isinstance(location, str) else location
    auth = itlp.OAuth2RefreshTokenAuth(
        base_url=location.auth_url,
        token_endpoint="/auth/o2/token",
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
    )
    client = itlp.RESTClient(
        base_url=location.api_url,
        auth=auth,
        headers={"Amazon-Advertising-API-ClientId": client_id},
    )

    def request_report(
        report_type_id: str,
        profile_id: str,
        ad_product: str,
        group_by: list[str],
        columns: list[str],
        start_date: dt.date,
        end_date: dt.date,
    ) -> dict:
        response = client.post(
            f"{location.api_url}/reporting/reports",
            headers={
                "Amazon-Advertising-API-Scope": profile_id,
                "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
            },
            json={
                "name": f"{report_type_id} | {start_date.isoformat()} - {end_date.isoformat()}",
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "configuration": {
                    "reportTypeId": report_type_id,
                    "adProduct": ad_product,
                    "groupBy": group_by,
                    "columns": columns,
                    "timeUnit": "DAILY",
                    "format": "GZIP_JSON",
                },
            },
        )
        response.raise_for_status()
        return response.json()

    def get_report_status(profile_id: str, report_id: str) -> dict:
        response = client.get(
            f"{location.api_url}/reporting/reports/{report_id}",
            headers={
                "Amazon-Advertising-API-Scope": profile_id,
                "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
            },
        )
        response.raise_for_status()
        return response.json()

    def download_report(url: str) -> list[dict]:
        response = httpx.get(url, timeout=None)
        response.raise_for_status()

        gzip_content = response.content
        json_content = gzip.decompress(gzip_content)

        return json.loads(json_content)

    def _log_retry(retry_state: RetryCallState) -> None:
        if retry_state.outcome is None:
            raise RuntimeError("log called before outcome was set")

        if retry_state.next_action is None:
            raise RuntimeError("log called before next_action was set")

        if retry_state.outcome.failed:
            ex = retry_state.outcome.exception()
            logger.debug(f"Retrying in {retry_state.next_action.sleep}. Raised {ex.__class__.__name__}: {ex}.")
            if isinstance(ex, httpx.HTTPStatusError) and ex.response.status_code == 429:
                logger.debug(f"Retry-After response header: {ex.response.headers.get('Retry-After')}")
        else:
            logger.debug(f"Retrying in {retry_state.next_action.sleep}. Returned {retry_state.outcome.result()}")

    @retry(stop=stop_after_attempt(5))
    def _authenticate() -> None:
        client.authenticate()

    def request_and_download_report(
        report_type_id: str,
        profile_id: str,
        ad_product: str,
        group_by: list[str],
        columns: list[str],
        start_date: dt.date,
        end_date: dt.date,
        max_request_delay: int | dt.timedelta = dt.timedelta(hours=1),
        max_wait_delay: int | dt.timedelta = dt.timedelta(hours=2),
        max_download_delay: int | dt.timedelta = dt.timedelta(minutes=10),
    ) -> list[dict]:
        ConfiguredRetrying = partial(
            Retrying,
            wait=wait_fixed(60),
            before=lambda _: _authenticate(),
            before_sleep=_log_retry,
            reraise=True,
        )
        retry_if_completed = retry_if_result(lambda status: status != "COMPLETED")
        retry_if_http_error = retry_if_exception(lambda e: issubclass(e.__class__, httpx.HTTPError))
        retry_if_not_400 = retry_if_exception(
            lambda e: not (isinstance(e, httpx.HTTPStatusError) and e.response.status_code == 400)
        )

        for attempt in ConfiguredRetrying(
            stop=stop_after_delay(max_request_delay),
            wait=wait_incrementing(start=60, increment=dt.timedelta(minutes=5), max=dt.timedelta(minutes=15)),
            retry=retry_if_not_400,
        ):
            with attempt:
                logger.info(
                    f"Requesting {report_type_id} report for profile {profile_id} "
                    f"(attempt {attempt.retry_state.attempt_number})"
                )
                data = request_report(report_type_id, profile_id, ad_product, group_by, columns, start_date, end_date)

        report_id = data["reportId"]
        logger.info(f"Report id: {report_id} for profile {profile_id}")

        # Wait for report
        for attempt in ConfiguredRetrying(
            stop=stop_after_delay(max_wait_delay),
            retry=retry_if_completed | retry_if_http_error,
        ):
            with attempt:
                logger.info(f"Waiting for report {report_id} (profile {profile_id}) ")
                response = get_report_status(profile_id, report_id)

            if attempt.retry_state.outcome and not attempt.retry_state.outcome.failed:
                attempt.retry_state.set_result(response["status"])

        report_url = response["url"]
        logger.info(f"Report URL: {report_url} for profile {profile_id}")

        for attempt in ConfiguredRetrying(
            stop=stop_after_delay(max_download_delay),
        ):
            with attempt:
                logger.info(f"Downloading report (attempt {attempt.retry_state.attempt_number})")
                report = download_report(report_url)

        return report

    @itlp.asset
    def profiles() -> pd.DataFrame:
        response = client.get("/v2/profiles")
        data = response.json()
        return pd.DataFrame(data)

    @itlp.asset
    def manager_accounts() -> pd.DataFrame:
        response = client.get("/managerAccounts")
        data = response.json()
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def products_advertised_products(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_PRODUCTS",
            report_type_id="spAdvertisedProduct",
            group_by=["advertiser"],
            columns=constants.PRODUCTS_ADVERTISED_PRODUCT_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def products_campaigns(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_PRODUCTS",
            report_type_id="spCampaigns",
            group_by=["campaign", "adGroup"],
            columns=constants.PRODUCTS_CAMPAIGN_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def products_gross_and_invalid_traffic(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_PRODUCTS",
            report_type_id="spGrossAndInvalids",
            group_by=["campaign"],
            columns=constants.PRODUCTS_GROSS_AND_INVALID_TRAFFIC_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def products_purchased_products(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_PRODUCTS",
            report_type_id="spPurchasedProduct",
            group_by=["asin"],
            columns=constants.PRODUCTS_PURCHASED_PRODUCT_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def products_search_terms(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_PRODUCTS",
            report_type_id="spSearchTerm",
            group_by=["searchTerm"],
            columns=constants.PRODUCTS_SEARCH_TERM_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def products_targeting(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_PRODUCTS",
            report_type_id="spTargeting",
            group_by=["targeting"],
            columns=constants.PRODUCTS_TARGETING_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def brands_ads(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbAds",
            group_by=["ads"],
            columns=constants.BRANDS_AD_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def brands_ad_groups(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbAdGroup",
            group_by=["adGroup"],
            columns=constants.BRANDS_AD_GROUP_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def brands_campaigns(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbCampaigns",
            group_by=["campaign"],
            columns=constants.BRANDS_CAMPAIGN_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def brands_gross_and_invalid_traffic(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbGrossAndInvalids",
            group_by=["campaign"],
            columns=constants.BRANDS_GROSS_AND_INVALID_TRAFFIC_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def brands_placements(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbCampaignPlacement",
            group_by=["campaignPlacement"],
            columns=constants.BRANDS_PLACEMENT_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def brands_purchased_products(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbPurchasedProduct",
            group_by=["purchasedAsin"],
            columns=constants.BRANDS_PURCHASED_PRODUCT_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def brands_search_terms(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbSearchTerm",
            group_by=["searchTerm"],
            columns=constants.BRANDS_SEARCH_TERM_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def brands_targeting(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbTargeting",
            group_by=["targeting"],
            columns=constants.BRANDS_TARGETING_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def display_ad_groups(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_DISPLAY",
            report_type_id="sdAdGroup",
            group_by=["adGroup"],
            columns=constants.DISPLAY_AD_GROUP_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def display_advertised_products(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_DISPLAY",
            report_type_id="sdAdvertisedProduct",
            group_by=["advertiser"],
            columns=constants.DISPLAY_ADVERTISED_PRODUCT_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def display_campaigns(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_DISPLAY",
            report_type_id="sdCampaigns",
            group_by=["campaign"],
            columns=constants.DISPLAY_CAMPAIGN_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def display_gross_and_invalid_traffic(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_DISPLAY",
            report_type_id="sdGrossAndInvalids",
            group_by=["campaign"],
            columns=constants.DISPLAY_GROSS_AND_INVALID_TRAFFIC_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def display_purchased_products(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_DISPLAY",
            report_type_id="sdPurchasedProduct",
            group_by=["asin"],
            columns=constants.DISPLAY_PURCHASED_PRODUCT_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def display_targeting(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_DISPLAY",
            report_type_id="sdTargeting",
            group_by=["targeting"],
            columns=constants.DISPLAY_TARGETING_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def television_campaigns(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_TELEVISION",
            report_type_id="stCampaigns",
            group_by=["campaign"],
            columns=constants.TELEVISION_CAMPAIGN_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def television_targeting(
        profile_id: str,
        date: dt.date = itlp.Date(),
    ) -> pd.DataFrame:
        data = request_and_download_report(
            profile_id=profile_id,
            ad_product="SPONSORED_TELEVISION",
            report_type_id="stTargeting",
            group_by=["targeting"],
            columns=constants.TELEVISION_TARGETING_METRICS,
            start_date=date,
            end_date=date,
        )
        return pd.DataFrame(data)

    return (
        profiles,
        manager_accounts,
        products_advertised_products,
        products_campaigns,
        products_gross_and_invalid_traffic,
        products_purchased_products,
        products_search_terms,
        products_targeting,
        brands_ads,
        brands_ad_groups,
        brands_campaigns,
        brands_gross_and_invalid_traffic,
        brands_placements,
        brands_purchased_products,
        brands_search_terms,
        brands_targeting,
        display_ad_groups,
        display_advertised_products,
        display_campaigns,
        display_gross_and_invalid_traffic,
        display_purchased_products,
        display_targeting,
        television_campaigns,
        television_targeting,
    )
