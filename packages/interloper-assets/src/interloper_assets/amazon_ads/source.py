import datetime as dt
import gzip
import json
from functools import partial
from typing import Any

import httpx
import interloper as il
import tenacity as tc
from interloper_pandas import DataFrameNormalizer

from interloper_assets.amazon_ads import constants, schemas
from interloper_assets.amazon_ads.connection import AmazonAdsConnection


# -- HELPERS -------------------------------------------------------------------
def request_report(
    connection: AmazonAdsConnection,
    profile_id: str,
    report_type_id: str,
    ad_product: str,
    group_by: list[str],
    columns: list[str],
    start_date: dt.date,
    end_date: dt.date,
) -> dict:
    """Request an async report from the Amazon Ads API."""
    response = connection.client.post(
        "/reporting/reports",
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


def get_report_status(connection: AmazonAdsConnection, profile_id: str, report_id: str) -> dict:
    """Check the status of an async report."""
    response = connection.client.get(
        f"/reporting/reports/{report_id}",
        headers={
            "Amazon-Advertising-API-Scope": profile_id,
            "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
        },
    )
    response.raise_for_status()
    return response.json()


def download_report(url: str) -> list[dict]:
    """Download and decompress a completed report."""
    response = httpx.get(url, timeout=None)
    response.raise_for_status()
    return json.loads(gzip.decompress(response.content))


def _log_retry(context: il.ExecutionContext, retry_state: tc.RetryCallState) -> None:
    if retry_state.outcome is None:
        raise RuntimeError("log called before outcome was set")
    if retry_state.next_action is None:
        raise RuntimeError("log called before next_action was set")

    if retry_state.outcome.failed:
        ex = retry_state.outcome.exception()
        context.logger.debug(f"Retrying in {retry_state.next_action.sleep}. Raised {ex.__class__.__name__}: {ex}.")
        if isinstance(ex, httpx.HTTPStatusError) and ex.response.status_code == 429:
            context.logger.debug(f"Retry-After response header: {ex.response.headers.get('Retry-After')}")
    else:
        context.logger.debug(f"Retrying in {retry_state.next_action.sleep}. Returned {retry_state.outcome.result()}")


@tc.retry(stop=tc.stop_after_attempt(5))
def _clear_auth(connection: AmazonAdsConnection) -> None:
    auth = connection.client.auth
    if isinstance(auth, il.OAuth2RefreshTokenAuth):
        auth.clear_token()


def request_and_download_report(
    connection: AmazonAdsConnection,
    profile_id: str,
    context: il.ExecutionContext,
    report_type_id: str,
    ad_product: str,
    group_by: list[str],
    columns: list[str],
    start_date: dt.date,
    end_date: dt.date,
    max_request_delay: int | dt.timedelta = dt.timedelta(hours=1),
    max_wait_delay: int | dt.timedelta = dt.timedelta(hours=2),
    max_download_delay: int | dt.timedelta = dt.timedelta(minutes=10),
) -> list[dict]:
    """Request, wait for, and download a report with retry logic."""
    ConfiguredRetrying = partial(
        tc.Retrying,
        wait=tc.wait_exponential(multiplier=10, max=60),
        before=lambda _: _clear_auth(connection),
        before_sleep=partial(_log_retry, context),
        reraise=True,
    )

    for attempt in ConfiguredRetrying(
        stop=tc.stop_after_delay(max_request_delay),
        wait=tc.wait_incrementing(start=60, increment=dt.timedelta(minutes=5), max=dt.timedelta(minutes=15)),
    ):
        with attempt:
            context.logger.info(
                f"Requesting {report_type_id} report for profile {profile_id} "
                f"(attempt {attempt.retry_state.attempt_number})"
            )
            data = request_report(
                connection, profile_id, report_type_id, ad_product, group_by, columns, start_date, end_date
            )

    report_id = data["reportId"]
    context.logger.info(f"Report id: {report_id} for profile {profile_id}")

    # Wait for report
    for attempt in ConfiguredRetrying(
        stop=tc.stop_after_delay(max_wait_delay),
        retry=tc.retry_if_result(lambda status: status != "COMPLETED")
        | tc.retry_if_exception(lambda e: issubclass(e.__class__, httpx.HTTPError)),
    ):
        with attempt:
            context.logger.info(f"Waiting for report {report_id} (profile {profile_id}) ")
            response = get_report_status(connection, profile_id, report_id)

        if attempt.retry_state.outcome and not attempt.retry_state.outcome.failed:
            attempt.retry_state.set_result(response["status"])

    report_url = response["url"]
    context.logger.info(f"Report URL: {report_url} for profile {profile_id}")

    for attempt in ConfiguredRetrying(
        stop=tc.stop_after_delay(max_download_delay),
    ):
        with attempt:
            context.logger.info(f"Downloading report (attempt {attempt.retry_state.attempt_number})")
            report = download_report(report_url)

    return report


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    icon="icon:amazon",
    normalizer=DataFrameNormalizer(
        snake_case_digits=True,
        flatten_max_level=1,
        column_overrides={
            "eCPAddToCart": "ecp_add_to_cart",
            "eCPBrandSearch": "ecp_brand_search",
        },
    ),
)
class AmazonAds(il.Source):
    """Amazon Ads advertising platform integration."""

    connection: AmazonAdsConnection

    profile_id: str = il.FetchField(
        title="Profile ID",
        description="Amazon Ads profile",
        provider="connection.profiles",
        label_key="name",
        value_key="profile_id",
    )

    def asset_table(self, asset: il.Asset) -> str:
        """Suffix tables with the profile_id so instances materialize side by side."""
        return f"{asset.key}__{self.profile_id}"

    @il.asset(schema=schemas.Profiles, tags=["Entity"])
    def profiles(self, connection: AmazonAdsConnection) -> list[dict[str, Any]]:
        """Advertising profiles associated with the account."""
        response = connection.client.get("/v2/profiles")
        response.raise_for_status()
        return response.json()

    # --- Sponsored Products ---

    @il.asset(
        schema=schemas.ProductsAdvertisedProductsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def products_advertised_products_stats(
        self, context: il.ExecutionContext, connection: AmazonAdsConnection
    ) -> list[dict[str, Any]]:
        """Performance of advertised products."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_PRODUCTS",
            report_type_id="spAdvertisedProduct",
            group_by=["advertiser"],
            columns=constants.PRODUCTS_ADVERTISED_PRODUCT_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    @il.asset(
        schema=schemas.ProductsCampaignsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def products_campaigns_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Campaign performance for sponsored products."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_PRODUCTS",
            report_type_id="spCampaigns",
            group_by=["campaign", "adGroup"],
            columns=constants.PRODUCTS_CAMPAIGN_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    @il.asset(
        schema=schemas.ProductsSearchTermsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def products_search_terms_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Performance of search terms in sponsored products campaigns."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_PRODUCTS",
            report_type_id="spSearchTerm",
            group_by=["searchTerm"],
            columns=constants.PRODUCTS_SEARCH_TERM_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    @il.asset(
        schema=schemas.ProductsTargetingStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def products_targeting_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Performance based on targeting criteria in sponsored products campaigns."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_PRODUCTS",
            report_type_id="spTargeting",
            group_by=["targeting"],
            columns=constants.PRODUCTS_TARGETING_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    @il.asset(
        schema=schemas.ProductsPurchasedProductsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def products_purchased_products_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Products purchased through sponsored products campaigns."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_PRODUCTS",
            report_type_id="spPurchasedProduct",
            group_by=["asin"],
            columns=constants.PRODUCTS_PURCHASED_PRODUCT_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    @il.asset(
        schema=schemas.ProductsGrossAndInvalidTrafficStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def products_gross_and_invalid_traffic_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Gross and invalid traffic metrics for sponsored products campaigns."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_PRODUCTS",
            report_type_id="spGrossAndInvalids",
            group_by=["campaign"],
            columns=constants.PRODUCTS_GROSS_AND_INVALID_TRAFFIC_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    # --- Sponsored Display ---

    @il.asset(
        schema=schemas.DisplayCampaignsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def display_campaigns_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Display advertising campaign performance."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_DISPLAY",
            report_type_id="sdCampaigns",
            group_by=["campaign"],
            columns=constants.DISPLAY_CAMPAIGN_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    @il.asset(
        schema=schemas.DisplayAdvertisedProductsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def display_advertised_products_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Performance of advertised products within display campaigns."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_DISPLAY",
            report_type_id="sdAdvertisedProduct",
            group_by=["advertiser"],
            columns=constants.DISPLAY_ADVERTISED_PRODUCT_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    @il.asset(
        schema=schemas.DisplayPurchasedProductsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def display_purchased_products_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Products purchased through display campaigns."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_DISPLAY",
            report_type_id="sdPurchasedProduct",
            group_by=["asin"],
            columns=constants.DISPLAY_PURCHASED_PRODUCT_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    @il.asset(
        schema=schemas.DisplayTargetingStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def display_targeting_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Display campaign performance based on targeting criteria."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_DISPLAY",
            report_type_id="sdTargeting",
            group_by=["targeting"],
            columns=constants.DISPLAY_TARGETING_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    @il.asset(
        schema=schemas.DisplayGrossAndInvalidTrafficStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def display_gross_and_invalid_traffic_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Traffic quality metrics for display campaigns."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_DISPLAY",
            report_type_id="sdGrossAndInvalids",
            group_by=["campaign"],
            columns=constants.DISPLAY_GROSS_AND_INVALID_TRAFFIC_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    @il.asset(
        schema=schemas.DisplayAdGroupsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def display_ad_groups_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Ad group performance within display campaigns."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_DISPLAY",
            report_type_id="sdAdGroup",
            group_by=["adGroup"],
            columns=constants.DISPLAY_AD_GROUP_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    # --- Sponsored Brands ---

    @il.asset(
        schema=schemas.BrandsCampaignsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def brands_campaigns_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Brand promotion campaign performance."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbCampaigns",
            group_by=["campaign"],
            columns=constants.BRANDS_CAMPAIGN_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    @il.asset(
        schema=schemas.BrandsAdsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def brands_ads_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Individual ad performance within brand campaigns."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbAds",
            group_by=["ads"],
            columns=constants.BRANDS_AD_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    @il.asset(
        schema=schemas.BrandsSearchTermsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def brands_search_terms_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Search term performance in brand campaigns."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbSearchTerm",
            group_by=["searchTerm"],
            columns=constants.BRANDS_SEARCH_TERM_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    @il.asset(
        schema=schemas.BrandsTargetingStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def brands_targeting_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Brand campaign performance based on targeting criteria."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbTargeting",
            group_by=["targeting"],
            columns=constants.BRANDS_TARGETING_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    @il.asset(
        schema=schemas.BrandsPurchasedProductsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def brands_purchased_products_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Products purchased through brand campaigns."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbPurchasedProduct",
            group_by=["purchasedAsin"],
            columns=constants.BRANDS_PURCHASED_PRODUCT_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    @il.asset(
        schema=schemas.BrandsGrossAndInvalidTrafficStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def brands_gross_and_invalid_traffic_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Traffic quality metrics for brand campaigns."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbGrossAndInvalids",
            group_by=["campaign"],
            columns=constants.BRANDS_GROSS_AND_INVALID_TRAFFIC_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    @il.asset(
        schema=schemas.BrandsPlacementsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def brands_placements_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Campaign performance by ad placement."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbCampaignPlacement",
            group_by=["campaignPlacement"],
            columns=constants.BRANDS_PLACEMENT_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    @il.asset(
        schema=schemas.BrandsAdGroupsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def brands_ad_groups_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Ad group performance within brand campaigns."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbAdGroup",
            group_by=["adGroup"],
            columns=constants.BRANDS_AD_GROUP_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    # --- Sponsored Television ---

    @il.asset(
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def television_campaigns_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Television campaign performance."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_TELEVISION",
            report_type_id="stCampaigns",
            group_by=["campaign"],
            columns=constants.TELEVISION_CAMPAIGN_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data

    @il.asset(
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def television_targeting_stats(
        self,
        context: il.ExecutionContext,
        connection: AmazonAdsConnection,
    ) -> list[dict[str, Any]]:
        """Television campaign performance by targeting."""
        data = request_and_download_report(
            connection,
            self.profile_id,
            context,
            ad_product="SPONSORED_TELEVISION",
            report_type_id="stTargeting",
            group_by=["targeting"],
            columns=constants.TELEVISION_TARGETING_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return data
