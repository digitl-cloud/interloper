import datetime as dt
import gzip
import json
from functools import partial
from typing import Any

import httpx2
import interloper as il
import pandas as pd
import tenacity as tc
from interloper_pandas import DataFrameNormalizer

from interloper_assets.amazon_ads import constants
from interloper_assets.amazon_ads.connection import AmazonAdsConnection
from interloper_assets.amazon_ads.connector import schemas


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
    response = httpx2.get(url, timeout=None)
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
        if isinstance(ex, httpx2.HTTPStatusError) and ex.response.status_code == 429:
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
        | tc.retry_if_exception(lambda e: issubclass(e.__class__, httpx2.HTTPError)),
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


def _list_entities(
    connection: AmazonAdsConnection, profile_id: str, path: str, media_type: str, key: str
) -> list[dict]:
    """List every Sponsored Products or Sponsored Brands entity of a list call, following its next token.

    Used by the products and brands campaigns and ad groups assets. Their list
    calls page through a ``nextToken`` in the POST body, which the client's
    GET paginators cannot drive. Archived entities are asked for explicitly,
    since the reports still carry their history, and the extended data adds
    the serving status and lifecycle times.

    Args:
        connection: The Amazon Ads connection.
        profile_id: The profile whose entities are listed, sent as the scope header.
        path: The list call's path, such as ``/sp/campaigns/list``.
        media_type: The resource's vendor media type, sent as both content type and accept.
        key: The response key holding the page's entities, ``campaigns`` or ``adGroups``.

    Returns:
        Every entity, in the order the API returned them.
    """
    headers = {"Amazon-Advertising-API-Scope": profile_id, "Content-Type": media_type, "Accept": media_type}
    body: dict[str, Any] = {
        "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
        "includeExtendedDataFields": True,
        "maxResults": 100,
    }
    rows: list[dict] = []
    while True:
        response = connection.client.post(path, headers=headers, json=body)
        response.raise_for_status()
        page = response.json()
        rows.extend(page.get(key, []))
        if not page.get("nextToken"):
            return rows
        body = {**body, "nextToken": page["nextToken"]}


def _list_display(connection: AmazonAdsConnection, profile_id: str, path: str) -> list[dict]:
    """List every Sponsored Display entity of an extended list call, by offset until an empty page.

    Used by the display campaigns and ad groups assets. The extended calls add
    the serving status and lifecycle times to the plain list.

    Args:
        connection: The Amazon Ads connection.
        profile_id: The profile whose entities are listed, sent as the scope header.
        path: The extended list call's path, such as ``/sd/campaigns/extended``.

    Returns:
        Every entity, in the order the API returned them.
    """
    pages = connection.client.paginate(
        path,
        il.OffsetPaginator(limit=100, offset_param="startIndex", limit_param="count"),
        params={"stateFilter": "enabled,paused,archived"},
        headers={"Amazon-Advertising-API-Scope": profile_id},
    )
    return [row for page in pages for row in page]


class SponsoredDisplayEntityNormalizer(DataFrameNormalizer):
    """The source's normalization, plus Sponsored Display's compact ``YYYYMMDD`` schedule dates.

    Sponsored Display sends ``startDate``/``endDate`` as ``20261001`` where the
    other ad products send ``2026-10-01``. Strict validation reads a digits-only
    string as a Unix timestamp and rejects it as a date, so the two columns are
    parsed here with their explicit format; a value in any other format fails.
    """

    def normalize(self, data: Any) -> pd.DataFrame:
        """Normalize as the source does, then parse the schedule dates.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame, ``start_date`` and ``end_date`` as dates.
        """
        df = super().normalize(data)
        for column in ("start_date", "end_date"):
            if column in df.columns:
                df[column] = pd.to_datetime(df[column], format="%Y%m%d").dt.date
        return df


# Sponsored Brands and Display send creation and update times as milliseconds since the epoch
_BRANDS_ENTITY_NORMALIZER = DataFrameNormalizer(
    snake_case_digits=True,
    flatten_max_level=1,
    epoch_columns=["extended_data_creation_date", "extended_data_last_update_date"],
    epoch_unit="ms",
)
_DISPLAY_ENTITY_NORMALIZER = SponsoredDisplayEntityNormalizer(
    snake_case_digits=True,
    flatten_max_level=1,
    epoch_columns=["creation_date", "last_updated_date"],
    epoch_unit="ms",
)


def _snapshot(rows: list[dict], day: dt.date) -> list[dict]:
    """Stamp entity rows with the snapshot day and keep their free-form tags whole.

    Used by every campaigns and ad groups asset. ``tags`` is an arbitrary
    key/value map that the source normalizer would flatten into one column per
    advertiser key, so it is kept as one JSON string under the vendor's name.

    Args:
        rows: The entities as the API returned them.
        day: The partition day.

    Returns:
        The rows with ``date`` set and ``tags``, when present, as JSON.
    """
    return [
        {**row, **({"tags": json.dumps(row["tags"])} if row.get("tags") is not None else {}), "date": day}
        for row in rows
    ]


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
        label="Profile ID",
        description="Amazon Ads profile",
        provider="connection.profiles",
        label_key="name",
        value_key="profile_id",
        discriminator=True,
    )

    @il.asset(
        schema=schemas.Profiles,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
    )
    def profiles(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """The advertising profile with its marketplace, currency, timezone and account."""
        response = self.connection.client.get("/v2/profiles")
        response.raise_for_status()
        # the call lists every profile the credentials see; each source snapshots its own
        own = [row for row in response.json() if str(row["profileId"]) == self.profile_id]
        return [{**row, "date": context.partition_date} for row in own]

    @il.asset(
        schema=schemas.ProductsCampaigns,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
    )
    def products_campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Sponsored Products campaigns with their schedule, budget, bidding and serving status."""
        rows = _list_entities(
            self.connection, self.profile_id, "/sp/campaigns/list", "application/vnd.spCampaign.v3+json", "campaigns"
        )
        return _snapshot(rows, context.partition_date)

    @il.asset(
        schema=schemas.ProductsAdGroups,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
    )
    def products_ad_groups(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Sponsored Products ad groups with their default bid and serving status."""
        rows = _list_entities(
            self.connection, self.profile_id, "/sp/adGroups/list", "application/vnd.spAdGroup.v3+json", "adGroups"
        )
        return _snapshot(rows, context.partition_date)

    @il.asset(
        schema=schemas.BrandsCampaigns,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_BRANDS_ENTITY_NORMALIZER,
    )
    def brands_campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Sponsored Brands campaigns with their schedule, budget, bidding and serving status."""
        rows = _list_entities(
            self.connection,
            self.profile_id,
            "/sb/v4/campaigns/list",
            "application/vnd.sbcampaignresource.v4+json",
            "campaigns",
        )
        return _snapshot(rows, context.partition_date)

    @il.asset(
        schema=schemas.BrandsAdGroups,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_BRANDS_ENTITY_NORMALIZER,
    )
    def brands_ad_groups(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Sponsored Brands ad groups with their serving status."""
        rows = _list_entities(
            self.connection,
            self.profile_id,
            "/sb/v4/adGroups/list",
            "application/vnd.sbadgroupresource.v4+json",
            "adGroups",
        )
        return _snapshot(rows, context.partition_date)

    @il.asset(
        schema=schemas.DisplayCampaigns,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_DISPLAY_ENTITY_NORMALIZER,
    )
    def display_campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Sponsored Display campaigns with their schedule, budget, tactic and serving status."""
        return _snapshot(
            _list_display(self.connection, self.profile_id, "/sd/campaigns/extended"), context.partition_date
        )

    @il.asset(
        schema=schemas.DisplayAdGroups,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_DISPLAY_ENTITY_NORMALIZER,
    )
    def display_ad_groups(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Sponsored Display ad groups with their default bid, tactic and serving status."""
        return _snapshot(
            _list_display(self.connection, self.profile_id, "/sd/adGroups/extended"), context.partition_date
        )

    # --- Sponsored Products ---

    @il.asset(
        schema=schemas.ProductsAdvertisedProductsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def products_advertised_products_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Performance of advertised products."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Campaign performance for sponsored products."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Performance of search terms in sponsored products campaigns."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Performance based on targeting criteria in sponsored products campaigns."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Products purchased through sponsored products campaigns."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Gross and invalid traffic metrics for sponsored products campaigns."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Display advertising campaign performance."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Performance of advertised products within display campaigns."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Products purchased through display campaigns."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Display campaign performance based on targeting criteria."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Traffic quality metrics for display campaigns."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Ad group performance within display campaigns."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Brand promotion campaign performance."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Individual ad performance within brand campaigns."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Search term performance in brand campaigns."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Brand campaign performance based on targeting criteria."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Products purchased through brand campaigns."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Traffic quality metrics for brand campaigns."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Campaign performance by ad placement."""
        data = request_and_download_report(
            self.connection,
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
    ) -> list[dict[str, Any]]:
        """Ad group performance within brand campaigns."""
        data = request_and_download_report(
            self.connection,
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
        schema=schemas.TelevisionCampaignsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def television_campaigns_stats(
        self,
        context: il.ExecutionContext,
    ) -> list[dict[str, Any]]:
        """Television campaign performance."""
        data = request_and_download_report(
            self.connection,
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
        schema=schemas.TelevisionTargetingStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def television_targeting_stats(
        self,
        context: il.ExecutionContext,
    ) -> list[dict[str, Any]]:
        """Television campaign performance by targeting."""
        data = request_and_download_report(
            self.connection,
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
