import json
from typing import Any

import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.amazon_ads import constants
from interloper_assets.amazon_ads.connection import AmazonAdsConnection
from interloper_assets.amazon_ads.connector import schemas

# the entity lists leave archived entities out unless asked for every state, and the reports still carry their history
_EVERY_STATE = {"stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]}}
# Sponsored Display's lists take the same filter as a query string; their extended calls carry the serving status
_EVERY_DISPLAY_STATE = {"stateFilter": "enabled,paused,archived"}
# the serving status and lifecycle times; the Sponsored Brands ads list takes no such field and returns them anyway
_EXTENDED_DATA = {"includeExtendedDataFields": True}


# -- NORMALIZERS ---------------------------------------------------------------
class AmazonAdsNormalizer(DataFrameNormalizer):
    """The source's normalization, keeping a campaign's free-form ``tags`` whole.

    ``tags`` is an arbitrary key/value map the advertiser writes on a campaign.
    Flattened like the other nested fields, it would become one column per key
    an advertiser ever used, so it is kept as one JSON string under the
    vendor's name before the frame is built.
    """

    def normalize(self, data: Any) -> pd.DataFrame:
        """Encode each row's ``tags`` map as JSON, then normalize as configured.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame, ``tags`` as a JSON string where a row has it.
        """
        records = data.to_dict("records") if isinstance(data, pd.DataFrame) else data
        rows = [
            {**row, "tags": json.dumps(row["tags"])} if isinstance(row.get("tags"), dict) else row for row in records
        ]
        return super().normalize(rows)


_DISPLAY_IDS = frozenset({"campaignId", "adGroupId", "adId", "portfolioId"})


class SponsoredDisplayEntityNormalizer(AmazonAdsNormalizer):
    """The source's normalization, plus Sponsored Display's numeric ids and compact ``YYYYMMDD`` dates.

    Sponsored Display sends ids as numbers where the other ad products send
    strings. They become strings row by row, before the frame is built: some
    ids are beyond what a float holds exactly, and a column with a missing
    value (``portfolioId``) would otherwise turn float. It also sends
    ``startDate``/``endDate`` as ``20261001`` where the others send
    ``2026-10-01``; strict validation reads a digits-only string as a Unix
    timestamp, so the two columns are parsed with their explicit format, and a
    value in any other format fails.
    """

    def normalize(self, data: Any) -> pd.DataFrame:
        """Stringify the ids, normalize as the source does, then parse the schedule dates.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame, ids as strings and ``start_date``/``end_date`` as dates.
        """
        records = data.to_dict("records") if isinstance(data, pd.DataFrame) else data
        rows = [
            {key: str(value) if key in _DISPLAY_IDS and pd.notna(value) else value for key, value in row.items()}
            for row in records
        ]
        df = super().normalize(rows)
        for column in ("start_date", "end_date"):
            if column in df.columns:
                df[column] = pd.to_datetime(df[column], format="%Y%m%d").dt.date
        return df


# Sponsored Brands and Display send creation and update times as milliseconds since the epoch
_BRANDS_ENTITY_NORMALIZER = AmazonAdsNormalizer(
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


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    icon="icon:amazon",
    normalizer=AmazonAdsNormalizer(
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
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
    )
    def profiles(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """The advertising profile with its marketplace, currency, timezone and account."""
        # the call lists every profile the credentials see; each source snapshots its own
        own = [row for row in self.connection.api.get("/v2/profiles") if str(row["profileId"]) == self.profile_id]
        if not own:
            raise RuntimeError(f"Amazon Ads does not list profile {self.profile_id}: the credentials no longer see it")
        return [{**row, "date": context.partition_date} for row in own]

    @il.asset(
        schema=schemas.ProductsCampaigns,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
    )
    def products_campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Sponsored Products campaigns with their schedule, budget, bidding and serving status."""
        rows = self.connection.api.rows(
            self.profile_id,
            "/sp/campaigns/list",
            media_type="application/vnd.spCampaign.v3+json",
            key="campaigns",
            **_EVERY_STATE,
            **_EXTENDED_DATA,
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.ProductsAdGroups,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
    )
    def products_ad_groups(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Sponsored Products ad groups with their default bid and serving status."""
        rows = self.connection.api.rows(
            self.profile_id,
            "/sp/adGroups/list",
            media_type="application/vnd.spAdGroup.v3+json",
            key="adGroups",
            **_EVERY_STATE,
            **_EXTENDED_DATA,
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.BrandsCampaigns,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_BRANDS_ENTITY_NORMALIZER,
    )
    def brands_campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Sponsored Brands campaigns with their schedule, budget, bidding and serving status."""
        rows = self.connection.api.rows(
            self.profile_id,
            "/sb/v4/campaigns/list",
            media_type="application/vnd.sbcampaignresource.v4+json",
            key="campaigns",
            **_EVERY_STATE,
            **_EXTENDED_DATA,
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.BrandsAdGroups,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_BRANDS_ENTITY_NORMALIZER,
    )
    def brands_ad_groups(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Sponsored Brands ad groups with their serving status."""
        rows = self.connection.api.rows(
            self.profile_id,
            "/sb/v4/adGroups/list",
            media_type="application/vnd.sbadgroupresource.v4+json",
            key="adGroups",
            **_EVERY_STATE,
            **_EXTENDED_DATA,
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.DisplayCampaigns,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_DISPLAY_ENTITY_NORMALIZER,
    )
    def display_campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Sponsored Display campaigns with their schedule, budget, tactic and serving status."""
        rows = self.connection.api.offset_rows(self.profile_id, "/sd/campaigns/extended", **_EVERY_DISPLAY_STATE)
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.DisplayAdGroups,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_DISPLAY_ENTITY_NORMALIZER,
    )
    def display_ad_groups(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Sponsored Display ad groups with their default bid, tactic and serving status."""
        rows = self.connection.api.offset_rows(self.profile_id, "/sd/adGroups/extended", **_EVERY_DISPLAY_STATE)
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.ProductsAds,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
    )
    def products_ads(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Sponsored Products product ads with their advertised product and serving status."""
        rows = self.connection.api.rows(
            self.profile_id,
            "/sp/productAds/list",
            media_type="application/vnd.spProductAd.v3+json",
            key="productAds",
            **_EVERY_STATE,
            **_EXTENDED_DATA,
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.BrandsAds,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_BRANDS_ENTITY_NORMALIZER,
    )
    def brands_ads(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Sponsored Brands ads with their creative, landing page and serving status."""
        rows = self.connection.api.rows(
            self.profile_id,
            "/sb/v4/ads/list",
            media_type="application/vnd.sbadresource.v4+json",
            key="ads",
            **_EVERY_STATE,
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.DisplayAds,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_DISPLAY_ENTITY_NORMALIZER,
    )
    def display_ads(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Sponsored Display product ads with their advertised product and serving status."""
        rows = self.connection.api.offset_rows(self.profile_id, "/sd/productAds/extended", **_EVERY_DISPLAY_STATE)
        return [{**row, "date": context.partition_date} for row in rows]

    # --- Sponsored Products ---

    @il.asset(
        schema=schemas.ProductsAdvertisedProductsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def products_advertised_products_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Performance of advertised products."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="spAdvertisedProduct",
            adProduct="SPONSORED_PRODUCTS",
            groupBy=["advertiser"],
            columns=constants.PRODUCTS_ADVERTISED_PRODUCT_METRICS,
            timeUnit="DAILY",
        )

    @il.asset(
        schema=schemas.ProductsCampaignsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def products_campaigns_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Campaign performance for sponsored products."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="spCampaigns",
            adProduct="SPONSORED_PRODUCTS",
            groupBy=["campaign", "adGroup"],
            columns=constants.PRODUCTS_CAMPAIGN_METRICS,
            timeUnit="DAILY",
        )

    @il.asset(
        schema=schemas.ProductsSearchTermsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def products_search_terms_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Performance of search terms in sponsored products campaigns."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="spSearchTerm",
            adProduct="SPONSORED_PRODUCTS",
            groupBy=["searchTerm"],
            columns=constants.PRODUCTS_SEARCH_TERM_METRICS,
            timeUnit="DAILY",
        )

    @il.asset(
        schema=schemas.ProductsTargetingStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def products_targeting_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Performance based on targeting criteria in sponsored products campaigns."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="spTargeting",
            adProduct="SPONSORED_PRODUCTS",
            groupBy=["targeting"],
            columns=constants.PRODUCTS_TARGETING_METRICS,
            timeUnit="DAILY",
        )

    @il.asset(
        schema=schemas.ProductsPurchasedProductsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def products_purchased_products_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Products purchased through sponsored products campaigns."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="spPurchasedProduct",
            adProduct="SPONSORED_PRODUCTS",
            groupBy=["asin"],
            columns=constants.PRODUCTS_PURCHASED_PRODUCT_METRICS,
            timeUnit="DAILY",
        )

    @il.asset(
        schema=schemas.ProductsGrossAndInvalidTrafficStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def products_gross_and_invalid_traffic_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Gross and invalid traffic metrics for sponsored products campaigns."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="spGrossAndInvalids",
            adProduct="SPONSORED_PRODUCTS",
            groupBy=["campaign"],
            columns=constants.PRODUCTS_GROSS_AND_INVALID_TRAFFIC_METRICS,
            timeUnit="DAILY",
        )

    # --- Sponsored Display ---

    @il.asset(
        schema=schemas.DisplayCampaignsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def display_campaigns_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Display advertising campaign performance."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="sdCampaigns",
            adProduct="SPONSORED_DISPLAY",
            groupBy=["campaign"],
            columns=constants.DISPLAY_CAMPAIGN_METRICS,
            timeUnit="DAILY",
        )

    @il.asset(
        schema=schemas.DisplayAdvertisedProductsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def display_advertised_products_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Performance of advertised products within display campaigns."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="sdAdvertisedProduct",
            adProduct="SPONSORED_DISPLAY",
            groupBy=["advertiser"],
            columns=constants.DISPLAY_ADVERTISED_PRODUCT_METRICS,
            timeUnit="DAILY",
        )

    @il.asset(
        schema=schemas.DisplayPurchasedProductsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def display_purchased_products_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Products purchased through display campaigns."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="sdPurchasedProduct",
            adProduct="SPONSORED_DISPLAY",
            groupBy=["asin"],
            columns=constants.DISPLAY_PURCHASED_PRODUCT_METRICS,
            timeUnit="DAILY",
        )

    @il.asset(
        schema=schemas.DisplayTargetingStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def display_targeting_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Display campaign performance based on targeting criteria."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="sdTargeting",
            adProduct="SPONSORED_DISPLAY",
            groupBy=["targeting"],
            columns=constants.DISPLAY_TARGETING_METRICS,
            timeUnit="DAILY",
        )

    @il.asset(
        schema=schemas.DisplayGrossAndInvalidTrafficStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def display_gross_and_invalid_traffic_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Traffic quality metrics for display campaigns."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="sdGrossAndInvalids",
            adProduct="SPONSORED_DISPLAY",
            groupBy=["campaign"],
            columns=constants.DISPLAY_GROSS_AND_INVALID_TRAFFIC_METRICS,
            timeUnit="DAILY",
        )

    @il.asset(
        schema=schemas.DisplayAdGroupsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def display_ad_groups_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad group performance within display campaigns."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="sdAdGroup",
            adProduct="SPONSORED_DISPLAY",
            groupBy=["adGroup"],
            columns=constants.DISPLAY_AD_GROUP_METRICS,
            timeUnit="DAILY",
        )

    # --- Sponsored Brands ---

    @il.asset(
        schema=schemas.BrandsCampaignsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def brands_campaigns_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Brand promotion campaign performance."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="sbCampaigns",
            adProduct="SPONSORED_BRANDS",
            groupBy=["campaign"],
            columns=constants.BRANDS_CAMPAIGN_METRICS,
            timeUnit="DAILY",
        )

    @il.asset(
        schema=schemas.BrandsAdsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def brands_ads_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Individual ad performance within brand campaigns."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="sbAds",
            adProduct="SPONSORED_BRANDS",
            groupBy=["ads"],
            columns=constants.BRANDS_AD_METRICS,
            timeUnit="DAILY",
        )

    @il.asset(
        schema=schemas.BrandsSearchTermsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def brands_search_terms_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Search term performance in brand campaigns."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="sbSearchTerm",
            adProduct="SPONSORED_BRANDS",
            groupBy=["searchTerm"],
            columns=constants.BRANDS_SEARCH_TERM_METRICS,
            timeUnit="DAILY",
        )

    @il.asset(
        schema=schemas.BrandsTargetingStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def brands_targeting_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Brand campaign performance based on targeting criteria."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="sbTargeting",
            adProduct="SPONSORED_BRANDS",
            groupBy=["targeting"],
            columns=constants.BRANDS_TARGETING_METRICS,
            timeUnit="DAILY",
        )

    @il.asset(
        schema=schemas.BrandsPurchasedProductsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def brands_purchased_products_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Products purchased through brand campaigns."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="sbPurchasedProduct",
            adProduct="SPONSORED_BRANDS",
            groupBy=["purchasedAsin"],
            columns=constants.BRANDS_PURCHASED_PRODUCT_METRICS,
            timeUnit="DAILY",
        )

    @il.asset(
        schema=schemas.BrandsGrossAndInvalidTrafficStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def brands_gross_and_invalid_traffic_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Traffic quality metrics for brand campaigns."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="sbGrossAndInvalids",
            adProduct="SPONSORED_BRANDS",
            groupBy=["campaign"],
            columns=constants.BRANDS_GROSS_AND_INVALID_TRAFFIC_METRICS,
            timeUnit="DAILY",
        )

    @il.asset(
        schema=schemas.BrandsPlacementsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def brands_placements_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Campaign performance by ad placement."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="sbCampaignPlacement",
            adProduct="SPONSORED_BRANDS",
            groupBy=["campaignPlacement"],
            columns=constants.BRANDS_PLACEMENT_METRICS,
            timeUnit="DAILY",
        )

    @il.asset(
        schema=schemas.BrandsAdGroupsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def brands_ad_groups_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad group performance within brand campaigns."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="sbAdGroup",
            adProduct="SPONSORED_BRANDS",
            groupBy=["adGroup"],
            columns=constants.BRANDS_AD_GROUP_METRICS,
            timeUnit="DAILY",
        )

    # --- Sponsored Television ---

    @il.asset(
        schema=schemas.TelevisionCampaignsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def television_campaigns_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Television campaign performance."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="stCampaigns",
            adProduct="SPONSORED_TELEVISION",
            groupBy=["campaign"],
            columns=constants.TELEVISION_CAMPAIGN_METRICS,
            timeUnit="DAILY",
        )

    @il.asset(
        schema=schemas.TelevisionTargetingStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def television_targeting_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Television campaign performance by targeting."""
        return self.connection.api.report(
            self.profile_id,
            context.partition_date,
            reportTypeId="stTargeting",
            adProduct="SPONSORED_TELEVISION",
            groupBy=["targeting"],
            columns=constants.TELEVISION_TARGETING_METRICS,
            timeUnit="DAILY",
        )
