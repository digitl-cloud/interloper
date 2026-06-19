
import interloper as il

from interloper_assets.pinterest_ads.connection import PinterestAdsConnection

# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": PinterestAdsConnection},
    tags=["Advertising"],
    icon="logos:pinterest",
)
class PinterestAds(il.Source):
    """Pinterest Ads advertising platform integration."""

    account_id: str = il.FetchField(
        endpoint="pinterest-ads/accounts",
        depends_on="connection",
        label_key="name",
        value_key="id",
        description="Pinterest Ads account",
    )

    # --- Entity assets (metadata) ---
