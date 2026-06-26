
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
        provider="connection.accounts",
        label_key="name",
        value_key="id",
        description="Pinterest Ads account",
    )

    # --- Entity assets ---
