
import interloper as il

from interloper_assets.pinterest_ads.connection import PinterestAdsConnection

# -- SOURCE --------------------------------------------------------------------


@il.source(
    tags=["Advertising"],
    icon="logos:pinterest",
)
class PinterestAds(il.Source):
    """Pinterest Ads advertising platform integration."""

    connection: PinterestAdsConnection

    account_id: str = il.FetchField(
        provider="connection.accounts",
        label_key="name",
        value_key="id",
        description="Pinterest Ads account",
        discriminator=True,
    )

    # --- Entity assets ---
