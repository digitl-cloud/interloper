
import interloper as il

from interloper_assets.google_ads.connection import GoogleAdsConnection

# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": GoogleAdsConnection},
    tags=["Advertising"],
    icon="logos:google-ads",
)
class GoogleAds(il.Source):
    """Google Ads advertising platform integration."""

    customer_id: str = il.FetchField(
        endpoint="google-ads/customers",
        depends_on="connection",
        label_key="name",
        value_key="customer_id",
        description="Google Ads customer ID (without hyphens)",
    )
    login_customer_id: str | None = il.InputField(
        default=None,
        description="Manager account customer ID (required if accessing through a manager account)",
    )
