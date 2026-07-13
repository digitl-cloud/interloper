
import interloper as il

from interloper_assets.search_ads_360.connection import SearchAds360Connection

# -- SOURCE --------------------------------------------------------------------


@il.source(
    name="Search Ads 360",
    tags=["Advertising"],
    icon="devicon:google",
    resources={"connection": SearchAds360Connection},
)
class SearchAds360(il.Source):
    """Search Ads 360 advertising platform integration."""

    manager_customer_id: str = il.InputField(description="SA360 manager account customer ID")
    customer_client_id: str = il.InputField(description="SA360 customer client ID to report on", discriminator=True)
