
import interloper as il
from pydantic_settings import SettingsConfigDict


@il.connection(
    name="Search Ads 360",
    icon="devicon:google",
    tags=["Advertising"],
)
class SearchAds360Connection(il.Connection):
    """Search Ads 360 API connection using Google service account credentials.

    Uses ``google-auth`` for lightweight service account authentication.
    The heavy ``google-ads`` SDK is not required.
    """

    model_config = SettingsConfigDict(env_prefix="search_ads_360_")

    service_account_key: str = il.SecretField(description="Google service account key JSON")
