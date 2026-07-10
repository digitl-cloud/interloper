
import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.linkedin_ads.connection import LinkedinAdsConnection

# -- SOURCE --------------------------------------------------------------------


@il.source(
    resources={"connection": LinkedinAdsConnection},
    tags=["Advertising"],
    normalizer=DataFrameNormalizer(flatten_max_level=1),
    icon="devicon:linkedin",
)
class LinkedinAds(il.Source):
    """LinkedIn Ads advertising platform integration."""

    account_id: str = il.FetchField(
        provider="connection.accounts",
        label_key="name",
        value_key="id",
        description="LinkedIn Ads account",
        discriminator=True,
    )
