from enum import Enum
from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict


class AmazonSellingPartnerLocation(Enum):
    NORTH_AMERICA = "NA"
    EUROPE = "EU"
    FAR_EAST = "FE"

    @property
    def api_url(self) -> str:
        return {
            AmazonSellingPartnerLocation.EUROPE: "https://sellingpartnerapi-eu.amazon.com",
            AmazonSellingPartnerLocation.FAR_EAST: "https://sellingpartnerapi-fe.amazon.com",
            AmazonSellingPartnerLocation.NORTH_AMERICA: "https://sellingpartnerapi-na.amazon.com",
        }[self]

    @property
    def auth_url(self) -> str:
        return {
            AmazonSellingPartnerLocation.EUROPE: "https://api.amazon.co.uk",
            AmazonSellingPartnerLocation.FAR_EAST: "https://api.amazon.co.jp",
            AmazonSellingPartnerLocation.NORTH_AMERICA: "https://api.amazon.com",
        }[self]


@il.connection(
    name="Amazon Selling Partner",
    icon="icon:amazon",
    tags=["E-commerce"],
)
class AmazonSellingPartnerConnection(il.Connection):
    """Amazon Selling Partner API connection with OAuth2 refresh token auth (LWA)."""

    model_config = SettingsConfigDict(env_prefix="amazon_sp_")

    location: str = il.SelectField(
        options=[
            {"label": "Europe", "value": "EU"},
            {"label": "North America", "value": "NA"},
            {"label": "Far East", "value": "FE"},
        ],
        description="API region",
    )
    client_id: str = il.InputField(description="LWA client ID")
    client_secret: str = il.SecretField(description="LWA client secret")
    refresh_token: str = il.SecretField(description="LWA refresh token")

    @cached_property
    def api_location(self) -> AmazonSellingPartnerLocation:
        return AmazonSellingPartnerLocation(self.location)

    @cached_property
    def client(self) -> il.RESTClient:
        location = self.api_location
        client = il.RESTClient(
            location.api_url,
            auth=il.OAuth2RefreshTokenAuth(
                base_url=location.auth_url,
                token_endpoint="/auth/o2/token",
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
            ),
        )
        client.headers.update({"host": location.api_url.replace("https://", "")})
        return client
