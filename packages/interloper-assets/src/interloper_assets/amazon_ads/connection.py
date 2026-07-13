from enum import Enum
from functools import cached_property

import httpx
import interloper as il
from pydantic_settings import SettingsConfigDict


class AmazonAdsAPILocation(Enum):
    NORTH_AMERICA = "NA"
    EUROPE = "EU"
    FAR_EAST = "FE"

    @property
    def api_url(self) -> str:
        return {
            AmazonAdsAPILocation.EUROPE: "https://advertising-api-eu.amazon.com",
            AmazonAdsAPILocation.FAR_EAST: "https://advertising-api-fe.amazon.com",
            AmazonAdsAPILocation.NORTH_AMERICA: "https://advertising-api.amazon.com",
        }[self]

    @property
    def auth_url(self) -> str:
        return {
            AmazonAdsAPILocation.EUROPE: "https://api.amazon.co.uk",
            AmazonAdsAPILocation.FAR_EAST: "https://api.amazon.co.jp",
            AmazonAdsAPILocation.NORTH_AMERICA: "https://api.amazon.com",
        }[self]


@il.connection(
    name="Amazon Ads",
    icon="icon:amazon",
    tags=["Advertising"],
    oauth=il.OAuthConfig("amazon", scope="advertising::campaign_management"),
)
class AmazonAdsConnection(il.RefreshTokenOAuthConnection):
    """Amazon Ads API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="amazon_ads_")

    location: str = il.SelectField(
        options=[
            {"label": "Europe", "value": "EU"},
            {"label": "North America", "value": "NA"},
            {"label": "Far East", "value": "FE"},
        ],
        description="API region",
        discriminator=True,
    )

    @cached_property
    def api_location(self) -> AmazonAdsAPILocation:
        return AmazonAdsAPILocation(self.location)

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
        client.base_url = location.api_url
        client.headers.update({"Amazon-Advertising-API-ClientId": self.client_id})
        return client

    @il.fetch_field_provider
    async def profiles(self) -> list[dict[str, str]]:
        """Fetch Amazon Ads advertising profiles for this connection."""
        location = self.api_location

        async with httpx.AsyncClient(timeout=30) as client:
            token_resp = await client.post(
                f"{location.auth_url}/auth/o2/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            token_resp.raise_for_status()
            access_token = token_resp.json()["access_token"]

            profiles_resp = await client.get(
                f"{location.api_url}/v2/profiles",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Amazon-Advertising-API-ClientId": self.client_id,
                },
            )
            profiles_resp.raise_for_status()
            profiles = profiles_resp.json()

        return [
            {
                "profile_id": str(p["profileId"]),
                "name": f"{p['accountInfo']['name']} ({p['countryCode']})",
                "account_id": p["accountInfo"]["id"],
                "country_code": p["countryCode"],
            }
            for p in profiles
        ]

    async def check(self) -> bool:
        """Prove the credentials work by running the ``profiles`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.profiles()
        return True
