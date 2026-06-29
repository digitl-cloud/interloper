from functools import cached_property
from typing import Any

import httpx
import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.facebook_ads import constants

_GRAPH_URL = f"https://graph.facebook.com/v{constants.API_VERSION}"


@il.connection(
    name="Facebook Ads",
    icon="logos:facebook",
    tags=["Advertising"],
    oauth=il.OAuthConfig(
        "facebook",
        scope="ads_read,ads_management",
        fields={
            "access_token": "access_token",
            "client_id": "app_id",
            "client_secret": "app_secret",
        },
    ),
)
class FacebookAdsConnection(il.OAuthConnection):
    """Facebook Ads (Meta Marketing) API connection.

    Facebook names its app credentials ``app_id`` / ``app_secret`` and uses a
    long-lived ``access_token``, so it declares those fields and maps them via
    ``OAuthConfig.fields``, leaving ``OAuthConnection``'s optional credential
    trio unused.
    """

    model_config = SettingsConfigDict(env_prefix="facebook_ads_")

    access_token: str = il.SecretField(description="Facebook access token")
    app_id: str = il.InputField(description="Facebook App ID")
    app_secret: str = il.SecretField(description="Facebook App secret")

    @cached_property
    def api(self) -> Any:
        """Initialise and return the Facebook Ads SDK API client."""
        from facebook_business.api import FacebookAdsApi

        return FacebookAdsApi.init(
            app_id=self.app_id,
            app_secret=self.app_secret,
            access_token=self.access_token,
            api_version=f"v{constants.API_VERSION}",
        )

    @il.fetch_field_provider
    async def accounts(self) -> list[dict[str, str]]:
        """List the active ad accounts reachable by this connection.

        Backs the source's ``account_id`` ``FetchField``. Uses the Graph API
        over httpx (not the SDK) so it runs in the API process, which omits
        the heavy SDK extras.
        """
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                f"{_GRAPH_URL}/me/adaccounts",
                params={
                    "access_token": self.access_token,
                    "fields": "account_id,name,account_status",
                    "limit": "500",
                },
            )
            response.raise_for_status()
            data = response.json().get("data", [])

        return [
            {
                "account_id": account["account_id"],
                "name": account.get("name", account["account_id"]),
            }
            for account in data
            if account.get("account_status") == 1  # ACTIVE only
        ]
