from functools import cached_property
from typing import Any

import httpx
import interloper as il
from pydantic import model_validator
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
        fields={"client_id": "app_id", "client_secret": "app_secret", "refresh_token": "access_token"},
    ),
)
class FacebookAdsConnection(il.OAuthConnection):
    """Facebook Ads (Meta Marketing) API connection.

    Facebook names its OAuth credentials ``app_id`` / ``app_secret`` and uses a
    long-lived ``access_token``, so it subclasses ``OAuthConnection`` and maps
    those field names via ``OAuthConfig.fields``. All three are required;
    ``app_id`` / ``app_secret`` may be supplied by the in-house
    ``INTERLOPER_FACEBOOK_CLIENT_ID`` / ``INTERLOPER_FACEBOOK_CLIENT_SECRET`` (injected
    before validation), and ``access_token`` is filled on sign-in.
    """

    model_config = SettingsConfigDict(env_prefix="facebook_ads_")

    access_token: str = il.SecretField(description="Facebook access token")
    app_id: str = il.InputField(label="App ID", description="Facebook App ID")
    app_secret: str = il.SecretField(description="Facebook App secret")

    @model_validator(mode="before")
    @classmethod
    def resolve_credentials(cls, data: Any) -> Any:
        """Inject blank ``app_id`` / ``app_secret`` from the in-house env creds."""
        if isinstance(data, dict):
            cls.resolve_field(data, "app_id", cls.env_credential("CLIENT_ID"))
            cls.resolve_field(data, "app_secret", cls.env_credential("CLIENT_SECRET"))
        return data

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

    async def check(self) -> bool:
        """Prove the credentials work by running the ``accounts`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.accounts()
        return True
