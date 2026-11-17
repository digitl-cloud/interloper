from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.snapchat_ads.api import SnapchatAdsApi
from interloper_assets.snapchat_ads.constants import BASE_URL

_TOKEN_BASE_URL = "https://accounts.snapchat.com"
_TOKEN_ENDPOINT = "/login/oauth2/access_token"


@il.connection(
    name="Snapchat Ads",
    icon="mdi:snapchat",
    tags=["Advertising"],
    oauth=il.OAuthConfig("snapchat", scope="snapchat-marketing-api"),
)
class SnapchatAdsConnection(il.RefreshTokenOAuthConnection):
    """Snapchat Ads API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="snapchat_ads_")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """The Snapchat Marketing API client every caller shares, with OAuth2 refresh-token auth.

        Snapchat's token endpoint lives on a different host, so the auth resolves
        it against ``accounts.snapchat.com``; the exchange is async-native (yielded
        into this client's flow), so no blocking refresh runs on the event loop.

        Returns:
            The authenticated client, cached per connection instance.
        """
        return il.AsyncRESTClient(
            BASE_URL,
            auth=il.OAuth2RefreshTokenAuth(
                _TOKEN_BASE_URL,
                self.client_id,
                self.client_secret,
                refresh_token=self.refresh_token,
                token_endpoint=_TOKEN_ENDPOINT,
            ),
        )

    @cached_property
    def api(self) -> SnapchatAdsApi:
        """The Snapchat Marketing API engine every caller shares, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return SnapchatAdsApi(self.client)

    @il.fetch_field_provider
    async def ad_accounts(self) -> list[dict[str, str]]:
        """List the active ad accounts of every organization the connection can see.

        Returns:
            The options for the field's dropdown.
        """
        accounts: list[dict[str, str]] = []
        for organization in await self.api.rows("/me/organizations", "organizations", "organization"):
            organization_id = organization.get("id")
            if not organization_id:
                continue
            path = f"/organizations/{organization_id}/adaccounts"
            for account in await self.api.rows(path, "adaccounts", "adaccount"):
                if account.get("status") == "ACTIVE":
                    accounts.append({"id": account["id"], "name": account.get("name", account["id"])})
        return accounts

    async def check(self) -> bool:
        """Prove the credentials work by running the ``ad_accounts`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.ad_accounts()
        return True
