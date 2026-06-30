from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

_API_BASE_URL = "https://adsapi.snapchat.com"
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
        """Async REST client with OAuth2 refresh-token auth.

        Snapchat's token endpoint lives on a different host, so the auth resolves
        it against ``accounts.snapchat.com``; the exchange is async-native (yielded
        into this client's flow), so no blocking refresh runs on the event loop.
        """
        return il.AsyncRESTClient(
            _API_BASE_URL,
            auth=il.OAuth2RefreshTokenAuth(
                _TOKEN_BASE_URL,
                self.client_id,
                self.client_secret,
                refresh_token=self.refresh_token,
                token_endpoint=_TOKEN_ENDPOINT,
            ),
        )

    @il.fetch_field_provider
    async def ad_accounts(self) -> list[dict[str, str]]:
        """Fetch Snapchat Ads ad accounts accessible by the connection.

        Flow: list organizations → list ad accounts per org.
        """
        org_resp = await self.client.get("/v1/me/organizations")
        org_resp.raise_for_status()
        orgs = org_resp.json().get("organizations", [])

        accounts: list[dict[str, str]] = []
        for org_wrapper in orgs:
            org = org_wrapper.get("organization", {})
            org_id = org.get("id")
            if not org_id:
                continue

            acct_resp = await self.client.get(f"/v1/organizations/{org_id}/adaccounts")
            acct_resp.raise_for_status()
            ad_accounts = acct_resp.json().get("adaccounts", [])

            for acct_wrapper in ad_accounts:
                acct = acct_wrapper.get("adaccount", {})
                if acct.get("status") != "ACTIVE":
                    continue
                accounts.append({"id": acct["id"], "name": acct.get("name", acct["id"])})

        return accounts
