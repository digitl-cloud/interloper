from functools import cached_property

import httpx
import interloper as il
from pydantic_settings import SettingsConfigDict


@il.connection(
    name="Snapchat Ads",
    icon="mdi:snapchat",
    tags=["Advertising"],
    oauth=il.OAuthConfig("snapchat", scope="snapchat-marketing-api"),
)
class SnapchatAdsConnection(il.OAuthConnection):
    """Snapchat Ads API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="snapchat_ads_")

    @cached_property
    def client(self) -> il.RESTClient:
        return il.RESTClient(
            "https://adsapi.snapchat.com",
            auth=il.OAuth2RefreshTokenAuth(
                base_url="https://accounts.snapchat.com",
                token_endpoint="/login/oauth2/access_token",
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
            ),
        )

    @il.fetch_field_provider
    async def ad_accounts(self) -> list[dict[str, str]]:
        """Fetch Snapchat Ads ad accounts accessible by the connection.

        Flow: authenticate → list organizations → list ad accounts per org.
        """
        base_url = "https://adsapi.snapchat.com/v1"
        token_url = "https://accounts.snapchat.com/login/oauth2/access_token"

        async with httpx.AsyncClient(timeout=30) as client:
            token_resp = await client.post(
                token_url,
                data={
                    "grant_type": "refresh_token",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "refresh_token": self.refresh_token,
                },
            )
            token_resp.raise_for_status()
            token = token_resp.json()["access_token"]

        headers = {"Authorization": f"Bearer {token}"}
        async with httpx.AsyncClient(timeout=30, headers=headers) as client:
            # 1. Get organizations
            org_resp = await client.get(f"{base_url}/me/organizations")
            org_resp.raise_for_status()
            orgs = org_resp.json().get("organizations", [])

            # 2. Get ad accounts for each organization
            accounts: list[dict[str, str]] = []
            for org_wrapper in orgs:
                org = org_wrapper.get("organization", {})
                org_id = org.get("id")
                if not org_id:
                    continue

                acct_resp = await client.get(f"{base_url}/organizations/{org_id}/adaccounts")
                acct_resp.raise_for_status()
                ad_accounts = acct_resp.json().get("adaccounts", [])

                for acct_wrapper in ad_accounts:
                    acct = acct_wrapper.get("adaccount", {})
                    if acct.get("status") != "ACTIVE":
                        continue
                    accounts.append({
                        "id": acct["id"],
                        "name": acct.get("name", acct["id"]),
                    })

        return accounts
