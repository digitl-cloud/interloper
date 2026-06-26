import os
from functools import cached_property

import httpx
import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.tiktok_ads.constants import BASE_URL


@il.connection(
    name="TikTok Ads",
    icon="logos:tiktok-icon",
    tags=["Advertising"],
    oauth=il.OAuthConfig("tiktok", fields={"access_token": "access_token"}),
)
class TiktokAdsConnection(il.Connection):
    """TikTok Ads (Business API) connection authenticated with a long-lived access token.

    TikTok's token exchange returns a single long-lived ``access_token`` (no
    refresh token), so this stays a plain ``Connection`` with a custom
    ``OAuthConfig.fields`` mapping rather than subclassing ``OAuthConnection``.
    """

    model_config = SettingsConfigDict(env_prefix="tiktok_ads_")

    access_token: str = il.SecretField(description="TikTok Ads API access token")

    @cached_property
    def aclient(self) -> il.AsyncRESTClient:
        # The TikTok Business API authenticates via the "Access-Token" header, not Bearer.
        # Static-header auth drops straight into the async client.
        return il.AsyncRESTClient(BASE_URL, headers={"Access-Token": self.access_token})

    @il.fetch_field_provider
    async def advertisers(self) -> list[dict[str, str]]:
        """List the advertisers this connection's access token can access, sorted by name.

        Backs the source's ``advertiser_id`` ``FetchField``. Listing advertisers needs
        the connector app's ``app_id`` / ``secret`` — read from the provider-scoped
        environment (``TIKTOK_CLIENT_ID`` / ``TIKTOK_CLIENT_SECRET``) — alongside the
        connection's access token. Talks to the v1.3 API over httpx (not the SDK) so it
        runs in the API process.
        """
        app_id = os.environ.get("TIKTOK_CLIENT_ID", "")
        secret = os.environ.get("TIKTOK_CLIENT_SECRET", "")

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                f"{BASE_URL}/oauth2/advertiser/get/",
                params={"app_id": app_id, "secret": secret},
                headers={"Access-Token": self.access_token},
            )
            response.raise_for_status()
            body = response.json()
            if body.get("code") != 0:
                raise RuntimeError(f"TikTok API error: {body.get('message')}")

        advertisers = body.get("data", {}).get("list", [])
        results = [
            {"advertiser_id": a["advertiser_id"], "name": a.get("advertiser_name") or a["advertiser_id"]}
            for a in advertisers
        ]
        return sorted(results, key=lambda a: a["name"].lower())
