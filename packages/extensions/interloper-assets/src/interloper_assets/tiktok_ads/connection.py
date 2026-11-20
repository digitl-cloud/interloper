import os
from functools import cached_property

import interloper as il
from interloper.errors import ConnectionCheckError
from pydantic_settings import SettingsConfigDict

from interloper_assets.tiktok_ads.api import TiktokAdsApi, TiktokAdsApiError
from interloper_assets.tiktok_ads.constants import BASE_URL


@il.connection(
    name="TikTok Ads",
    icon="logos:tiktok-icon",
    tags=["Advertising"],
    oauth=il.OAuthConfig("tiktok", fields={"refresh_token": "access_token"}),
)
class TiktokAdsConnection(il.OAuthConnection):
    """TikTok Ads (Business API) connection authenticated with a long-lived access token.

    TikTok's token exchange returns a single long-lived ``access_token`` (no
    refresh token, no stored app credentials), so it subclasses
    ``OAuthConnection`` and maps just the ``refresh_token`` role to its
    ``access_token`` field.
    """

    model_config = SettingsConfigDict(env_prefix="tiktok_ads_")

    access_token: str = il.SecretField(description="TikTok Ads API access token")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """The TikTok Ads API client every caller shares.

        Returns:
            The authenticated client, cached per connection instance.
        """
        # The TikTok Business API authenticates via the "Access-Token" header, not Bearer.
        # Static-header auth drops straight into the async client.
        return il.AsyncRESTClient(BASE_URL, headers={"Access-Token": self.access_token}, retry=il.HttpRetryPolicy())

    @cached_property
    def api(self) -> TiktokAdsApi:
        """The TikTok Business API engine every caller shares, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return TiktokAdsApi(self.client)

    @il.fetch_field_provider
    async def advertisers(self) -> list[dict[str, str]]:
        """List the advertisers this connection's access token can access, sorted by name.

        Backs the source's ``advertiser_id`` ``FetchField``. Listing advertisers needs
        the connector app's ``app_id`` / ``secret`` — read from the provider-scoped
        environment (``INTERLOPER_TIKTOK_CLIENT_ID`` / ``INTERLOPER_TIKTOK_CLIENT_SECRET``)
        — alongside the connection's access token. Talks to the v1.3 API over the
        lightweight ``AsyncRESTClient`` (not the SDK) so it runs in the API process.

        TikTok's refusal names the cause (a token issued to another app, a
        revoked token) and carries neither the secret nor the token, so the
        connection check reports it as is.

        Returns:
            The options for the field's dropdown.

        Raises:
            ConnectionCheckError: If TikTok refuses the lookup, carrying its
                code and message.

        """
        app_id = os.environ.get("INTERLOPER_TIKTOK_CLIENT_ID", "")
        secret = os.environ.get("INTERLOPER_TIKTOK_CLIENT_SECRET", "")

        try:
            data = await self.api.get("/oauth2/advertiser/get/", app_id=app_id, secret=secret)
        except TiktokAdsApiError as error:
            raise ConnectionCheckError(str(error)) from error

        advertisers = data.get("list", [])
        results = [
            {"advertiser_id": a["advertiser_id"], "name": a.get("advertiser_name") or a["advertiser_id"]}
            for a in advertisers
        ]
        return sorted(results, key=lambda a: a["name"].lower())

    async def check(self) -> bool:
        """Prove the credentials work by running the ``advertisers`` lookup.

        Returns:
            True — any credential failure raises out of the lookup, TikTok's
            own rejection as a `ConnectionCheckError`.
        """
        await self.advertisers()
        return True
