from functools import cached_property
from typing import TYPE_CHECKING, Any

import interloper as il
from pydantic import model_validator
from pydantic_settings import SettingsConfigDict

from interloper_assets.facebook_ads import constants
from interloper_assets.meta.api import GraphApi

if TYPE_CHECKING:
    from interloper_assets.facebook_ads.api import FacebookGraphApi, FacebookMarketingApi

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
        """Inject blank ``app_id`` / ``app_secret`` from the in-house env creds.

        Args:
            data: The raw init payload, mutated in place when it is a dict.

        Returns:
            The payload, with the blank credential fields resolved.
        """
        if isinstance(data, dict):
            cls.resolve_field(data, "app_id", cls.env_credential("CLIENT_ID"))
            cls.resolve_field(data, "app_secret", cls.env_credential("CLIENT_SECRET"))
        return data

    @cached_property
    def client(self) -> "FacebookGraphApi":
        """The Graph API client every asset request goes through, on the Facebook SDK.

        The SDK is imported on first use, since the API process does not carry it.

        Returns:
            The client, cached per connection instance.
        """
        from interloper_assets.facebook_ads.api import FacebookGraphApi

        return FacebookGraphApi.init(
            app_id=self.app_id,
            app_secret=self.app_secret,
            access_token=self.access_token,
            api_version=f"v{constants.API_VERSION}",
            # the SDK's crash reporter would send our tracebacks to Meta
            crash_log=False,
        )

    @cached_property
    def api(self) -> "FacebookMarketingApi":
        """The Marketing API engine every asset reads through, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        from interloper_assets.facebook_ads.api import FacebookMarketingApi

        return FacebookMarketingApi(self.client)

    @il.fetch_field_provider
    async def accounts(self) -> list[dict[str, str]]:
        """List the active ad accounts reachable by this connection.

        Backs the source's ``account_id`` ``FetchField``. It reads Graph
        through `GraphApi` rather than ``api``, whose client is the Facebook
        SDK, so that it runs in the API process, which omits the heavy SDK
        extras: every page, rate limits waited out, the access token as the
        bearer.

        Returns:
            The options for the field's dropdown.

        """
        async with il.AsyncRESTClient(_GRAPH_URL, auth=il.HTTPBearerAuth(self.access_token), timeout=30) as client:
            fields = ["account_id", "name", "account_status"]
            data = await GraphApi(client).rows("/me/adaccounts", fields=fields, limit=500)

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
