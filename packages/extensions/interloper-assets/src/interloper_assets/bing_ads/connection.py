import os
from functools import cached_property
from typing import Any

import interloper as il
from pydantic import model_validator
from pydantic_settings import SettingsConfigDict

from interloper_assets.bing_ads.api import BingAdsApi


@il.connection(
    name="Bing Ads",
    icon="icon:bing",
    tags=["Advertising"],
    oauth=il.OAuthConfig(
        "microsoft",
        scope="offline_access https://ads.microsoft.com/msads.manage",
        # ``developer_token`` rides the same in-house path as the credential
        # trio: listing it here hides it in sign-in mode (the form hides every
        # mapped field), and ``resolve_credentials`` fills it from env.
        fields={
            "client_id": "client_id",
            "client_secret": "client_secret",
            "refresh_token": "refresh_token",
            "developer_token": "developer_token",
        },
    ),
)
class BingAdsConnection(il.RefreshTokenOAuthConnection):
    """Bing Ads API connection with OAuth2 refresh token auth.

    Holds only credentials. The account a report targets lives on the
    ``BingAds`` source (``account_id``), so a single connection can serve any
    account its token is authorized for.
    """

    model_config = SettingsConfigDict(env_prefix="bing_ads_")

    developer_token: str = il.SecretField(description="Bing Ads developer token")

    @model_validator(mode="before")
    @classmethod
    def resolve_credentials(cls, data: Any) -> Any:
        """Inject blank credentials from the in-house env before validation.

        Overrides the base to also resolve ``developer_token``, which — unlike
        the provider-scoped OAuth trio (``INTERLOPER_MICROSOFT_*``) — is a
        Bing-specific token read from ``INTERLOPER_BING_ADS_DEVELOPER_TOKEN``.
        An explicit value overrides the in-house one; when neither the caller
        nor env supplies one, the required check fails.

        Returns:
            The (possibly augmented) input data.
        """
        if isinstance(data, dict):
            cls.resolve_field(data, "client_id", cls.env_credential("CLIENT_ID"))
            cls.resolve_field(data, "client_secret", cls.env_credential("CLIENT_SECRET"))
            cls.resolve_field(data, "developer_token", os.environ.get("INTERLOPER_BING_ADS_DEVELOPER_TOKEN"))
        return data

    @cached_property
    def api(self) -> BingAdsApi:
        """The Microsoft Advertising API engine every caller shares.

        Built on the credentials rather than a ``client``: Customer Management
        carries its access token inside each SOAP envelope, and the SDK's
        services sign their own calls.

        Returns:
            The engine, cached per connection instance, so the SDK exchanges the refresh token once per connection.
        """
        return BingAdsApi(
            client_id=self.client_id,
            client_secret=self.client_secret,
            refresh_token=self.refresh_token,
            developer_token=self.developer_token,
        )

    @il.fetch_field_provider
    async def accounts(self) -> list[dict[str, str]]:
        """Fetch the accounts this connection's token can access.

        Resolves the authenticated user (``GetUser``) then lists every
        account they can reach (``SearchAccounts`` filtered by that user id).

        Returns:
            The options for the field's dropdown.
        """
        users = await self.api.call("GetUser", "User", UserId=None)
        if not users:
            return []
        accounts = await self.api.call(
            "SearchAccounts",
            "AdvertiserAccount",
            Predicates={"Predicate": {"Field": "UserId", "Operator": "Equals", "Value": users[0].get("Id")}},
            Ordering=None,
            PageInfo={"Index": 0, "Size": 1000},
        )
        return [
            {
                "account_id": account.get("Id") or "",
                "customer_id": account.get("ParentCustomerId") or "",
                "name": f"{account.get('Name')} ({account.get('Number')})",
            }
            for account in accounts
        ]

    async def check(self) -> bool:
        """Prove the credentials work by running the ``accounts`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.accounts()
        return True
