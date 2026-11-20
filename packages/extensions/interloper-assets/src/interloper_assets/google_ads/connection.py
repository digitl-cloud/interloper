import os
from functools import cached_property
from typing import Any

import interloper as il
from interloper.errors import ConnectionCheckError
from interloper.rest import OAuth2TokenError
from pydantic import model_validator
from pydantic_settings import SettingsConfigDict

from interloper_assets.google.errors import GoogleApiError
from interloper_assets.google_ads.api import GoogleAdsApi
from interloper_assets.google_ads.constants import BASE_URL

_TOKEN_BASE_URL = "https://oauth2.googleapis.com"
_TOKEN_ENDPOINT = "/token"
_NAME_QUERY = "SELECT customer.id, customer.descriptive_name, customer.status FROM customer LIMIT 1"


@il.connection(
    name="Google Ads",
    icon="logos:google-ads",
    tags=["Advertising"],
    oauth=il.OAuthConfig(
        "google",
        scope="https://www.googleapis.com/auth/adwords",
        fields={
            "client_id": "client_id",
            "client_secret": "client_secret",
            "refresh_token": "refresh_token",
            "developer_token": "developer_token",
        },
    ),
    maturity="beta",
)
class GoogleAdsConnection(il.RefreshTokenOAuthConnection):
    """Google Ads API connection with OAuth2 refresh token auth and a developer token."""

    model_config = SettingsConfigDict(env_prefix="google_ads_")

    developer_token: str = il.SecretField(description="Google Ads API developer token")

    @model_validator(mode="before")
    @classmethod
    def resolve_credentials(cls, data: Any) -> Any:
        """Inject blank credentials from the in-house env before validation.

        Overrides the base to also resolve ``developer_token``, a Google-Ads-
        specific token read from ``INTERLOPER_GOOGLE_ADS_DEVELOPER_TOKEN`` (the
        OAuth trio comes from the provider-scoped ``INTERLOPER_GOOGLE_*``). An
        explicit value overrides the in-house one; when neither the caller nor
        env supplies one, the required check fails.

        Args:
            data: The raw connection input, before field validation.

        Returns:
            The (possibly augmented) input data.
        """
        if isinstance(data, dict):
            cls.resolve_field(data, "client_id", os.environ.get("INTERLOPER_GOOGLE_ADS_CLIENT_ID"))
            cls.resolve_field(data, "client_secret", os.environ.get("INTERLOPER_GOOGLE_ADS_CLIENT_SECRET"))
            cls.resolve_field(data, "developer_token", os.environ.get("INTERLOPER_GOOGLE_ADS_DEVELOPER_TOKEN"))
        return data

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """The Google Ads REST API client every caller shares.

        Every request carries the developer token beside the access token the
        refresh token buys. GAQL is read with a POST, ``googleAds:searchStream``,
        which only reads, so the client repeats a failed POST as it would a GET.

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
            headers={"developer-token": self.developer_token},
            retry=il.HttpRetryPolicy(idempotent_methods={"GET", "POST"}),
        )

    @cached_property
    def api(self) -> GoogleAdsApi:
        """The Google Ads API engine every caller shares, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return GoogleAdsApi(self.client)

    @il.fetch_field_provider
    async def customers(self) -> list[dict[str, str]]:
        """List the customers this connection can access, by name.

        Backs the source's ``customer_id`` ``FetchField``, through the same
        engine as the assets. A customer that refuses to be named (suspended,
        cancelled) is offered under its id.

        Returns:
            The options for the field's dropdown.

        Raises:
            ConnectionCheckError: If Google refuses the token exchange or the
                listing, lists no customer, or refuses to name every customer
                it lists, carrying Google's reason, which names no credential.
        """
        try:
            listing = await self.api.get("/customers:listAccessibleCustomers")
        except (GoogleApiError, OAuth2TokenError) as error:
            raise ConnectionCheckError(str(error)) from error
        resource_names: list[str] = listing.get("resourceNames") or []
        if not resource_names:
            raise ConnectionCheckError("Google Ads lists no customer these credentials can access")

        options: list[dict[str, str]] = []
        refusals: list[GoogleApiError] = []
        for resource_name in resource_names:
            customer_id = resource_name.split("/")[-1]
            try:
                rows = await self.api.search_stream(customer_id, _NAME_QUERY)
            except GoogleApiError as refusal:
                refusals.append(refusal)
                options.append({"customer_id": customer_id, "name": customer_id})
                continue
            for row in rows:
                name = row.get("customer", {}).get("descriptiveName", customer_id)
                options.append({"customer_id": customer_id, "name": name})

        # credentials every customer refuses can run no report, whatever the listing says
        if len(refusals) == len(resource_names):
            raise ConnectionCheckError(str(refusals[0])) from refusals[0]
        return options

    async def check(self) -> bool:
        """Prove the credentials work by running the ``customers`` lookup.

        Returns:
            True — any credential failure raises out of the lookup, Google's refusal as a `ConnectionCheckError`.
        """
        await self.customers()
        return True
