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
_CUSTOMER_QUERY = "SELECT customer.id, customer.descriptive_name, customer.manager FROM customer LIMIT 1"
_CLIENTS_QUERY = (
    "SELECT customer_client.id, customer_client.descriptive_name, customer_client.manager "
    "FROM customer_client WHERE customer_client.level > 0"
)
_LOOKUP_CONCURRENCY = 10


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
        """List the client accounts this connection can report on.

        Backs the source's ``customer_id`` ``FetchField``, through the same
        engine as the assets. A customer reached directly is offered by its
        name; a client account of an accessible manager is offered as
        ``<name> (via <manager>)``, that manager being the source's
        ``login_customer_id``. One reached both ways is offered once, directly.
        Manager accounts report no metrics, so they are offered by ``managers``
        instead, and a customer that refuses to be named (suspended, cancelled)
        is offered under its id.

        Returns:
            The options for the field's dropdown.

        Raises:
            ConnectionCheckError: If a manager refuses to list its client
                accounts, carrying Google's reason.
        """
        accessible = await self._accessible()
        managers = [customer for customer in accessible if customer.get("manager")]
        options = {
            customer["id"]: {"customer_id": customer["id"], "name": self._name(customer)}
            for customer in accessible
            if not customer.get("manager")
        }
        try:
            listings = await il.bounded_gather(
                (self.api.search_stream(manager["id"], _CLIENTS_QUERY) for manager in managers),
                limit=_LOOKUP_CONCURRENCY,
            )
        except GoogleApiError as error:
            raise ConnectionCheckError(str(error)) from error
        for manager, rows in zip(managers, listings, strict=True):
            for row in rows:
                client = row["customerClient"]
                if not client.get("manager"):
                    name = f"{self._name(client)} (via {self._name(manager)})"
                    options.setdefault(client["id"], {"customer_id": client["id"], "name": name})
        return list(options.values())

    @il.fetch_field_provider
    async def managers(self) -> list[dict[str, str]]:
        """List the manager accounts this connection can access.

        Backs the source's ``login_customer_id`` ``FetchField``: the manager a
        client account is reached through, as ``customers`` names it.

        Returns:
            The options for the field's dropdown.
        """
        return [
            {"customer_id": customer["id"], "name": self._name(customer)}
            for customer in await self._accessible()
            if customer.get("manager")
        ]

    async def _accessible(self) -> list[dict[str, Any]]:
        """Name every customer these credentials reach directly.

        Returns:
            Each accessible customer's ``customer`` as Google sends it (``id``,
            ``descriptiveName``, ``manager``), in the listing's order, or its
            ``id`` alone for a customer that refuses to be named.

        Raises:
            ConnectionCheckError: If Google refuses the token exchange or the
                listing, lists no customer, or refuses to name every customer
                it lists, carrying Google's reason, which names no credential.
        """
        try:
            listing = await self.api.get("/customers:listAccessibleCustomers")
        except (GoogleApiError, OAuth2TokenError) as error:
            raise ConnectionCheckError(str(error)) from error
        customer_ids = [resource_name.split("/")[-1] for resource_name in listing.get("resourceNames") or []]
        if not customer_ids:
            raise ConnectionCheckError("Google Ads lists no customer these credentials can access")

        answers = await il.bounded_gather(
            (self._customer(customer_id) for customer_id in customer_ids), limit=_LOOKUP_CONCURRENCY
        )
        refusals = [answer for answer in answers if isinstance(answer, GoogleApiError)]
        # credentials every customer refuses can run no report, whatever the listing says
        if len(refusals) == len(customer_ids):
            raise ConnectionCheckError(str(refusals[0])) from refusals[0]
        return [
            {"id": customer_id} if isinstance(answer, GoogleApiError) else answer
            for customer_id, answer in zip(customer_ids, answers, strict=True)
        ]

    async def _customer(self, customer_id: str) -> dict[str, Any] | GoogleApiError:
        """Read one accessible customer's name and whether it is a manager.

        Args:
            customer_id: The customer, as the accessible customers listing names it.

        Returns:
            The customer as Google sends it, its id alone when Google sends no row, or Google's refusal.
        """
        try:
            rows = await self.api.search_stream(customer_id, _CUSTOMER_QUERY)
        except GoogleApiError as refusal:
            return refusal
        return rows[0]["customer"] if rows else {"id": customer_id}

    @staticmethod
    def _name(customer: dict[str, Any]) -> str:
        """Name a customer for a dropdown.

        Args:
            customer: A ``customer`` or ``customerClient`` as Google sends it.

        Returns:
            Its descriptive name, or its id when it has none.
        """
        return customer.get("descriptiveName") or customer["id"]

    async def check(self) -> bool:
        """Prove the credentials work by running the ``customers`` lookup.

        Returns:
            True — any credential failure raises out of the lookup, Google's refusal as a `ConnectionCheckError`.
        """
        await self.customers()
        return True
