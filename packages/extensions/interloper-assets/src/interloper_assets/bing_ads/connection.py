import os
import tempfile
from functools import cached_property
from typing import Any
from xml.etree import ElementTree as ET

import httpx2
import interloper as il
from pydantic import model_validator
from pydantic_settings import SettingsConfigDict

# Microsoft Advertising OAuth + Customer Management SOAP endpoints, which the
# connection talks raw SOAP to over httpx2 instead of the ``bingads`` SDK (see
# ``accounts`` below).
_TOKEN_URL = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
_OAUTH_SCOPE = "https://ads.microsoft.com/msads.manage offline_access"
_CUSTOMER_MANAGEMENT_URL = (
    "https://clientcenter.api.bingads.microsoft.com/Api/CustomerManagement/v13/CustomerManagementService.svc"
)
_CUSTOMER_NS = "https://bingads.microsoft.com/Customer/v13"
_ENTITIES_NS = "https://bingads.microsoft.com/Customer/v13/Entities"


def _local_name(tag: str) -> str:
    """Strip the ``{namespace}`` prefix ElementTree puts on every tag.

    Returns:
        The bare tag name.

    """
    return tag.rsplit("}", 1)[-1]


def _child_text(elem: ET.Element, name: str) -> str | None:
    """Text of the first *direct child* named *name* (not any descendant).

    Returns:
        The child's text, or ``None`` when there is no such child.

    """
    for child in elem:
        if _local_name(child.tag) == name:
            return child.text
    return None


def _first_descendant(root: ET.Element, name: str) -> ET.Element | None:
    for node in root.iter():
        if _local_name(node.tag) == name:
            return node
    return None


def _element_value(element: ET.Element) -> Any:
    """Read one field of a SOAP entity: a leaf's text, or a nested array's records.

    A field with child elements is an array of records, such as an account's
    ``LinkedAgencies`` listing ``CustomerInfo`` records, read as one dict per
    record. A nil field has no text and reads as ``None``.

    Args:
        element: The field's element.

    Returns:
        The text, or a list of ``{field: text}`` dicts.

    """
    if len(element) == 0:
        return element.text
    return [{_local_name(field.tag): field.text for field in record} for record in element]


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
    def _authentication(self) -> Any:
        """OAuth authentication for the bingads SDK.

        Cached so the refresh-token exchange happens once per connection.
        """
        from bingads import OAuthAuthorization, OAuthWebAuthCodeGrant

        oauth_web_auth_code_grant = OAuthWebAuthCodeGrant(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirection_uri=None,
        )
        oauth_tokens = oauth_web_auth_code_grant.request_oauth_tokens_by_refresh_token(self.refresh_token)
        return OAuthAuthorization(
            client_id=oauth_web_auth_code_grant.client_id,
            oauth_tokens=oauth_tokens,
        )

    def authorization_data(self, account_id: str, customer_id: str | None = None) -> Any:
        """Return ``AuthorizationData`` for the Bing Ads SDK scoped to *account_id*.

        The Reporting service authorizes on the account alone; the Bulk
        service also needs the account's parent customer, which the SDK
        otherwise sends as the string ``"None"``.

        Args:
            account_id: The account every call is made for.
            customer_id: The account's parent customer, see ``customer_id``; None leaves it unset.

        Returns:
            The SDK's authorization data.
        """
        from bingads import AuthorizationData

        auth_data = AuthorizationData(
            developer_token=self.developer_token,
            authentication=self._authentication,
        )
        auth_data.account_id = account_id
        auth_data.customer_id = customer_id
        return auth_data

    def reporting_service(self, account_id: str) -> Any:
        """Return a ReportingService client for building report requests."""
        from bingads import ServiceClient

        return ServiceClient("ReportingService", 13, self.authorization_data(account_id))

    def reporting_service_manager(self, account_id: str) -> Any:
        """Return a ReportingServiceManager for downloading reports.

        A fresh ``working_directory`` per manager: the SDK defaults to a
        shared ``/tmp`` path it creates with a racy exists-then-makedirs,
        so concurrent report assets in one pod crash with ``FileExistsError``.
        """
        from bingads.v13.reporting.reporting_service_manager import ReportingServiceManager

        return ReportingServiceManager(
            self.authorization_data(account_id),
            working_directory=tempfile.mkdtemp(prefix="bingads-"),
        )

    def bulk_service_manager(self, account_id: str, customer_id: str) -> Any:
        """Return a BulkServiceManager for downloading the account's entities.

        A fresh ``working_directory`` per manager, for the same reason as
        ``reporting_service_manager``.

        Args:
            account_id: The account whose entities are downloaded.
            customer_id: The account's parent customer, see ``customer_id``.

        Returns:
            The SDK's bulk service manager.
        """
        from bingads.v13.bulk import BulkServiceManager

        return BulkServiceManager(
            self.authorization_data(account_id, customer_id),
            working_directory=tempfile.mkdtemp(prefix="bingads-"),
        )

    async def account(self, account_id: str) -> dict[str, Any]:
        """Fetch one account as Customer Management's ``GetAccount`` returns it.

        Args:
            account_id: The account to fetch.

        Returns:
            The ``AdvertiserAccount``'s fields by their Bing names, see ``_element_value``.

        Raises:
            RuntimeError: If the response carries no account.
        """
        request = (
            f'<GetAccountRequest xmlns="{_CUSTOMER_NS}"><AccountId>{int(account_id)}</AccountId></GetAccountRequest>'
        )
        async with httpx2.AsyncClient(timeout=30) as client:
            access_token = await self._access_token(client)
            response = await self._soap_call(client, access_token, "GetAccount", request)
        account = _first_descendant(response, "Account")
        if account is None:
            raise RuntimeError(f"Bing Ads returned no account for {account_id}")
        return {_local_name(field.tag): _element_value(field) for field in account}

    async def customer_id(self, account_id: str) -> str:
        """Resolve the customer an account belongs to, which the Bulk service requires.

        The source names only the account, so its parent customer is read
        from the account itself rather than configured beside it.

        Args:
            account_id: The account whose customer is resolved.

        Returns:
            The account's parent customer id.

        Raises:
            RuntimeError: If the account names no parent customer.
        """
        customer_id = (await self.account(account_id)).get("ParentCustomerId")
        if not customer_id:
            raise RuntimeError(f"Bing Ads account {account_id} names no parent customer")
        return customer_id

    @il.fetch_field_provider
    async def accounts(self) -> list[dict[str, str]]:
        """Fetch the accounts this connection's token can access.

        Talks raw SOAP over httpx2 rather than the ``bingads`` SDK: fetch
        providers run inside the API process, which installs the connection
        classes but not the heavy SDK extra. Resolves the authenticated user
        (``GetUser``) then lists every account they can reach
        (``SearchAccounts`` filtered by that user id).

        Returns:
            The options for the field's dropdown.

        """
        async with httpx2.AsyncClient(timeout=30) as client:
            access_token = await self._access_token(client)
            user = await self._soap_call(
                client,
                access_token,
                "GetUser",
                f'<GetUserRequest xmlns="{_CUSTOMER_NS}"><UserId i:nil="true"/></GetUserRequest>',
            )
            user_elem = _first_descendant(user, "User")
            if user_elem is None:
                return []
            user_id = _child_text(user_elem, "Id")

            search_body = (
                f'<SearchAccountsRequest xmlns="{_CUSTOMER_NS}">'
                f'<Predicates xmlns:a="{_ENTITIES_NS}">'
                "<a:Predicate><a:Field>UserId</a:Field><a:Operator>Equals</a:Operator>"
                f"<a:Value>{user_id}</a:Value></a:Predicate>"
                "</Predicates>"
                '<Ordering i:nil="true"/>'
                f'<PageInfo xmlns:a="{_ENTITIES_NS}"><a:Index>0</a:Index><a:Size>1000</a:Size></PageInfo>'
                "</SearchAccountsRequest>"
            )
            accounts_root = await self._soap_call(client, access_token, "SearchAccounts", search_body)

        return [
            {
                "account_id": _child_text(account, "Id") or "",
                "customer_id": _child_text(account, "ParentCustomerId") or "",
                "name": f"{_child_text(account, 'Name')} ({_child_text(account, 'Number')})",
            }
            for account in accounts_root.iter()
            if _local_name(account.tag) == "AdvertiserAccount"
        ]

    async def _access_token(self, client: httpx2.AsyncClient) -> str:
        """Exchange the refresh token for an access token the SOAP calls carry.

        Args:
            client: The HTTP client the exchange is made with.

        Returns:
            The access token.
        """
        response = await client.post(
            _TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": _OAUTH_SCOPE,
            },
        )
        response.raise_for_status()
        return response.json()["access_token"]

    async def _soap_call(
        self,
        client: httpx2.AsyncClient,
        access_token: str,
        action: str,
        body: str,
    ) -> ET.Element:
        """POST one Customer Management SOAP action and return the parsed body.

        Returns:
            The parsed SOAP body element.

        """
        envelope = (
            '<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" '
            'xmlns:i="http://www.w3.org/2001/XMLSchema-instance">'
            f'<s:Header xmlns="{_CUSTOMER_NS}">'
            f'<Action mustUnderstand="1">{action}</Action>'
            f"<DeveloperToken>{self.developer_token}</DeveloperToken>"
            f"<AuthenticationToken>{access_token}</AuthenticationToken>"
            "</s:Header>"
            f"<s:Body>{body}</s:Body>"
            "</s:Envelope>"
        )
        response = await client.post(
            _CUSTOMER_MANAGEMENT_URL,
            content=envelope,
            headers={"Content-Type": "text/xml; charset=utf-8", "SOAPAction": action},
        )
        response.raise_for_status()
        return ET.fromstring(response.text)

    async def check(self) -> bool:
        """Prove the credentials work by running the ``accounts`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.accounts()
        return True
