import json
import time
from functools import cached_property
from typing import Any

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.search_ads_360.constants import BASE_URL, SCOPES

_TOKEN_BASE_URL = "https://oauth2.googleapis.com"
_TOKEN_ENDPOINT = "/token"
_ASSERTION_LIFETIME = 600


class _ServiceAccountAuth(il.OAuth2Auth):
    """OAuth2 JWT-bearer grant for a Google service account.

    Reuses the base flow (token request yielded into the active client, one
    refresh on a 401) and swaps only the grant: instead of a client secret, each
    exchange carries a freshly signed assertion, since an assertion expires
    after ten minutes while the access token it buys lasts an hour. google-auth
    does the signing only; the exchange goes through httpx2.
    """

    def __init__(self, key_info: dict[str, Any], scopes: list[str]):
        """Build the grant for one service account key.

        Args:
            key_info: The parsed service account key; ``client_email`` is the
                assertion issuer and ``private_key`` signs it.
            scopes: The OAuth scopes the access token is requested for.
        """
        super().__init__(
            base_url=_TOKEN_BASE_URL,
            client_id=key_info["client_email"],
            client_secret="",
            scope=" ".join(scopes),
            token_endpoint=_TOKEN_ENDPOINT,
        )
        self._key_info = key_info

    @property
    def grant_type(self) -> str:
        """The JWT-bearer grant type.

        Returns:
            The OAuth grant type URN sent on every token request.
        """
        return "urn:ietf:params:oauth:grant-type:jwt-bearer"

    @property
    def auth_data(self) -> dict[str, str]:
        """The token request form: the grant type and a newly signed assertion.

        Returns:
            The form fields of the token request.
        """
        from google.auth import crypt, jwt

        signer = crypt.RSASigner.from_service_account_info(self._key_info)
        now = int(time.time())
        payload = {
            "iss": self._client_id,
            "scope": self._scope,
            "aud": _TOKEN_BASE_URL + _TOKEN_ENDPOINT,
            "iat": now,
            "exp": now + _ASSERTION_LIFETIME,
        }
        return {"grant_type": self.grant_type, "assertion": jwt.encode(signer, payload).decode()}


@il.connection(
    name="Search Ads 360",
    icon="devicon:google",
    tags=["Advertising"],
)
class SearchAds360Connection(il.Connection):
    """Search Ads 360 Reporting API connection using Google service account credentials.

    Uses ``google-auth`` for lightweight service account authentication.
    The heavy ``google-ads`` SDK is not required.
    """

    model_config = SettingsConfigDict(env_prefix="search_ads_360_")
    key = "search_ads_360_connection"

    service_account_key: str = il.JsonField(description="Google service account key JSON")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """The Search Ads 360 Reporting API client every caller shares.

        Returns:
            The client authenticated as the service account, cached per connection instance.
        """
        auth = _ServiceAccountAuth(json.loads(self.service_account_key), SCOPES)
        return il.AsyncRESTClient(BASE_URL, auth=auth, timeout=60)
