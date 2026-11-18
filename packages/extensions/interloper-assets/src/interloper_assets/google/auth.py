"""The service account grant Google's REST clients authenticate with."""

from __future__ import annotations

import time
from typing import Any

import interloper as il
from google.auth import crypt, jwt

_TOKEN_BASE_URL = "https://oauth2.googleapis.com"
_TOKEN_ENDPOINT = "/token"
_ASSERTION_LIFETIME = 600


class ServiceAccountAuth(il.OAuth2Auth):
    """OAuth2 JWT-bearer grant for a Google service account.

    Reuses the base flow (token request yielded into the active client, one
    refresh on a 401) and swaps only the grant: instead of a client secret, each
    exchange carries a freshly signed assertion, since an assertion expires
    after ten minutes while the access token it buys lasts an hour. google-auth
    does the signing only; the exchange goes through httpx2, so no Google SDK
    transport is involved.
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
