"""Sign in with a Google account."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from interloper_api.auth.provider import Account, LoginError, LoginProvider

if TYPE_CHECKING:
    from interloper_api.settings import ApiSettings

logger = logging.getLogger(__name__)


class GoogleProvider(LoginProvider):
    """Google, which names an account by the ID token's ``sub`` for the account's whole life.

    Google vouches for an address only with ``email_verified``, so a login
    without it is refused.
    """

    name = "google"
    label = "Google"
    metadata_url = "https://accounts.google.com/.well-known/openid-configuration"

    @classmethod
    def from_settings(cls, settings: ApiSettings) -> GoogleProvider | None:
        """Build the provider from the deployment's Google OAuth web client.

        Args:
            settings: The API's settings, whose ``auth`` holds the client and
                whose ``server`` the public URL the callback is served on.

        Returns:
            The provider, or ``None`` when no Google client is configured.
        """
        auth = settings.auth
        if not auth.google_client_id or not auth.google_client_secret:
            return None
        return cls(
            client_id=auth.google_client_id,
            client_secret=auth.google_client_secret,
            public_url=settings.server.public_url,
        )

    def account(self, claims: dict[str, Any]) -> Account:
        """Read the Google account a verified ID token proves.

        Args:
            claims: The verified ID token's claims.

        Returns:
            The account.

        Raises:
            LoginError: ``login_failed`` when the token names no account or
                address, or Google has not verified the address.
        """
        if not claims.get("sub") or not claims.get("email") or claims.get("email_verified") is not True:
            logger.warning("Google login refused: the ID token names no account or no verified address")
            raise LoginError("login_failed")
        return Account(
            issuer=claims["iss"],
            subject=claims["sub"],
            email=claims["email"],
            name=claims.get("name"),
            avatar_url=claims.get("picture"),
        )
