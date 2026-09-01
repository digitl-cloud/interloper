"""Core-shipped OAuth providers.

Each constant is registered through interloper-core's own
``interloper.oauth_providers`` entry points (see ``pyproject.toml``) —
the same mechanism third-party packages use, so the registry loader has
no special cases.

Providers whose dialect deviates from plain RFC 6749 are subclasses right
next to their instance: the class carries the deviation, the instance the
identity.
"""

from __future__ import annotations

import base64
from typing import Any, ClassVar

import httpx

from interloper.oauth.base import OAuthProvider, RefreshTokenResponse


class FacebookProvider(OAuthProvider):
    """Facebook's Graph OAuth dialect: GET token requests, no refresh grant."""

    def authorization_code_request(
        self,
        *,
        code: str,
        redirect_uri: str,
        client_id: str,
        client_secret: str,
    ) -> httpx.Request:
        """Build the authorization-code grant: a GET carrying no ``grant_type``.

        Args:
            code: The authorization code from the consent screen.
            redirect_uri: The redirect URI the code was issued against.
            client_id: The OAuth app's client id.
            client_secret: The OAuth app's client secret.

        Returns:
            The request to send.
        """
        return httpx.Request(
            "GET",
            self.token_url,
            params={
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": client_id,
                "client_secret": client_secret,
            },
        )

    def refresh_token_request(
        self,
        *,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        scope: str | None = None,
    ) -> httpx.Request:
        """Build the renewal: Facebook's ``fb_exchange_token`` grant.

        Facebook has no refresh-token grant — a long-lived access token
        (~60 days) plays the refresh-token role, and renews by being
        exchanged for a fresh long-lived token while still valid.

        Args:
            client_id: The Facebook app id.
            client_secret: The Facebook app secret.
            refresh_token: The current long-lived access token.
            scope: Ignored — the grant takes none.

        Returns:
            The request to send.
        """
        return httpx.Request(
            "GET",
            self.token_url,
            params={
                "grant_type": "fb_exchange_token",
                "client_id": client_id,
                "client_secret": client_secret,
                "fb_exchange_token": refresh_token,
            },
        )

    def parse_refresh_token_response(self, payload: dict[str, Any]) -> RefreshTokenResponse:
        """Read the exchange's response: the fresh token arrives as ``access_token``.

        Args:
            payload: The grant's JSON response body.

        Returns:
            The fresh long-lived token and its validity when reported
            (``expires_in`` here describes the issued token itself).
        """
        return RefreshTokenResponse(
            refresh_token=payload.get("access_token"),
            expires_in=payload.get("expires_in"),
        )


class PinterestProvider(OAuthProvider):
    """Pinterest dialect: client credentials also ride a Basic Authorization header."""

    def authorization_code_request(
        self,
        *,
        code: str,
        redirect_uri: str,
        client_id: str,
        client_secret: str,
    ) -> httpx.Request:
        """Build the standard authorization-code grant with the Basic header added.

        Args:
            code: The authorization code from the consent screen.
            redirect_uri: The redirect URI the code was issued against.
            client_id: The OAuth app's client id.
            client_secret: The OAuth app's client secret.

        Returns:
            The request to send.
        """
        request = super().authorization_code_request(
            code=code, redirect_uri=redirect_uri, client_id=client_id, client_secret=client_secret
        )
        request.headers["Authorization"] = _basic_authorization(client_id, client_secret)
        return request

    def refresh_token_request(
        self,
        *,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        scope: str | None = None,
    ) -> httpx.Request:
        """Build the standard refresh grant with the Basic header added.

        Args:
            client_id: The OAuth app's client id.
            client_secret: The OAuth app's client secret.
            refresh_token: The credential to exchange for a fresh one.
            scope: The connection's declared scope; ignored, as in the base
                grant.

        Returns:
            The request to send.
        """
        request = super().refresh_token_request(
            client_id=client_id, client_secret=client_secret, refresh_token=refresh_token, scope=scope
        )
        request.headers["Authorization"] = _basic_authorization(client_id, client_secret)
        return request


class TikTokProvider(OAuthProvider):
    """TikTok Business dialect: bespoke parameter names, and no refresh flow.

    TikTok Business tokens do not expire, so the dialect has no
    credential-refresh flow — connections on this provider derive as
    non-renewable.
    """

    supports_refresh: ClassVar[bool] = False

    def authorization_code_request(
        self,
        *,
        code: str,
        redirect_uri: str,
        client_id: str,
        client_secret: str,
    ) -> httpx.Request:
        """Build TikTok's authorization-code grant: ``app_id`` / ``secret`` / ``auth_code``.

        Args:
            code: The authorization code from the consent screen.
            redirect_uri: Accepted for signature parity but not sent —
                TikTok validates it at consent time only.
            client_id: The TikTok app id.
            client_secret: The TikTok app secret.

        Returns:
            The request to send.
        """
        return httpx.Request(
            "POST",
            self.token_url,
            json={"app_id": client_id, "secret": client_secret, "auth_code": code},
        )


def _basic_authorization(client_id: str, client_secret: str) -> str:
    """Encode the client credentials as an HTTP Basic ``Authorization`` value.

    Args:
        client_id: The OAuth app's client id.
        client_secret: The OAuth app's client secret.

    Returns:
        The ``Basic <base64>`` header value.
    """
    credentials = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    return f"Basic {credentials}"


AMAZON = OAuthProvider(
    key="amazon",
    auth_url="https://www.amazon.com/ap/oa",
    token_url="https://api.amazon.com/auth/o2/token",
    icon="icon:amazon",
)

CRITEO = OAuthProvider(
    key="criteo",
    auth_url="https://consent.criteo.com/request",
    token_url="https://api.criteo.com/oauth2/token",
    token_encoding="form",
)

FACEBOOK = FacebookProvider(
    key="facebook",
    auth_url="https://www.facebook.com/v19.0/dialog/oauth",
    token_url="https://graph.facebook.com/v19.0/oauth/access_token",
    icon="logos:facebook",
)

GOOGLE = OAuthProvider(
    key="google",
    auth_url="https://accounts.google.com/o/oauth2/v2/auth",
    token_url="https://oauth2.googleapis.com/token",
    icon="devicon:google",
)

LINKEDIN = OAuthProvider(
    key="linkedin",
    auth_url="https://www.linkedin.com/oauth/v2/authorization",
    token_url="https://www.linkedin.com/oauth/v2/accessToken",
    label="LinkedIn",
    icon="devicon:linkedin",
    token_encoding="form",
)

MICROSOFT = OAuthProvider(
    key="microsoft",
    auth_url="https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
    token_url="https://login.microsoftonline.com/common/oauth2/v2.0/token",
    icon="logos:microsoft-icon",
    token_encoding="form",
)

PINTEREST = PinterestProvider(
    key="pinterest",
    auth_url="https://www.pinterest.com/oauth",
    token_url="https://api.pinterest.com/v5/oauth/token",
    icon="logos:pinterest",
    token_encoding="form",
)

SNAPCHAT = OAuthProvider(
    key="snapchat",
    auth_url="https://accounts.snapchat.com/login/oauth2/authorize",
    token_url="https://accounts.snapchat.com/login/oauth2/access_token",
    icon="logos:snapchat",
    token_encoding="form",
)

TIKTOK = TikTokProvider(
    key="tiktok",
    auth_url="https://business-api.tiktok.com/portal/auth",
    token_url="https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token",
    label="TikTok",
    icon="logos:tiktok-icon",
)
