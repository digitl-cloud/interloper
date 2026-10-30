"""Google login, on Authlib's Starlette client.

Authlib runs the OpenID Connect leg: it reads Google's discovery document,
sends the browser to Google with ``state``, a nonce and a PKCE challenge, and
at the callback checks ``state``, exchanges the code and verifies the ID
token's signature, issuer, audience, expiry and nonce against Google's keys.

What it keeps between the two legs lives in a signed session cookie scoped to
the login routes, alive for the ten minutes a login may take. Every API
replica must be able to read a cookie another one wrote, so it is signed with a
key derived from the instance's encryption secret; a deployment without one
has no Google login.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
from dataclasses import dataclass
from typing import Any, Literal

import httpx2
from authlib.integrations.base_client import MismatchingStateError, OAuthError
from authlib.integrations.starlette_client import OAuth
from fastapi import FastAPI
from joserfc.errors import JoseError
from starlette.middleware.sessions import SessionMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse

logger = logging.getLogger(__name__)

GOOGLE_METADATA_URL = "https://accounts.google.com/.well-known/openid-configuration"
SESSION_COOKIE = "login_state"
SESSION_PATH = "/api/auth/google"
SESSION_MAX_AGE = 600
_DESTINATION = "destination"
_SESSION_KEY_LABEL = b"interloper login state"


class LoginError(Exception):
    """A Google login that did not complete.

    Attributes:
        reason: What the login page tells the user: ``login_expired`` when
            the callback does not belong to a login this browser started (or
            it took too long), ``login_failed`` for everything else.
    """

    def __init__(self, reason: Literal["login_expired", "login_failed"]) -> None:
        """Record why the login did not complete.

        Args:
            reason: The login page's error code.
        """
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class GoogleAccount:
    """The Google account a login proved, from its verified ID token.

    Attributes:
        google_id: The account's stable identifier (the token's ``sub``).
        email: The account's address, verified by Google.
        name: The display name, when the account shares one.
        avatar_url: The profile picture, when the account has one.
    """

    google_id: str
    email: str
    name: str | None
    avatar_url: str | None

    @classmethod
    def from_claims(cls, claims: dict[str, Any]) -> GoogleAccount:
        """Read the account from the ID token's claims.

        Args:
            claims: The verified ID token's claims.

        Returns:
            The account.

        Raises:
            LoginError: ``login_failed`` when the token names no account or
                address, or Google has not verified the address: the address
                decides signup and super-admin rights.
        """
        if not claims.get("sub") or not claims.get("email") or claims.get("email_verified") is not True:
            logger.warning("Google login refused: the ID token names no account or no verified address")
            raise LoginError("login_failed")
        return cls(
            google_id=claims["sub"],
            email=claims["email"],
            name=claims.get("name"),
            avatar_url=claims.get("picture"),
        )


class GoogleLogin:
    """The Google client the login routes run, and the session it keeps between them.

    Attributes:
        redirect_uri: The callback URL registered on the Google client.
    """

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        encryption_key: str,
        cookie_secure: bool,
        transport: httpx2.AsyncBaseTransport | None = None,
    ) -> None:
        """Register the Google client.

        Args:
            client_id: The Google OAuth web client's id.
            client_secret: Its secret.
            redirect_uri: The callback URL registered on it.
            encryption_key: The instance's encryption secret, from which the
                session cookie's signing key is derived.
            cookie_secure: Whether the session cookie is sent over HTTPS only.
            transport: The HTTP transport Authlib talks to Google through;
                ``None`` for the network.
        """
        self.redirect_uri = redirect_uri
        self._cookie_secure = cookie_secure
        self._session_secret = hmac.new(encryption_key.encode(), _SESSION_KEY_LABEL, hashlib.sha256).hexdigest()
        client_kwargs: dict[str, Any] = {"scope": "openid email profile", "code_challenge_method": "S256"}
        if transport is not None:
            client_kwargs["transport"] = transport
        self._client: Any = OAuth().register(
            "google",
            client_id=client_id,
            client_secret=client_secret,
            server_metadata_url=GOOGLE_METADATA_URL,
            client_kwargs=client_kwargs,
        )

    @classmethod
    def from_settings(cls, settings: Any) -> GoogleLogin | None:
        """Build the login from the application settings.

        Args:
            settings: The full ``AppSettings``.

        Returns:
            The login, or ``None`` when no Google client or no encryption key
            is configured.
        """
        auth = settings.auth
        if not auth.google_client_id or not auth.google_client_secret or not settings.secrets.encryption_key:
            return None
        return cls(
            client_id=auth.google_client_id,
            client_secret=auth.google_client_secret,
            redirect_uri=auth.google_redirect_uri,
            encryption_key=settings.secrets.encryption_key,
            cookie_secure=auth.cookie_secure,
        )

    def install(self, app: FastAPI) -> None:
        """Add the session the login keeps between its two routes.

        Only the login routes receive its cookie, and it lives as long as a
        login may take.

        Args:
            app: The application.
        """
        app.add_middleware(
            SessionMiddleware,
            secret_key=self._session_secret,
            session_cookie=SESSION_COOKIE,
            max_age=SESSION_MAX_AGE,
            path=SESSION_PATH,
            same_site="lax",
            https_only=self._cookie_secure,
        )

    async def redirect(self, request: Request, destination: str) -> RedirectResponse:
        """Send the browser to Google's account picker.

        Args:
            request: The login request.
            destination: Where to land once the login completes, as asked.

        Returns:
            The redirect to Google.

        Raises:
            LoginError: ``login_failed`` when Google's discovery document
                cannot be read.
        """
        request.session[_DESTINATION] = destination
        try:
            return await self._client.authorize_redirect(request, self.redirect_uri, prompt="select_account")
        except httpx2.HTTPError as error:
            logger.warning("Google login could not start: %s", error)
            raise LoginError("login_failed") from error

    async def complete(self, request: Request) -> tuple[GoogleAccount, str]:
        """Turn Google's callback into the account it proves.

        Args:
            request: The callback request.

        Returns:
            The account and the destination the login asked for, as asked.

        Raises:
            LoginError: ``login_expired`` when the callback's ``state`` is not
                one this browser's session holds; ``login_failed`` when the
                user declined, the exchange failed, or the ID token is
                missing or does not verify.
        """
        destination = request.session.pop(_DESTINATION, "/")
        try:
            token = await self._client.authorize_access_token(request)
        except MismatchingStateError as error:
            raise LoginError("login_expired") from error
        except (OAuthError, JoseError, httpx2.HTTPError) as error:
            logger.warning("Google login failed: %s", error)
            raise LoginError("login_failed") from error
        claims = token.get("userinfo")
        if claims is None:
            logger.warning("Google login failed: the token response carries no ID token")
            raise LoginError("login_failed")
        return GoogleAccount.from_claims(claims), destination
