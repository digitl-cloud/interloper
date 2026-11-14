"""A sign-in provider: an OpenID Connect identity provider people sign in through.

Authlib runs the OpenID Connect leg: it reads the provider's discovery
document, sends the browser there with ``state``, a nonce and a PKCE
challenge, and at the callback checks ``state``, exchanges the code and
verifies the ID token's signature, issuer, audience, expiry and nonce against
the provider's keys. What it keeps between the two legs lives in the login
session :class:`~interloper_api.auth.Login` installs.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self

import httpx2
from authlib.integrations.base_client import MismatchingStateError, OAuthError
from authlib.integrations.starlette_client import OAuth
from joserfc.errors import JoseError
from starlette.requests import Request
from starlette.responses import RedirectResponse

if TYPE_CHECKING:
    from interloper_api.settings import ApiSettings

logger = logging.getLogger(__name__)


class LoginError(Exception):
    """A login that did not complete.

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
class Account:
    """The account a login proved, from the provider's verified ID token.

    Attributes:
        issuer: The provider's issuer identifier, verified against its
            discovery document.
        subject: The stable identifier the provider names the person by,
            unique within the issuer.
        email: The person's address, verified by the provider.
        name: The display name, when the provider shares one.
        avatar_url: The profile picture, when the provider has one.
    """

    issuer: str
    subject: str
    email: str
    name: str | None
    avatar_url: str | None


class LoginProvider(ABC):
    """An identity provider people sign in through, and the Authlib client that talks to it.

    A subclass names the provider, points at its discovery document, reads its
    client from the settings, and turns verified claims into the account they
    prove, refusing what its own rules do not trust.

    Attributes:
        name: The provider's name in the login routes,
            ``/api/auth/<name>/login`` and ``/api/auth/<name>/callback``.
        label: The provider's name as the login page shows it.
        metadata_url: The provider's OpenID Connect discovery document.
        redirect_uri: The callback URL registered with the provider.
    """

    name: ClassVar[str]
    label: ClassVar[str]
    metadata_url: ClassVar[str]

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        public_url: str,
        transport: httpx2.AsyncBaseTransport | None = None,
    ) -> None:
        """Register the provider's client.

        Args:
            client_id: The OAuth client's id at the provider.
            client_secret: Its secret.
            public_url: The app's public base URL, without a trailing slash;
                the callback URL is the provider's callback route on it.
            transport: The HTTP transport Authlib talks to the provider
                through; ``None`` for the network.
        """
        self.redirect_uri = f"{public_url}/api/auth/{self.name}/callback"
        client_kwargs: dict[str, Any] = {"scope": "openid email profile", "code_challenge_method": "S256"}
        if transport is not None:
            client_kwargs["transport"] = transport
        self._client: Any = OAuth().register(
            self.name,
            client_id=client_id,
            client_secret=client_secret,
            server_metadata_url=self.metadata_url,
            client_kwargs=client_kwargs,
        )

    @classmethod
    @abstractmethod
    def from_settings(cls, settings: ApiSettings) -> Self | None:
        """Build the provider from the application settings.

        Args:
            settings: The API's settings.

        Returns:
            The provider, or ``None`` when the deployment configures no
            client for it.
        """

    @abstractmethod
    def account(self, claims: dict[str, Any]) -> Account:
        """Read the account a verified ID token proves.

        Args:
            claims: The verified ID token's claims.

        Returns:
            The account.

        Raises:
            LoginError: ``login_failed`` when the claims name no account, or
                an address the provider does not vouch for: the address
                decides signup and super-admin rights.
        """

    async def redirect(self, request: Request, destination: str) -> RedirectResponse:
        """Send the browser to the provider's account picker.

        Args:
            request: The login request.
            destination: Where to land once the login completes, as asked.

        Returns:
            The redirect to the provider.

        Raises:
            LoginError: ``login_failed`` when the provider's discovery
                document cannot be read.
        """
        request.session[self._destination_key] = destination
        try:
            return await self._client.authorize_redirect(request, self.redirect_uri, prompt="select_account")
        except httpx2.HTTPError as error:
            logger.warning("%s login could not start: %s", self.label, error)
            raise LoginError("login_failed") from error

    async def complete(self, request: Request) -> tuple[Account, str]:
        """Turn the provider's callback into the account it proves.

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
        destination = request.session.pop(self._destination_key, "/")
        try:
            token = await self._client.authorize_access_token(request)
        except MismatchingStateError as error:
            raise LoginError("login_expired") from error
        except (OAuthError, JoseError, httpx2.HTTPError) as error:
            logger.warning("%s login failed: %s", self.label, error)
            raise LoginError("login_failed") from error
        claims = token.get("userinfo")
        if claims is None:
            logger.warning("%s login failed: the token response carries no ID token", self.label)
            raise LoginError("login_failed")
        return self.account(claims), destination

    @property
    def _destination_key(self) -> str:
        """Name the session entry holding this provider's login destination.

        Returns:
            The key, distinct per provider, as Authlib's own entries are.
        """
        return f"{self.name}_destination"
