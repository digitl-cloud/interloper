"""The sign-in providers a deployment offers, the session a login keeps between its two legs, and the links it seals."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Iterable
from datetime import timedelta
from typing import TYPE_CHECKING

from cryptography.fernet import Fernet, InvalidToken
from fastapi import FastAPI
from interloper_db.crypto import derive_fernet_key
from starlette.middleware.sessions import SessionMiddleware

from interloper_api.auth.google import GoogleProvider
from interloper_api.auth.link import LINK_EXPIRY, LinkProposal
from interloper_api.auth.microsoft import MicrosoftProvider
from interloper_api.auth.provider import LoginProvider

if TYPE_CHECKING:
    from interloper_api.settings import ApiSettings

SESSION_COOKIE = "login_state"
SESSION_PATH = "/api/auth"
SESSION_MAX_AGE = 600
_SESSION_KEY_LABEL = b"interloper login state"
_LINK_KEY_LABEL = b"interloper link"

PROVIDERS: tuple[type[LoginProvider], ...] = (GoogleProvider, MicrosoftProvider)


class Login:
    """The sign-in providers a deployment offers, and the session their logins keep.

    A login keeps its ``state``, nonce, PKCE verifier and destination in a
    signed session cookie scoped to the login routes, alive for the ten
    minutes a login may take. Every API replica must be able to read a cookie
    another one wrote, so it is signed with a key derived from the instance's
    encryption secret; a deployment without one offers no login. The links
    emailed to confirm a :class:`LinkProposal` carry it encrypted, under a key
    derived from the same secret for links alone.

    Attributes:
        providers: The providers offered, by name.
    """

    def __init__(self, providers: Iterable[LoginProvider], *, encryption_key: str, cookie_secure: bool) -> None:
        """Offer the providers, and derive the keys of the session cookie and the links.

        Args:
            providers: The providers offered.
            encryption_key: The instance's encryption secret, from which both
                keys are derived.
            cookie_secure: Whether the session cookie is sent over HTTPS only.
        """
        self.providers = {provider.name: provider for provider in providers}
        self._cookie_secure = cookie_secure
        self._session_secret = hmac.new(encryption_key.encode(), _SESSION_KEY_LABEL, hashlib.sha256).hexdigest()
        link_secret = hmac.new(encryption_key.encode(), _LINK_KEY_LABEL, hashlib.sha256).hexdigest()
        self._links = Fernet(derive_fernet_key(link_secret))

    @classmethod
    def from_settings(cls, settings: ApiSettings) -> Login | None:
        """Offer every provider the settings configure a client for.

        Args:
            settings: The API's settings.

        Returns:
            The login, or ``None`` when no provider is configured or no
            encryption key is.
        """
        providers = [provider_type.from_settings(settings) for provider_type in PROVIDERS]
        configured = [provider for provider in providers if provider is not None]
        if not configured or not settings.encryption.key:
            return None
        return cls(configured, encryption_key=settings.encryption.key, cookie_secure=settings.auth.cookie_secure)

    def seal_link(self, proposal: LinkProposal) -> str:
        """Encrypt a proposal into the token a link emailed to confirm it carries.

        Args:
            proposal: The proposal.

        Returns:
            The token, URL-safe and opaque: it holds the proposal, so nothing
            is stored, and reveals none of it, so the URL carries no address
            or account id.
        """
        return self._links.encrypt(json.dumps(proposal.as_mapping()).encode()).decode()

    def open_link(self, token: str, *, max_age: timedelta = LINK_EXPIRY) -> LinkProposal | None:
        """Read the proposal a link's token carries.

        Args:
            token: The token.
            max_age: How long after signing the token is honoured, an hour by
                default.

        Returns:
            The proposal, or ``None`` when the token was not sealed by this
            instance, was altered, or expired.
        """
        try:
            sealed = self._links.decrypt(token.encode(), ttl=int(max_age.total_seconds()))
        except InvalidToken:
            return None
        return LinkProposal.from_mapping(json.loads(sealed))

    def providers_of(self, issuers: Iterable[str]) -> list[LoginProvider]:
        """Name the offered providers that issue any of a person's identities.

        Args:
            issuers: The issuers of the identities.

        Returns:
            The providers, in the order offered; an issuer no offered
            provider issues names none.
        """
        issuers = set(issuers)
        return [provider for provider in self.providers.values() if any(provider.issues(issuer) for issuer in issuers)]

    def install(self, app: FastAPI) -> None:
        """Add the session a login keeps between its two routes.

        Only the authentication routes receive its cookie, and it lives as
        long as a login may take.

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
