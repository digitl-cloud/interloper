"""The sign-in providers a deployment offers, and the session a login keeps between its two legs."""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Iterable
from typing import TYPE_CHECKING

from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware

from interloper_api.auth.google import GoogleProvider
from interloper_api.auth.provider import LoginProvider

if TYPE_CHECKING:
    from interloper_api.settings import ApiSettings

SESSION_COOKIE = "login_state"
SESSION_PATH = "/api/auth"
SESSION_MAX_AGE = 600
_SESSION_KEY_LABEL = b"interloper login state"

PROVIDERS: tuple[type[LoginProvider], ...] = (GoogleProvider,)


class Login:
    """The sign-in providers a deployment offers, and the session their logins keep.

    A login keeps its ``state``, nonce, PKCE verifier and destination in a
    signed session cookie scoped to the login routes, alive for the ten
    minutes a login may take. Every API replica must be able to read a cookie
    another one wrote, so it is signed with a key derived from the instance's
    encryption secret; a deployment without one offers no login.

    Attributes:
        providers: The providers offered, by name.
    """

    def __init__(self, providers: Iterable[LoginProvider], *, encryption_key: str, cookie_secure: bool) -> None:
        """Offer the providers, and derive the session cookie's signing key.

        Args:
            providers: The providers offered.
            encryption_key: The instance's encryption secret, from which the
                session cookie's signing key is derived.
            cookie_secure: Whether the session cookie is sent over HTTPS only.
        """
        self.providers = {provider.name: provider for provider in providers}
        self._cookie_secure = cookie_secure
        self._session_secret = hmac.new(encryption_key.encode(), _SESSION_KEY_LABEL, hashlib.sha256).hexdigest()

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
