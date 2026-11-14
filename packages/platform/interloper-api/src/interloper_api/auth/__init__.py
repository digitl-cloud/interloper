"""Signing in: the identity providers people sign in through, over OpenID Connect.

A :class:`LoginProvider` runs one provider's login and turns its verified ID
token into the :class:`Account` it proves; :class:`Login` holds the providers
a deployment offers and installs the session their logins keep.
"""

from interloper_api.auth.base import PROVIDERS, SESSION_COOKIE, SESSION_MAX_AGE, SESSION_PATH, Login
from interloper_api.auth.google import GoogleProvider
from interloper_api.auth.provider import Account, LoginError, LoginProvider

__all__ = [
    "PROVIDERS",
    "SESSION_COOKIE",
    "SESSION_MAX_AGE",
    "SESSION_PATH",
    "Account",
    "GoogleProvider",
    "Login",
    "LoginError",
    "LoginProvider",
]
