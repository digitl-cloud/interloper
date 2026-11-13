"""Tests for ``interloper_api.login.google``: what a Google provider is built from and what it accepts.

The login's flow against Google runs in ``tests/routes/test_auth.py``.
"""

from __future__ import annotations

from typing import Any

import pytest

from interloper_api.login import Account, GoogleProvider, LoginError
from interloper_api.settings import ApiSettings

ISSUER = "https://accounts.google.com"


def _provider() -> GoogleProvider:
    return GoogleProvider(client_id="cid", client_secret="secret", public_url="https://app.example.com")


class TestFromSettings:
    def test_a_configured_client_builds_a_provider(self, fake_settings: ApiSettings) -> None:
        provider = GoogleProvider.from_settings(fake_settings)

        assert provider is not None
        assert provider.name == "google"

    def test_the_callback_is_served_on_the_public_url(self, fake_settings: ApiSettings) -> None:
        fake_settings.server.external_url = "https://app.example.com/"

        provider = GoogleProvider.from_settings(fake_settings)

        assert provider is not None
        assert provider.redirect_uri == "https://app.example.com/api/auth/google/callback"

    @pytest.mark.parametrize("field", ["google_client_id", "google_client_secret"])
    def test_a_missing_client_builds_none(self, fake_settings: ApiSettings, field: str) -> None:
        setattr(fake_settings.auth, field, "")

        assert GoogleProvider.from_settings(fake_settings) is None


class TestAccount:
    def test_reads_the_account_from_the_claims(self) -> None:
        claims = {
            "iss": ISSUER,
            "sub": "g-1",
            "email": "a@example.com",
            "email_verified": True,
            "name": "A",
            "picture": "https://p",
        }

        assert _provider().account(claims) == Account(
            issuer=ISSUER, subject="g-1", email="a@example.com", name="A", avatar_url="https://p"
        )

    def test_the_name_and_picture_are_optional(self) -> None:
        account = _provider().account({"iss": ISSUER, "sub": "g-1", "email": "a@example.com", "email_verified": True})

        assert account.name is None
        assert account.avatar_url is None

    @pytest.mark.parametrize(
        "claims",
        [
            {"iss": ISSUER, "email": "a@example.com", "email_verified": True},
            {"iss": ISSUER, "sub": "g-1", "email_verified": True},
            {"iss": ISSUER, "sub": "g-1", "email": "a@example.com"},
            {"iss": ISSUER, "sub": "g-1", "email": "a@example.com", "email_verified": False},
            {"iss": ISSUER, "sub": "g-1", "email": "a@example.com", "email_verified": "true"},
        ],
    )
    def test_an_account_without_a_verified_address_is_refused(self, claims: dict[str, Any]) -> None:
        with pytest.raises(LoginError) as raised:
            _provider().account(claims)

        assert raised.value.reason == "login_failed"
