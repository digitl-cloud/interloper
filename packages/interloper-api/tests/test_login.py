"""Tests for ``interloper_api.login``: what a Google login is built from and what it accepts.

The login's flow against Google runs in ``tests/routes/test_auth.py``.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from interloper_api.login import GoogleAccount, GoogleLogin, LoginError


class TestFromSettings:
    def test_a_configured_client_and_key_build_a_login(self, fake_settings: SimpleNamespace) -> None:
        login = GoogleLogin.from_settings(fake_settings)

        assert login is not None
        assert login.redirect_uri == "https://app/api/auth/google/callback"

    @pytest.mark.parametrize(
        ("section", "field"),
        [("auth", "google_client_id"), ("auth", "google_client_secret"), ("secrets", "encryption_key")],
    )
    def test_anything_missing_builds_none(self, fake_settings: SimpleNamespace, section: str, field: str) -> None:
        setattr(getattr(fake_settings, section), field, "")

        assert GoogleLogin.from_settings(fake_settings) is None


class TestGoogleAccount:
    def test_reads_the_account_from_the_claims(self) -> None:
        claims = {"sub": "g-1", "email": "a@example.com", "email_verified": True, "name": "A", "picture": "https://p"}

        assert GoogleAccount.from_claims(claims) == GoogleAccount(
            google_id="g-1", email="a@example.com", name="A", avatar_url="https://p"
        )

    def test_the_name_and_picture_are_optional(self) -> None:
        account = GoogleAccount.from_claims({"sub": "g-1", "email": "a@example.com", "email_verified": True})

        assert account.name is None
        assert account.avatar_url is None

    @pytest.mark.parametrize(
        "claims",
        [
            {"email": "a@example.com", "email_verified": True},
            {"sub": "g-1", "email_verified": True},
            {"sub": "g-1", "email": "a@example.com"},
            {"sub": "g-1", "email": "a@example.com", "email_verified": False},
            {"sub": "g-1", "email": "a@example.com", "email_verified": "true"},
        ],
    )
    def test_an_account_without_a_verified_address_is_refused(self, claims: dict[str, Any]) -> None:
        with pytest.raises(LoginError) as raised:
            GoogleAccount.from_claims(claims)

        assert raised.value.reason == "login_failed"
