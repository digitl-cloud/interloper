"""Tests for ``interloper_api.auth.base``: which providers a deployment offers.

The session the login installs is exercised through the routes in
``tests/routes/test_auth.py``.
"""

from __future__ import annotations

import pytest

from interloper_api.auth import GoogleProvider, Login, MicrosoftProvider
from interloper_api.settings import ApiSettings

GOOGLE = "https://accounts.google.com"
MICROSOFT = "https://login.microsoftonline.com/0c1ea3a1-36e3-4a64-8a3a-2b3e8b0e2a51/v2.0"


def _login() -> Login:
    providers = [
        GoogleProvider(client_id="gid", client_secret="g", public_url="https://app.example.com"),
        MicrosoftProvider(client_id="mid", client_secret="m", public_url="https://app.example.com"),
    ]
    return Login(providers, encryption_key="key", cookie_secure=False)


class TestFromSettings:
    def test_offers_every_configured_provider(self, fake_settings: ApiSettings) -> None:
        login = Login.from_settings(fake_settings)

        assert login is not None
        assert list(login.providers) == ["google"]

    def test_offers_microsoft_after_google_when_both_are_configured(self, fake_settings: ApiSettings) -> None:
        fake_settings.auth.microsoft_client_id = "mid"
        fake_settings.auth.microsoft_client_secret = "msecret"

        login = Login.from_settings(fake_settings)

        assert login is not None
        assert list(login.providers) == ["google", "microsoft"]

    def test_microsoft_alone_offers_a_login(self, fake_settings: ApiSettings) -> None:
        fake_settings.auth.google_client_id = ""
        fake_settings.auth.microsoft_client_id = "mid"
        fake_settings.auth.microsoft_client_secret = "msecret"

        login = Login.from_settings(fake_settings)

        assert login is not None
        assert list(login.providers) == ["microsoft"]

    def test_no_configured_provider_offers_no_login(self, fake_settings: ApiSettings) -> None:
        fake_settings.auth.google_client_id = ""

        assert Login.from_settings(fake_settings) is None

    def test_no_encryption_key_offers_no_login(self, fake_settings: ApiSettings) -> None:
        fake_settings.encryption.key = ""

        assert Login.from_settings(fake_settings) is None


class TestProvidersOf:
    """Which offered providers a set of identities signs in through."""

    @pytest.mark.parametrize(
        ("issuers", "names"),
        [
            pytest.param([GOOGLE], ["google"], id="google"),
            pytest.param([MICROSOFT], ["microsoft"], id="microsoft"),
            pytest.param([MICROSOFT, GOOGLE, GOOGLE], ["google", "microsoft"], id="both, in the order offered"),
            pytest.param(["https://idp.example.com"], [], id="a provider not offered"),
            pytest.param([], [], id="no identity"),
        ],
    )
    def test_names_the_offered_providers_issuing_them(self, issuers: list[str], names: list[str]) -> None:
        assert [provider.name for provider in _login().providers_of(issuers)] == names
