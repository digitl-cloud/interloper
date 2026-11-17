"""Tests for ``interloper_api.auth.base``: which providers a deployment offers, and the links it signs.

The session the login installs is exercised through the routes in
``tests/routes/test_auth.py``.
"""

from __future__ import annotations

import base64
from datetime import timedelta
from uuid import uuid4

import pytest

from interloper_api.auth import GoogleProvider, LinkProposal, Login, MicrosoftProvider
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


class TestLinks:
    """A proposed link travels in an emailed link, sealed with a key derived from the instance's secret."""

    @staticmethod
    def _proposal() -> LinkProposal:
        return LinkProposal(
            issuer=MICROSOFT,
            subject="oid-1",
            provider="microsoft",
            profile_id=uuid4(),
            email="ada@contoso.com",
            destination="/jobs",
        )

    def test_a_sealed_link_opens_to_its_proposal(self) -> None:
        login, proposal = _login(), self._proposal()

        assert login.open_link(login.seal_link(proposal)) == proposal

    def test_a_sealed_link_reveals_nothing_of_its_proposal(self) -> None:
        proposal = self._proposal()

        readable = base64.urlsafe_b64decode(_login().seal_link(proposal))

        assert proposal.email.encode() not in readable
        assert proposal.subject.encode() not in readable

    def test_a_tampered_link_opens_to_nothing(self) -> None:
        login = _login()
        token = login.seal_link(self._proposal())

        assert login.open_link(token[:-2] + ("aa" if not token.endswith("aa") else "bb")) is None

    def test_an_expired_link_opens_to_nothing(self) -> None:
        login = _login()

        assert login.open_link(login.seal_link(self._proposal()), max_age=timedelta(seconds=-1)) is None

    def test_a_link_another_instance_sealed_opens_to_nothing(self) -> None:
        other = Login([], encryption_key="another-key", cookie_secure=False)

        assert _login().open_link(other.seal_link(self._proposal())) is None
