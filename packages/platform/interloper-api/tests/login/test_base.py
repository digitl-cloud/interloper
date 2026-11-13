"""Tests for ``interloper_api.login.base``: which providers a deployment offers.

The session the login installs is exercised through the routes in
``tests/routes/test_auth.py``.
"""

from __future__ import annotations

from interloper_api.login import Login
from interloper_api.settings import ApiSettings


class TestFromSettings:
    def test_offers_every_configured_provider(self, fake_settings: ApiSettings) -> None:
        login = Login.from_settings(fake_settings)

        assert login is not None
        assert list(login.providers) == ["google"]

    def test_no_configured_provider_offers_no_login(self, fake_settings: ApiSettings) -> None:
        fake_settings.auth.google_client_id = ""

        assert Login.from_settings(fake_settings) is None

    def test_no_encryption_key_offers_no_login(self, fake_settings: ApiSettings) -> None:
        fake_settings.encryption.key = ""

        assert Login.from_settings(fake_settings) is None
