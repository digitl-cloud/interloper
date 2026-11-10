"""Tests for OAuthConfig."""

import pytest

from interloper.errors import ConfigError
from interloper.oauth import OAuthConfig


class TestOAuthConfig:
    def test_resolves_metadata_from_registry(self):
        config = OAuthConfig("amazon", scope="advertising::campaign_management")

        assert config.auth_url == "https://www.amazon.com/ap/oa"
        assert config.label == "Amazon"
        assert config.icon == "icon:amazon"
        assert config.scope == "advertising::campaign_management"

    def test_explicit_overrides_win_over_registry(self):
        config = OAuthConfig("amazon", auth_url="https://other/auth", label="Other", icon="icon:other")

        assert config.auth_url == "https://other/auth"
        assert config.label == "Other"
        assert config.icon == "icon:other"

    def test_unknown_provider_requires_auth_url(self):
        with pytest.raises(ConfigError, match="Unknown OAuth provider: 'nope'"):
            OAuthConfig("nope")

    def test_unknown_provider_with_auth_url_is_allowed(self):
        config = OAuthConfig("nope", auth_url="https://nope/auth")

        assert config.auth_url == "https://nope/auth"
        assert config.label == "Nope"

    def test_fields_default_to_identity_trio(self):
        assert OAuthConfig("amazon").fields == {
            "client_id": "client_id",
            "client_secret": "client_secret",
            "refresh_token": "refresh_token",
        }

    def test_to_schema_ext(self):
        config = OAuthConfig(
            "facebook",
            scope="ads_read",
            fields={"client_id": "app_id", "client_secret": "app_secret", "refresh_token": "access_token"},
        )

        assert config.to_schema_ext() == {
            "provider": "facebook",
            "auth_url": "https://www.facebook.com/v19.0/dialog/oauth",
            "scope": "ads_read",
            "label": "Facebook",
            "icon": "logos:facebook",
            "fields": {"client_id": "app_id", "client_secret": "app_secret", "refresh_token": "access_token"},
        }
