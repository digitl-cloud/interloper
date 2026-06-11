"""Tests for OAuthConfig."""

import pytest

from interloper.errors import ConfigError
from interloper.oauth import OAuthConfig


class TestOAuthConfig:
    def test_resolves_metadata_from_registry(self):
        cfg = OAuthConfig("amazon", scope="advertising::campaign_management")

        assert cfg.auth_url == "https://www.amazon.com/ap/oa"
        assert cfg.label == "Amazon"
        assert cfg.icon == "icon:amazon"
        assert cfg.scope == "advertising::campaign_management"

    def test_explicit_overrides_win_over_registry(self):
        cfg = OAuthConfig("amazon", auth_url="https://other/auth", label="Other", icon="icon:other")

        assert cfg.auth_url == "https://other/auth"
        assert cfg.label == "Other"
        assert cfg.icon == "icon:other"

    def test_unknown_provider_requires_auth_url(self):
        with pytest.raises(ConfigError, match="Unknown OAuth provider: 'nope'"):
            OAuthConfig("nope")

    def test_unknown_provider_with_auth_url_is_allowed(self):
        cfg = OAuthConfig("nope", auth_url="https://nope/auth")

        assert cfg.auth_url == "https://nope/auth"
        assert cfg.label == "Nope"

    def test_default_fields_mapping_is_standard_trio(self):
        cfg = OAuthConfig("amazon")

        assert cfg.fields == {
            "client_id": "client_id",
            "client_secret": "client_secret",
            "refresh_token": "refresh_token",
        }

    def test_to_schema_ext(self):
        cfg = OAuthConfig("facebook", scope="ads_read", fields={"access_token": "access_token"})

        assert cfg.to_schema_ext() == {
            "provider": "facebook",
            "auth_url": "https://www.facebook.com/v19.0/dialog/oauth",
            "scope": "ads_read",
            "label": "Facebook",
            "icon": "logos:facebook",
            "fields": {"access_token": "access_token"},
        }
