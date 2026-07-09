"""Tests for the OAuth provider registry."""

import pytest

from interloper.oauth import DEFAULT_TOKEN_PARAMS, PROVIDERS, OAuthProvider, token_params


class TestRegistry:
    """Registry loading: entry points from installed packages."""

    def test_entry_points_are_discovered(self):
        # interloper-core declares the built-in providers as entry points —
        # this asserts the discovery mechanism end to end, without any
        # explicit import or registration call.
        assert {
            "amazon",
            "criteo",
            "facebook",
            "google",
            "linkedin",
            "microsoft",
            "pinterest",
            "snapchat",
            "tiktok",
        } <= set(PROVIDERS.keys())

    def test_lookup_by_key(self):
        assert PROVIDERS["amazon"].auth_url == "https://www.amazon.com/ap/oa"

    def test_unknown_key_raises_actionable_error(self):
        with pytest.raises(KeyError, match="'nope' is not registered"):
            PROVIDERS["nope"]


class TestOAuthProvider:
    def test_label_defaults_to_titlecased_key(self):
        spec = OAuthProvider(key="acme", auth_url="https://a", token_url="https://t")
        assert spec.label == "Acme"

    def test_explicit_label_preserved(self):
        spec = OAuthProvider(key="acme", auth_url="https://a", token_url="https://t", label="ACME Corp")
        assert spec.label == "ACME Corp"

    def test_default_token_params(self):
        spec = OAuthProvider(key="acme", auth_url="https://a", token_url="https://t")
        assert spec.token_params == DEFAULT_TOKEN_PARAMS

    def test_token_params_helper_omits_and_renames(self):
        params = token_params("grant_type", "redirect_uri", code="auth_code")
        assert params == {"code": "auth_code", "client_id": "client_id", "client_secret": "client_secret"}
