"""Tests for the OAuth provider registry."""

from typing import ClassVar

import pytest

from interloper.oauth import (
    DEFAULT_TOKEN_PARAMS,
    PROVIDERS,
    OAuthAppCredentials,
    OAuthProvider,
    is_provider_configured,
    provider_env_name,
    provider_env_names,
    token_params,
)


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


class TestAppCredentials:
    """In-house app credentials: the INTERLOPER_<PROVIDER>_* env convention."""

    TRIO: ClassVar[dict[str, str]] = {
        "INTERLOPER_ACME_CLIENT_ID": "id",
        "INTERLOPER_ACME_CLIENT_SECRET": "secret",
        "INTERLOPER_ACME_REDIRECT_URI": "https://cb",
    }

    def test_env_names_own_the_convention(self):
        assert provider_env_name("acme", "client_id") == "INTERLOPER_ACME_CLIENT_ID"
        assert provider_env_names("acme") == {
            "client_id": "INTERLOPER_ACME_CLIENT_ID",
            "client_secret": "INTERLOPER_ACME_CLIENT_SECRET",
            "redirect_uri": "INTERLOPER_ACME_REDIRECT_URI",
        }

    def test_from_env_resolves_complete_trio(self, monkeypatch):
        for name, value in self.TRIO.items():
            monkeypatch.setenv(name, value)
        creds = OAuthAppCredentials.from_env("acme")
        assert creds == OAuthAppCredentials(client_id="id", client_secret="secret", redirect_uri="https://cb")
        assert is_provider_configured("acme")

    def test_partial_trio_resolves_to_nothing(self, monkeypatch):
        for name, value in list(self.TRIO.items())[:2]:
            monkeypatch.setenv(name, value)
        assert OAuthAppCredentials.from_env("acme") is None
        assert not is_provider_configured("acme")

    def test_empty_value_counts_as_unset(self, monkeypatch):
        for name, value in self.TRIO.items():
            monkeypatch.setenv(name, value)
        monkeypatch.setenv("INTERLOPER_ACME_CLIENT_SECRET", "")
        assert OAuthAppCredentials.from_env("acme") is None

    def test_unset_provider_is_not_configured(self):
        assert OAuthAppCredentials.from_env("acme") is None
        assert not is_provider_configured("acme")
