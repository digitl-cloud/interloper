"""Tests for the OAuth provider base dialect and the provider registry."""

import json
import urllib.parse
from typing import ClassVar

import pytest

from interloper.oauth import (
    PROVIDERS,
    OAuthAppCredentials,
    OAuthProvider,
    RefreshTokenResponse,
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


class TestTokenFlows:
    """The base dialect: plain RFC 6749 requests, parameterized only by encoding."""

    SPEC: ClassVar[OAuthProvider] = OAuthProvider(key="acme", auth_url="https://a", token_url="https://t/token")

    def test_authorization_code_grant_is_rfc_shaped(self):
        request = self.SPEC.authorization_code_request(
            code="the-code", redirect_uri="https://cb", client_id="cid", client_secret="cs"
        )
        assert (request.method, str(request.url)) == ("POST", "https://t/token")
        assert json.loads(request.content) == {
            "grant_type": "authorization_code",
            "code": "the-code",
            "redirect_uri": "https://cb",
            "client_id": "cid",
            "client_secret": "cs",
        }

    def test_form_encoding_knob(self):
        spec = OAuthProvider(key="acme", auth_url="https://a", token_url="https://t/token", token_encoding="form")
        request = spec.authorization_code_request(
            code="the-code", redirect_uri="https://cb", client_id="cid", client_secret="cs"
        )
        assert request.headers["Content-Type"] == "application/x-www-form-urlencoded"
        assert urllib.parse.parse_qs(request.content.decode())["code"] == ["the-code"]

    def test_refresh_grant_omits_scope(self):
        request = self.SPEC.refresh_token_request(
            client_id="cid", client_secret="cs", refresh_token="OLD", scope="a b"
        )
        assert json.loads(request.content) == {
            "grant_type": "refresh_token",
            "refresh_token": "OLD",
            "client_id": "cid",
            "client_secret": "cs",
        }

    def test_parse_refresh_reads_the_rfc_shape(self):
        parsed = self.SPEC.parse_refresh_token_response(
            {"access_token": "a", "refresh_token": "NEW", "refresh_token_expires_in": 1000, "expires_in": 60}
        )
        assert parsed == RefreshTokenResponse(refresh_token="NEW", expires_in=1000)

    def test_parse_refresh_without_rotation(self):
        assert self.SPEC.parse_refresh_token_response({"access_token": "a"}) == RefreshTokenResponse()


class TestAppCredentials:
    """In-house app credentials: the INTERLOPER_<PROVIDER>_* env convention."""

    TRIO: ClassVar[dict[str, str]] = {
        "INTERLOPER_ACME_CLIENT_ID": "id",
        "INTERLOPER_ACME_CLIENT_SECRET": "secret",
        "INTERLOPER_ACME_REDIRECT_URI": "https://cb",
    }

    def test_env_names_own_the_convention(self):
        assert OAuthAppCredentials.env_name("acme", "client_id") == "INTERLOPER_ACME_CLIENT_ID"
        assert OAuthAppCredentials.env_names("acme") == {
            "client_id": "INTERLOPER_ACME_CLIENT_ID",
            "client_secret": "INTERLOPER_ACME_CLIENT_SECRET",
            "redirect_uri": "INTERLOPER_ACME_REDIRECT_URI",
        }

    def test_from_env_resolves_complete_trio(self, monkeypatch):
        for name, value in self.TRIO.items():
            monkeypatch.setenv(name, value)
        creds = OAuthAppCredentials.from_env("acme")
        assert creds == OAuthAppCredentials(client_id="id", client_secret="secret", redirect_uri="https://cb")
        assert OAuthAppCredentials.is_configured("acme")

    def test_partial_trio_resolves_to_nothing(self, monkeypatch):
        for name, value in list(self.TRIO.items())[:2]:
            monkeypatch.setenv(name, value)
        assert OAuthAppCredentials.from_env("acme") is None
        assert not OAuthAppCredentials.is_configured("acme")

    def test_empty_value_counts_as_unset(self, monkeypatch):
        for name, value in self.TRIO.items():
            monkeypatch.setenv(name, value)
        monkeypatch.setenv("INTERLOPER_ACME_CLIENT_SECRET", "")
        assert OAuthAppCredentials.from_env("acme") is None

    def test_unset_provider_is_not_configured(self):
        assert OAuthAppCredentials.from_env("acme") is None
        assert not OAuthAppCredentials.is_configured("acme")
