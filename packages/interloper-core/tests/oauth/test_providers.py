"""Tests for the built-in provider dialects (``interloper.oauth.providers``)."""

import json
import urllib.parse

from interloper.oauth import PROVIDERS, RefreshTokenResponse


class TestFacebookDialect:
    """GET token requests; renewal is the ``fb_exchange_token`` grant."""

    def test_authorization_code_grant_is_a_get_without_grant_type(self):
        request = PROVIDERS["facebook"].authorization_code_request(
            code="the-code", redirect_uri="https://cb", client_id="cid", client_secret="cs"
        )
        assert request.method == "GET"
        params = dict(urllib.parse.parse_qsl(request.url.query.decode()))
        assert params == {"code": "the-code", "redirect_uri": "https://cb", "client_id": "cid", "client_secret": "cs"}

    def test_renewal_is_the_fb_exchange_token_grant(self):
        request = PROVIDERS["facebook"].refresh_token_request(
            client_id="app-id", client_secret="app-secret", refresh_token="LONG-LIVED"
        )
        assert request.method == "GET"
        params = dict(urllib.parse.parse_qsl(request.url.query.decode()))
        assert params == {
            "grant_type": "fb_exchange_token",
            "client_id": "app-id",
            "client_secret": "app-secret",
            "fb_exchange_token": "LONG-LIVED",
        }

    def test_fresh_token_arrives_as_access_token(self):
        parsed = PROVIDERS["facebook"].parse_refresh_token_response({"access_token": "FRESH", "expires_in": 5183944})
        assert parsed == RefreshTokenResponse(refresh_token="FRESH", expires_in=5183944)


class TestMicrosoftDialect:
    """Refresh grants carry the connection's scope (AADSTS90023)."""

    def test_refresh_grant_includes_the_scope(self):
        request = PROVIDERS["microsoft"].refresh_token_request(
            client_id="cid", client_secret="cs", refresh_token="OLD", scope="offline_access msads.manage"
        )
        parsed = urllib.parse.parse_qs(request.content.decode())
        assert parsed["scope"] == ["offline_access msads.manage"]
        assert parsed["grant_type"] == ["refresh_token"]

    def test_refresh_grant_without_declared_scope(self):
        request = PROVIDERS["microsoft"].refresh_token_request(client_id="cid", client_secret="cs", refresh_token="OLD")
        assert "scope" not in urllib.parse.parse_qs(request.content.decode())


class TestPinterestDialect:
    """Client credentials also ride a Basic Authorization header."""

    def test_both_grants_carry_the_basic_header(self):
        exchange = PROVIDERS["pinterest"].authorization_code_request(
            code="the-code", redirect_uri="https://cb", client_id="cid", client_secret="cs"
        )
        refresh = PROVIDERS["pinterest"].refresh_token_request(client_id="cid", client_secret="cs", refresh_token="OLD")
        for request in (exchange, refresh):
            assert request.headers["Authorization"].startswith("Basic ")
            assert urllib.parse.parse_qs(request.content.decode())["client_id"] == ["cid"]


class TestTikTokDialect:
    """Bespoke parameter names, no ``grant_type`` or ``redirect_uri``."""

    def test_authorization_code_grant_renames_the_parameters(self):
        request = PROVIDERS["tiktok"].authorization_code_request(
            code="the-code", redirect_uri="https://cb", client_id="cid", client_secret="cs"
        )
        assert request.method == "POST"
        assert json.loads(request.content) == {"app_id": "cid", "secret": "cs", "auth_code": "the-code"}