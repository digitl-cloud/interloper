"""Tests for Connection and OAuthConnection."""

from typing import ClassVar

import pytest
from pydantic_settings import SettingsConfigDict

from interloper.connection import Connection, OAuthConnection
from interloper.oauth import OAuthConfig


class TestConnection:
    def test_definition_without_oauth(self):
        class Plain(Connection):
            host: str = "localhost"

        definition = Plain.definition()

        assert definition.provider is None
        assert "x-oauth" not in definition.config_schema

    def test_plain_connection_ignores_oauth_classvar(self):
        # oauth lives on OAuthConnection; a plain Connection neither validates
        # nor injects x-oauth, so the schema stays oauth-free.
        class WithOAuth(Connection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("facebook", scope="ads_read")

            access_token: str = "t"

        definition = WithOAuth.definition()

        assert definition.provider is None
        assert "x-oauth" not in definition.config_schema


class TestOAuthConnection:
    def test_declares_standard_trio(self):
        assert {"client_id", "client_secret", "refresh_token"} <= set(OAuthConnection.model_fields)

    def test_oauth_injected_into_schema(self):
        class MyConn(OAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("facebook", scope="ads_read")

        definition = MyConn.definition()

        assert definition.provider == "facebook"
        assert definition.config_schema["x-oauth"]["scope"] == "ads_read"

    def test_custom_token_shape_tags_its_own_field(self):
        # A long-lived-access-token connection declares its own field and maps
        # it; the union tags it for hiding alongside the (unused) trio.
        class TokenOnly(OAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig(
                "facebook", scope="ads_read", fields={"access_token": "access_token"},
            )

            access_token: str = "t"

        properties = TokenOnly.definition().config_schema["properties"]

        assert properties["access_token"]["x-oauth-managed"] is True
        assert TokenOnly.oauth_managed_fields() == ["client_id", "client_secret", "refresh_token", "access_token"]

    def test_oauth_fields_mapping_validated_at_class_build(self):
        with pytest.raises(TypeError, match=r"Broken: OAuthConfig.fields .* \['nope'\]"):

            class Broken(OAuthConnection):
                oauth: ClassVar[OAuthConfig] = OAuthConfig("amazon", fields={"refresh_token": "nope"})

    def test_subclass_satisfies_default_mapping(self):
        class MyConn(OAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("linkedin", scope="r_ads")

            account_id: str = "a"

        definition = MyConn.definition()

        assert definition.provider == "linkedin"
        properties = definition.config_schema["properties"]
        assert {"client_id", "client_secret", "refresh_token", "account_id"} <= set(properties)
        assert properties["client_secret"]["x-widget"] == "password"

    def test_managed_fields_cover_app_credentials_and_token(self):
        # Sign-in mode hides the whole credential trio: the app credentials
        # resolved from env plus the token-filled refresh_token. The set is
        # derived from the model — only the connection's own fields stay shown.
        class MyConn(OAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("linkedin", scope="r_ads")

            account_id: str = "a"

        assert MyConn.oauth_managed_fields() == ["client_id", "client_secret", "refresh_token"]

        properties = MyConn.definition().config_schema["properties"]
        assert all(properties[f]["x-oauth-managed"] is True for f in ("client_id", "client_secret", "refresh_token"))
        assert "x-oauth-managed" not in properties["account_id"]

    def test_default_mapping_auto_fills_only_refresh_token(self):
        # The app credentials are not pulled from the token exchange; only the
        # per-user refresh token is, so the in-house secret never round-trips.
        assert OAuthConfig("amazon").fields == {"refresh_token": "refresh_token"}

    def test_app_credentials_resolved_from_provider_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("AMAZON_CLIENT_ID", "in-house-id")
        monkeypatch.setenv("AMAZON_CLIENT_SECRET", "in-house-secret")

        class AmazonConn(OAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("amazon")
            model_config = SettingsConfigDict(env_prefix="amazon_conn_test_")

        conn = AmazonConn(refresh_token="rt")

        assert (conn.client_id, conn.client_secret) == ("in-house-id", "in-house-secret")
        assert conn.refresh_token == "rt"

    def test_explicit_app_credentials_override_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("AMAZON_CLIENT_ID", "in-house-id")
        monkeypatch.setenv("AMAZON_CLIENT_SECRET", "in-house-secret")

        class AmazonConn(OAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("amazon")
            model_config = SettingsConfigDict(env_prefix="amazon_conn_test2_")

        conn = AmazonConn(refresh_token="rt", client_id="my-id", client_secret="my-secret")

        # A per-connection override always wins over the in-house env app.
        assert (conn.client_id, conn.client_secret) == ("my-id", "my-secret")
