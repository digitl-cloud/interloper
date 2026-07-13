"""Tests for Connection, OAuthConnection and RefreshTokenOAuthConnection."""

from typing import ClassVar

import pytest
from pydantic_settings import SettingsConfigDict

from interloper.connection import Connection, OAuthConnection, RefreshTokenOAuthConnection
from interloper.oauth import OAuthConfig
from interloper.resource import InputField, SecretField
from interloper.utils.concurrency import invoke


class TestConnection:
    def test_definition_without_oauth(self):
        class Plain(Connection):
            host: str = "localhost"

        definition = Plain.definition()

        assert definition.provider is None
        assert "x-oauth" not in definition.config_schema

    def test_plain_connection_ignores_oauth_classvar(self):
        # oauth machinery lives on OAuthConnection; a plain Connection injects no
        # x-oauth, so the schema stays oauth-free.
        class WithOAuth(Connection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("facebook", scope="ads_read")

            access_token: str = "t"

        definition = WithOAuth.definition()

        assert definition.provider is None
        assert "x-oauth" not in definition.config_schema


class TestOAuthConnection:
    def test_declares_no_credential_fields(self):
        # The base carries no trio — custom-shape connections declare their own.
        assert not ({"client_id", "client_secret", "refresh_token"} & set(OAuthConnection.model_fields))

    def test_custom_fields_mapping_injected(self):
        # A connection with its own field names maps them via OAuthConfig.fields;
        # the mapping is exposed verbatim for the form.
        class FbConn(OAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig(
                "facebook",
                scope="ads_read",
                fields={"client_id": "app_id", "client_secret": "app_secret", "refresh_token": "access_token"},
            )

            access_token: str = SecretField()
            app_id: str = InputField("")
            app_secret: str = SecretField("")

        definition = FbConn.definition()

        assert definition.provider == "facebook"
        assert definition.config_schema["x-oauth"]["scope"] == "ads_read"
        assert definition.config_schema["x-oauth"]["fields"] == {
            "client_id": "app_id",
            "client_secret": "app_secret",
            "refresh_token": "access_token",
        }

    def test_partial_fields_mapping(self):
        # A token-only connection maps just the refresh_token role.
        class TkConn(OAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("tiktok", fields={"refresh_token": "access_token"})

            access_token: str = SecretField()

        assert TkConn.definition().config_schema["x-oauth"]["fields"] == {"refresh_token": "access_token"}

    def test_env_credential_reads_prefixed_env(self, monkeypatch: pytest.MonkeyPatch):
        # A custom-shape connection resolves its own credential fields by suffix
        # from INTERLOPER_<PROVIDER>_<SUFFIX> via this helper (see facebook_ads).
        monkeypatch.setenv("INTERLOPER_FACEBOOK_CLIENT_ID", "fb-id")

        class FbConn(OAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("facebook", fields={"client_id": "app_id"})
            model_config = SettingsConfigDict(env_prefix="fb_helper_test_")

            app_id: str = InputField("")

        conn = FbConn()

        assert conn.env_credential("CLIENT_ID") == "fb-id"
        assert conn.env_credential("CLIENT_SECRET") is None


class TestRefreshTokenOAuthConnection:
    def test_declares_standard_trio(self):
        assert {"client_id", "client_secret", "refresh_token"} <= set(RefreshTokenOAuthConnection.model_fields)

    def test_default_mapping_and_widgets(self):
        class MyConn(RefreshTokenOAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("linkedin", scope="r_ads")

            account_id: str = "a"

        definition = MyConn.definition()
        properties = definition.config_schema["properties"]

        assert definition.provider == "linkedin"
        assert definition.config_schema["x-oauth"]["fields"] == {
            "client_id": "client_id",
            "client_secret": "client_secret",
            "refresh_token": "refresh_token",
        }
        assert {"client_id", "client_secret", "refresh_token", "account_id"} <= set(properties)
        assert properties["client_id"]["x-widget"] == "text"
        assert properties["client_secret"]["x-widget"] == "password"

    def test_oauth_credentials_resolved_from_provider_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("INTERLOPER_AMAZON_CLIENT_ID", "in-house-id")
        monkeypatch.setenv("INTERLOPER_AMAZON_CLIENT_SECRET", "in-house-secret")

        class AmazonConn(RefreshTokenOAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("amazon")
            model_config = SettingsConfigDict(env_prefix="amazon_conn_test_")

        conn = AmazonConn(refresh_token="rt")

        assert (conn.client_id, conn.client_secret) == ("in-house-id", "in-house-secret")
        assert conn.refresh_token == "rt"

    def test_explicit_oauth_credentials_override_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("INTERLOPER_AMAZON_CLIENT_ID", "in-house-id")
        monkeypatch.setenv("INTERLOPER_AMAZON_CLIENT_SECRET", "in-house-secret")

        class AmazonConn(RefreshTokenOAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("amazon")
            model_config = SettingsConfigDict(env_prefix="amazon_conn_test2_")

        conn = AmazonConn(refresh_token="rt", client_id="my-id", client_secret="my-secret")

        # A per-connection override always wins over the in-house env credentials.
        assert (conn.client_id, conn.client_secret) == ("my-id", "my-secret")


class TestConnectionCheck:
    def test_base_check_not_implemented(self):
        class Plain(Connection):
            host: str = "localhost"

        assert Plain.checkable() is False
        assert Plain.definition().checkable is False
        with pytest.raises(NotImplementedError):
            Plain().check()

    def test_sync_check_override(self):
        class Checked(Connection):
            api_key: str = "k"

            def check(self) -> bool:
                return self.api_key == "k"

        assert Checked.checkable() is True
        assert Checked.definition().checkable is True
        assert Checked().check() is True

    async def test_async_check_override(self):
        class Checked(Connection):
            async def check(self) -> bool:
                return True

        assert Checked.checkable() is True
        assert await invoke(Checked().check) is True

    def test_oauth_connection_definition_carries_checkable(self):
        # The x-oauth enrichment chain must not lose the checkable flag.
        class Checked(RefreshTokenOAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("amazon")
            model_config = SettingsConfigDict(env_prefix="amazon_conn_check_")

            def check(self) -> bool:
                return True

        assert Checked.definition().checkable is True
