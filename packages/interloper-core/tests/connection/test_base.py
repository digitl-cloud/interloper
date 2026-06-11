"""Tests for Connection and OAuthConnection."""

from typing import ClassVar

import pytest

from interloper.connection import Connection, OAuthConnection
from interloper.oauth import OAuthConfig


class TestConnection:
    def test_definition_without_oauth(self):
        class Plain(Connection):
            host: str = "localhost"

        definition = Plain.definition()

        assert definition.provider is None
        assert "x-oauth" not in definition.config_schema

    def test_oauth_classvar_injected_into_schema(self):
        class WithOAuth(Connection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig(
                "facebook",
                scope="ads_read",
                fields={"access_token": "access_token"},
            )

            access_token: str = "t"

        definition = WithOAuth.definition()

        assert definition.provider == "facebook"
        assert definition.config_schema["x-oauth"]["scope"] == "ads_read"

    def test_oauth_fields_mapping_validated_at_class_build(self):
        with pytest.raises(TypeError, match=r"Broken: OAuthConfig.fields .* \['nope'\]"):

            class Broken(Connection):
                oauth: ClassVar[OAuthConfig] = OAuthConfig("amazon", fields={"refresh_token": "nope"})

                client_id: str = "x"


class TestOAuthConnection:
    def test_declares_standard_trio(self):
        assert {"client_id", "client_secret", "refresh_token"} <= set(OAuthConnection.model_fields)

    def test_subclass_satisfies_default_mapping(self):
        class MyConn(OAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("linkedin", scope="r_ads")

            account_id: str = "a"

        definition = MyConn.definition()

        assert definition.provider == "linkedin"
        properties = definition.config_schema["properties"]
        assert {"client_id", "client_secret", "refresh_token", "account_id"} <= set(properties)
        assert properties["client_secret"]["x-widget"] == "password"
