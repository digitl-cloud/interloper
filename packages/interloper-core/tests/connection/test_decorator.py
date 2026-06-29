"""Tests for the @connection decorator."""

import pytest

from interloper.connection import Connection, OAuthConnection, connection
from interloper.oauth import OAuthConfig


class TestConnectionDecorator:
    def test_bare_on_plain_class(self):
        @connection
        class MyConn:
            host: str = "localhost"

        assert issubclass(MyConn, Connection)
        assert "host" in MyConn.model_fields

    def test_classvars_from_kwargs(self):
        @connection(key="custom", name="Custom", icon="icon:x", tags=["Tag"])
        class MyConn(Connection):
            url: str = "u"

        assert MyConn.key == "custom"
        assert MyConn.name == "Custom"
        assert MyConn.icon == "icon:x"
        assert MyConn.tags == ["Tag"]

    def test_oauth_kwarg_on_connection_subclass(self):
        @connection(name="Test", oauth=OAuthConfig("linkedin", scope="r_ads"))
        class MyConn(OAuthConnection):
            account_id: str = "a"

        definition = MyConn.definition()

        assert isinstance(MyConn.oauth, OAuthConfig)
        assert definition.provider == "linkedin"
        assert definition.config_schema["x-oauth"]["auth_url"] == "https://www.linkedin.com/oauth/v2/authorization"

    def test_oauth_kwarg_rejected_on_plain_connection(self):
        # OAuth lives on OAuthConnection; a class that resolves to a plain
        # Connection (here a bare class) is rejected.
        with pytest.raises(TypeError, match=r"requires subclassing OAuthConnection"):

            @connection(oauth=OAuthConfig("amazon"))  # ty: ignore[invalid-argument-type]
            class MyConn:
                host: str = "h"

    def test_oauth_kwarg_does_not_become_model_field(self):
        @connection(oauth=OAuthConfig("amazon"))
        class MyConn(OAuthConnection):
            pass

        assert "oauth" not in MyConn.model_fields
        assert "oauth" not in MyConn.definition().config_schema.get("properties", {})

    def test_oauth_fields_mapping_validated_on_subclass_path(self):
        # ClassVars are stamped on already-built OAuthConnection subclasses
        # after the pydantic hooks ran -- the decorator must still validate.
        with pytest.raises(TypeError, match=r"Broken: OAuthConfig.fields .* \['nope'\]"):

            @connection(oauth=OAuthConfig("amazon", fields={"refresh_token": "nope"}))
            class Broken(OAuthConnection):
                pass
