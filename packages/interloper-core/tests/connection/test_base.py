"""Tests for Connection, OAuthConnection and RefreshTokenOAuthConnection."""

import datetime as dt
from typing import ClassVar

import httpx
import pytest
from pydantic_settings import SettingsConfigDict

from interloper.connection import Connection, OAuthConnection, RefreshTokenOAuthConnection, Renewal
from interloper.oauth import OAuthConfig
from interloper.operation import OperationContext
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
        class FacebookConnection(OAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig(
                "facebook",
                scope="ads_read",
                fields={"client_id": "app_id", "client_secret": "app_secret", "refresh_token": "access_token"},
            )

            access_token: str = SecretField()
            app_id: str = InputField("")
            app_secret: str = SecretField("")

        definition = FacebookConnection.definition()

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

        class FacebookConnection(OAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("facebook", fields={"client_id": "app_id"})
            model_config = SettingsConfigDict(env_prefix="fb_helper_test_")

            app_id: str = InputField("")

        connection = FacebookConnection()

        assert connection.env_credential("CLIENT_ID") == "fb-id"
        assert connection.env_credential("CLIENT_SECRET") is None


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

        class AmazonConnection(RefreshTokenOAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("amazon")
            model_config = SettingsConfigDict(env_prefix="amazon_conn_test_")

        connection = AmazonConnection(refresh_token="rt")

        assert (connection.client_id, connection.client_secret) == ("in-house-id", "in-house-secret")
        assert connection.refresh_token == "rt"

    def test_explicit_oauth_credentials_override_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("INTERLOPER_AMAZON_CLIENT_ID", "in-house-id")
        monkeypatch.setenv("INTERLOPER_AMAZON_CLIENT_SECRET", "in-house-secret")

        class AmazonConnection(RefreshTokenOAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("amazon")
            model_config = SettingsConfigDict(env_prefix="amazon_conn_test2_")

        connection = AmazonConnection(refresh_token="rt", client_id="my-id", client_secret="my-secret")

        # A per-connection override always wins over the in-house env credentials.
        assert (connection.client_id, connection.client_secret) == ("my-id", "my-secret")


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


def _mock_transport(monkeypatch: pytest.MonkeyPatch, handler) -> None:
    """Route the AsyncClient the generic renew constructs through a mock transport."""
    real = httpx.AsyncClient

    def factory(**kwargs):
        kwargs.pop("timeout", None)
        kwargs.pop("follow_redirects", None)
        return real(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", factory)


class TestConnectionRenewal:
    def test_base_renew_not_implemented(self):
        class Plain(Connection):
            host: str = "localhost"

        assert Plain.renewable() is False
        definition = Plain.definition()
        assert definition.renewable is False
        # An inert toggle is dropped from the form schema.
        assert "auto_renew" not in definition.config_schema.get("properties", {})
        with pytest.raises(NotImplementedError):
            Plain().renew()

    def test_renew_override_advertises_renewable(self):
        class Renewed(Connection):
            def renew(self) -> Renewal:
                return Renewal()

        assert Renewed.renewable() is True
        definition = Renewed.definition()
        assert definition.renewable is True
        assert "auto_renew" in definition.config_schema["properties"]
        assert Renewed().auto_renew is True

    async def test_generic_refresh_grant_rotates(self, monkeypatch: pytest.MonkeyPatch):
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(
                200, json={"access_token": "a", "refresh_token": "NEW", "refresh_token_expires_in": 1000}
            )

        _mock_transport(monkeypatch, handler)

        class LinkedinConn(RefreshTokenOAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("linkedin")
            model_config = SettingsConfigDict(env_prefix="linkedin_conn_renew_")

        conn = LinkedinConn(client_id="cid", client_secret="cs", refresh_token="OLD")
        renewal = await conn.renew()

        assert renewal.fields == {"refresh_token": "NEW"}
        assert renewal.expires_in == 1000
        (request,) = requests
        assert str(request.url) == "https://www.linkedin.com/oauth/v2/accessToken"
        body = request.content.decode()
        assert "grant_type=refresh_token" in body
        assert "refresh_token=OLD" in body

    async def test_generic_refresh_grant_without_rotation(self, monkeypatch: pytest.MonkeyPatch):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"access_token": "a", "refresh_token": "OLD"})

        _mock_transport(monkeypatch, handler)

        class LinkedinConn(RefreshTokenOAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("linkedin")
            model_config = SettingsConfigDict(env_prefix="linkedin_conn_norot_")

        renewal = await LinkedinConn(client_id="cid", client_secret="cs", refresh_token="OLD").renew()

        assert renewal.fields == {}
        assert renewal.expires_in is None

    async def test_generic_refresh_grant_basic_auth(self, monkeypatch: pytest.MonkeyPatch):
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json={"access_token": "a"})

        _mock_transport(monkeypatch, handler)

        class PinterestConn(RefreshTokenOAuthConnection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig("pinterest")
            model_config = SettingsConfigDict(env_prefix="pinterest_conn_renew_")

        await PinterestConn(client_id="cid", client_secret="cs", refresh_token="OLD").renew()

        (request,) = requests
        assert request.headers["Authorization"].startswith("Basic ")


class TestRenewalFailureMessage:
    def test_http_status_error_hides_the_url(self):
        # Token exchanges carry credentials as query params; the URL must not
        # survive into the persisted message.
        request = httpx.Request("GET", "https://provider/exchange?client_secret=SECRET")
        error = httpx.HTTPStatusError("boom", request=request, response=httpx.Response(400, request=request))

        message = Connection.renewal_failure_message(error)

        assert message == "The provider rejected the renewal (HTTP 400)."
        assert "SECRET" not in message

    def test_network_errors_collapse_to_category(self):
        assert Connection.renewal_failure_message(httpx.ConnectTimeout("t")) == "The renewal timed out."
        assert Connection.renewal_failure_message(httpx.ConnectError("c")) == "Network error during renewal."

    def test_other_errors_format_through_format_exception(self):
        assert Connection.renewal_failure_message(ValueError("boom")) == "ValueError: boom"


class TestConnectionOperation:
    """The operation template over ``renew``: effects in, effects out."""

    def test_renewal_runs_are_not_billable(self):
        assert Connection.billable is False

    def test_renewal_failures_never_attach_tracebacks(self):
        # Raw provider errors embed credentials in URLs; the runner keeps
        # the traceback out of the failure event for this operation.
        assert Connection.capture_traceback is False

    async def test_execute_returns_rotation_and_schedule_effects(self, monkeypatch: pytest.MonkeyPatch):
        class Renewed(Connection):
            def renew(self) -> Renewal:
                return Renewal(fields={"refresh_token": "NEW"}, expires_in=7200)

        result = await Renewed().execute(OperationContext())

        assert result.error is None
        assert result.config == {"refresh_token": "NEW"}
        assert result.state["last_renewal_error"] is None
        due = dt.datetime.fromisoformat(result.state["next_renewal_at"])
        # expires_in/2 with the reported 7200s validity.
        assert dt.timedelta(minutes=55) < due - dt.datetime.now(dt.timezone.utc) < dt.timedelta(minutes=65)

    async def test_execute_falls_back_to_the_class_interval(self):
        class Renewed(Connection):
            renewal_interval: ClassVar[dt.timedelta] = dt.timedelta(hours=6)

            def renew(self) -> Renewal:
                return Renewal()

        result = await Renewed().execute(OperationContext())

        assert result.config == {}
        due = dt.datetime.fromisoformat(result.state["next_renewal_at"])
        assert dt.timedelta(hours=5) < due - dt.datetime.now(dt.timezone.utc) <= dt.timedelta(hours=6)

    def test_failure_stamps_the_curated_message_and_a_retry_slot(self):
        class Renewed(Connection):
            def renew(self) -> Renewal:
                return Renewal()

        request = httpx.Request("GET", "https://provider/exchange?client_secret=SECRET")
        error = httpx.HTTPStatusError("boom", request=request, response=httpx.Response(400, request=request))

        failure = Renewed().failure(error)

        assert failure.error == "The provider rejected the renewal (HTTP 400)."
        assert failure.state["last_renewal_error"] == failure.error
        assert "SECRET" not in str(failure.state)
        retry_at = dt.datetime.fromisoformat(failure.state["next_renewal_at"])
        assert retry_at > dt.datetime.now(dt.timezone.utc)
