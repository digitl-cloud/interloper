"""Tests for ``interloper_aws.connection``."""

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from interloper_aws import AWSConnection
from interloper_aws import connection as connection_module

_AWS_ENV = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION")


@pytest.fixture(autouse=True)
def _no_ambient_aws_env(monkeypatch):
    """Keep the developer's own AWS environment out of these tests."""
    for name in _AWS_ENV:
        monkeypatch.delenv(name, raising=False)


def _with_client(conn: AWSConnection, client: MagicMock) -> MagicMock:
    session = MagicMock()
    session.client.return_value = client
    object.__setattr__(conn, "session", session)  # noqa: PLC2801 - bypasses pydantic's __setattr__
    return session


class TestFields:
    def test_defaults(self):
        conn = AWSConnection()
        assert conn.access_key_id is None
        assert conn.secret_access_key is None
        assert conn.session_token is None
        assert conn.region == "eu-central-1"

    def test_loads_from_the_standard_environment(self, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret-placeholder")
        monkeypatch.setenv("AWS_SESSION_TOKEN", "token-placeholder")
        monkeypatch.setenv("AWS_REGION", "us-east-1")

        conn = AWSConnection()

        assert conn.access_key_id == "AKIAEXAMPLE"
        assert conn.secret_access_key == "secret-placeholder"
        assert conn.session_token == "token-placeholder"
        assert conn.region == "us-east-1"


class TestSession:
    def test_built_from_the_fields(self):
        conn = AWSConnection(
            access_key_id="AKIAEXAMPLE",
            secret_access_key="secret-placeholder",
            session_token="token-placeholder",
            region="eu-west-1",
        )
        with patch.object(connection_module.boto3.session, "Session") as session_cls:
            assert conn.session is session_cls.return_value
        session_cls.assert_called_once_with(
            aws_access_key_id="AKIAEXAMPLE",
            aws_secret_access_key="secret-placeholder",
            aws_session_token="token-placeholder",
            region_name="eu-west-1",
        )

    def test_without_keys_defers_to_the_default_chain(self):
        with patch.object(connection_module.boto3.session, "Session") as session_cls:
            _ = AWSConnection().session
        session_cls.assert_called_once_with(
            aws_access_key_id=None,
            aws_secret_access_key=None,
            aws_session_token=None,
            region_name="eu-central-1",
        )

    def test_is_cached(self):
        conn = AWSConnection()
        with patch.object(connection_module.boto3.session, "Session") as session_cls:
            assert conn.session is conn.session
        session_cls.assert_called_once()

    def test_client_comes_from_the_session(self):
        conn = AWSConnection()
        client = MagicMock()
        session = _with_client(conn, client)

        assert conn.client("s3") is client
        session.client.assert_called_once_with("s3")


class TestCheck:
    def test_calls_get_caller_identity(self):
        conn = AWSConnection()
        sts = MagicMock()
        session = _with_client(conn, sts)

        assert conn.check() is True
        session.client.assert_called_once_with("sts")
        sts.get_caller_identity.assert_called_once_with()

    def test_bad_credentials_raise(self):
        conn = AWSConnection()
        sts = MagicMock()
        sts.get_caller_identity.side_effect = ClientError(
            {"Error": {"Code": "InvalidClientTokenId", "Message": "bad"}}, "GetCallerIdentity"
        )
        _with_client(conn, sts)

        with pytest.raises(ClientError):
            conn.check()


class TestBuckets:
    def test_sorted_case_insensitively(self):
        conn = AWSConnection()
        s3 = MagicMock()
        s3.list_buckets.return_value = {"Buckets": [{"Name": "zeta"}, {"Name": "Alpha"}, {"Name": "beta"}]}
        session = _with_client(conn, s3)

        assert conn.buckets() == [{"name": "Alpha"}, {"name": "beta"}, {"name": "zeta"}]
        session.client.assert_called_once_with("s3")

    def test_no_buckets(self):
        conn = AWSConnection()
        s3 = MagicMock()
        s3.list_buckets.return_value = {}
        _with_client(conn, s3)

        assert conn.buckets() == []
