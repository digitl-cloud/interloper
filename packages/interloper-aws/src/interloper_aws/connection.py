"""AWS connection resource holding IAM credentials."""

from __future__ import annotations

from functools import cached_property
from typing import Any

import boto3
from interloper.connection import Connection, connection
from interloper.resource.fields import InputField, SecretField, fetch_field_provider
from pydantic_settings import SettingsConfigDict


@connection(
    key="aws_connection",
    name="AWS",
    icon="icon:aws",
    tags=["Cloud"],
    maturity="alpha",
)
class AWSConnection(Connection):
    """Connection resource holding AWS credentials.

    With an access key pair, every client signs with it (plus the session
    token, for temporary credentials). Without one, boto3's default
    credential chain applies: environment, shared config, then the instance
    or pod role (EC2 instance profile, EKS IRSA or Pod Identity), so an
    in-cluster deployment needs no stored secret at all. Every field also
    loads from the standard environment variables (``AWS_ACCESS_KEY_ID``,
    ``AWS_SECRET_ACCESS_KEY``, ``AWS_SESSION_TOKEN``, ``AWS_REGION``).
    """

    model_config = SettingsConfigDict(env_prefix="aws_")

    access_key_id: str | None = InputField(
        default=None,
        label="Access key ID",
        description="IAM access key ID; leave empty to use the ambient role",
    )
    secret_access_key: str | None = SecretField(
        default=None,
        label="Secret access key",
        description="IAM secret access key",
    )
    session_token: str | None = SecretField(
        default=None,
        label="Session token",
        description="Session token, for temporary credentials only",
    )
    region: str = InputField(default="eu-central-1", description="AWS region clients are built for")

    @cached_property
    def session(self) -> boto3.session.Session:
        """The boto3 session every client is built from.

        Unset credentials are passed as ``None``, which leaves boto3 to
        resolve them through its default chain.

        Returns:
            The session, cached per connection instance.
        """
        return boto3.session.Session(
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            aws_session_token=self.session_token,
            region_name=self.region,
        )

    def client(self, service: str) -> Any:
        """Build a client for one AWS service from the session.

        Args:
            service: The boto3 service name, such as ``"s3"`` or ``"sts"``.

        Returns:
            The service client.
        """
        return self.session.client(service)

    @fetch_field_provider
    def buckets(self) -> list[dict[str, str]]:
        """List the S3 buckets the credentials can see.

        Backs the S3 destination's ``bucket`` ``FetchField``.

        Returns:
            Bucket options with ``name``, sorted case-insensitively.
        """
        response = self.client("s3").list_buckets()
        results = [{"name": bucket["Name"]} for bucket in response.get("Buckets", [])]
        return sorted(results, key=lambda b: b["name"].lower())

    def check(self) -> bool:
        """Prove the credentials work via STS ``GetCallerIdentity``.

        The call needs no IAM permission, so it isolates a bad credential
        from a missing grant.

        Returns:
            True; invalid credentials raise out of the call instead.
        """
        self.client("sts").get_caller_identity()
        return True
