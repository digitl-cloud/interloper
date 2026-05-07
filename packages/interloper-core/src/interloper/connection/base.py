"""Connection: a resource for holding connection/credential values with env-loading support."""

from __future__ import annotations

from typing import ClassVar

from pydantic_settings import BaseSettings

from interloper.resource import Resource
from interloper.resource.fields import OAuthConfig


class Connection(BaseSettings, Resource):
    """A resource for database/service connection credentials.

    Extends both ``Resource`` and ``BaseSettings``, so connection values
    can be loaded from environment variables, .env files, or passed
    directly::

        class MyConnection(Connection):
            host: str = "localhost"
            port: int = 5432
            username: str
            password: str

        # Loads USERNAME, PASSWORD from environment if not passed explicitly
        conn = MyConnection()

    Connections that support OAuth can declare an ``oauth`` ClassVar::

        class AmazonAdsConnection(Connection):
            oauth: ClassVar[OAuthConfig] = OAuthConfig(
                provider="amazon",
                auth_url="https://www.amazon.com/ap/oa",
                scope="advertising::campaign_management",
                icon="icon:amazon",
            )

            client_id: str
            client_secret: str = SecretField()
            refresh_token: str = SecretField()
    """

    kind: ClassVar[str] = "connection"
    tags: ClassVar[list[str]] = []
    oauth: ClassVar[OAuthConfig | None] = None
