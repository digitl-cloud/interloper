"""ClickHouse connection resource holding server address and user credentials."""

from __future__ import annotations

from functools import cached_property

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from interloper.connection import Connection, connection
from interloper.resource.fields import InputField, SecretField, fetch_field_provider
from pydantic import Field
from pydantic_settings import SettingsConfigDict

_SYSTEM_DATABASES = ("INFORMATION_SCHEMA", "information_schema", "system")


@connection(
    key="clickhouse_connection",
    name="ClickHouse",
    icon="icon:clickhouse",
    tags=["Database"],
    maturity="alpha",
)
class ClickHouseConnection(Connection):
    """Connection resource holding the address of a ClickHouse server and a user's credentials.

    It talks to ClickHouse over its HTTP interface, which both self-hosted
    servers and ClickHouse Cloud expose; Cloud only accepts TLS, on port 8443.
    """

    model_config = SettingsConfigDict(env_prefix="clickhouse_")

    host: str = InputField(description="Server host name, e.g. abc123.eu-central-1.aws.clickhouse.cloud")
    port: int | None = Field(default=None, description="HTTP port; 8443 with TLS, 8123 without when empty")
    username: str = InputField(default="default", description="ClickHouse user name")
    password: str = SecretField(description="ClickHouse user password")
    secure: bool = Field(default=True, title="TLS", description="Connect over HTTPS; ClickHouse Cloud requires it")

    @cached_property
    def client(self) -> Client:
        """The client every statement goes through, shared by every destination on this connection.

        One client serves concurrent writes: each call is its own HTTP request
        on a shared connection pool. Session ids are turned off because a
        ClickHouse session admits one query at a time, and nothing here relies
        on session state.

        Returns:
            The client, cached per connection instance.
        """
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            secure=self.secure,
            autogenerate_session_id=False,
        )

    @fetch_field_provider
    def databases(self) -> list[dict[str, str]]:
        """List the databases this connection's user can see, system databases excluded.

        Nothing binds it yet: it exists so a destination field can offer the
        databases as a picker instead of free text.

        Returns:
            Database options with ``name``, sorted case-insensitively.
        """
        result = self.client.query(
            "SELECT name FROM system.databases WHERE name NOT IN {system:Array(String)}",
            parameters={"system": list(_SYSTEM_DATABASES)},
        )
        return sorted(({"name": name} for (name,) in result.result_rows), key=lambda option: option["name"].lower())

    def check(self) -> bool:
        """Prove the server answers and the credentials work by running ``SELECT 1``.

        Returns:
            True; a network or authentication failure raises out of the client.
        """
        self.client.command("SELECT 1")
        return True
