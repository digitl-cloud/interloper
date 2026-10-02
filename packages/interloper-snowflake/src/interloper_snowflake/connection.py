"""Snowflake connection resource holding user and password credentials."""

from __future__ import annotations

from functools import cached_property
from typing import Any

import snowflake.connector
from interloper.connection import Connection, connection
from interloper.resource.fields import InputField, SecretField, fetch_field_provider
from pydantic_settings import SettingsConfigDict
from snowflake.connector import SnowflakeConnection as Session


@connection(
    key="snowflake_connection",
    name="Snowflake",
    icon="icon:snowflake",
    tags=["Cloud"],
    maturity="alpha",
)
class SnowflakeConnection(Connection):
    """Connection resource holding Snowflake credentials.

    The connection holds the credentials; each destination bound to it opens
    its own session on its own database and warehouse.
    """

    model_config = SettingsConfigDict(env_prefix="snowflake_")

    account: str = InputField(label="Account identifier", description="e.g. xy12345.eu-central-1 or myorg-myaccount")
    user: str = InputField(description="Snowflake user name")
    password: str = SecretField(description="Snowflake user password")
    role: str | None = InputField(default=None, description="Role to assume; the user's default role when empty")

    def connect(self, **session: Any) -> Session:
        """Open a new Snowflake session with this connection's credentials.

        Autocommit stays on so a lone statement commits by itself; a caller
        that needs atomicity opens an explicit ``BEGIN``.

        Args:
            **session: Session settings passed to the connector, such as
                ``warehouse`` and ``database``.

        Returns:
            The new connector session.
        """
        return snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            role=self.role,
            autocommit=True,
            **session,
        )

    @cached_property
    def client(self) -> Session:
        """The session the connection's own check and pickers run on.

        A destination opens its own session through :meth:`connect` instead,
        so its warehouse and transactions never touch a session another
        component shares.

        Returns:
            The connector session, cached per connection instance.
        """
        return self.connect()

    def _names(self, sql: str) -> list[dict[str, str]]:
        """Run a ``SHOW`` statement and return its ``name`` column as options.

        ``SHOW`` output carries many columns whose order is not part of its
        contract, so the column is found by name through the cursor's
        description.

        Args:
            sql: The ``SHOW`` statement to run.

        Returns:
            Options with ``name``, sorted case-insensitively.
        """
        cursor = self.client.cursor()
        try:
            cursor.execute(sql)
            rows: list[Any] = cursor.fetchall()
            index = [column[0] for column in cursor.description].index("name")
        finally:
            cursor.close()
        return sorted(({"name": row[index]} for row in rows), key=lambda option: option["name"].lower())

    @fetch_field_provider
    def databases(self) -> list[dict[str, str]]:
        """List the databases this connection's role can see.

        Backs the destination's ``database`` ``FetchField``.

        Returns:
            Database options with ``name``.
        """
        return self._names("SHOW DATABASES")

    @fetch_field_provider
    def warehouses(self) -> list[dict[str, str]]:
        """List the virtual warehouses this connection's role can see.

        Backs the destination's ``warehouse`` ``FetchField``.

        Returns:
            Warehouse options with ``name``.
        """
        return self._names("SHOW WAREHOUSES")

    def check(self) -> bool:
        """Prove the credentials work by opening a session and running ``SELECT 1``.

        Returns:
            True; a login or network failure raises out of the connector.
        """
        cursor = self.client.cursor()
        try:
            cursor.execute("SELECT 1")
            cursor.fetchall()
        finally:
            cursor.close()
        return True
