"""DuckDB connection resource: a local database file or a MotherDuck database."""

from __future__ import annotations

from functools import cached_property

import duckdb
from interloper.connection import Connection, connection
from interloper.resource.fields import InputField, SecretField, fetch_field_provider
from pydantic_settings import SettingsConfigDict

_SYSTEM_SCHEMAS = ("information_schema", "pg_catalog")


@connection(
    key="duckdb_connection",
    name="DuckDB",
    icon="icon:duckdb",
    tags=["Database"],
)
class DuckDBConnection(Connection):
    """Connection resource opening a DuckDB database.

    ``database`` is a path to a ``.duckdb`` file, or ``md:<database>`` for a
    MotherDuck database, which authenticates with ``motherduck_token``.
    """

    model_config = SettingsConfigDict(env_prefix="duckdb_")

    database: str = InputField(
        label="Database",
        description="Path to a .duckdb file, or md:<database> for MotherDuck",
        info=(
            "A local file is created on first use and admits one writing process at a time. "
            "A MotherDuck database (md:my_db) needs the token below."
        ),
    )
    motherduck_token: str | None = SecretField(
        default=None,
        label="MotherDuck token",
        description="Service token; only for md: databases",
    )

    @cached_property
    def client(self) -> duckdb.DuckDBPyConnection:
        """The DuckDB connection every operation derives a cursor from.

        One connection is opened per instance and never used directly: each
        operation takes its own ``client.cursor()``, a duplicate connection to
        the same database, so threads writing concurrently never share one
        DuckDB connection object (which is not thread-safe).

        Returns:
            The connection, cached per connection instance.
        """
        config = {"motherduck_token": self.motherduck_token} if self.motherduck_token else {}
        return duckdb.connect(self.database, config=config)

    @fetch_field_provider
    def schemas(self) -> list[dict[str, str]]:
        """List the schemas of the database, system schemas excluded.

        Nothing binds it yet: it exists so a future destination field can
        offer the schemas as a picker instead of free text.

        Returns:
            Schema options with ``name``, sorted case-insensitively.
        """
        cursor = self.client.cursor()
        try:
            rows = cursor.execute(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE catalog_name = current_database() AND schema_name NOT IN (?, ?)",
                list(_SYSTEM_SCHEMAS),
            ).fetchall()
        finally:
            cursor.close()
        return sorted(({"name": name} for (name,) in rows), key=lambda s: s["name"].lower())

    def check(self) -> bool:
        """Prove the database opens and answers a query.

        Returns:
            True; a database that cannot be opened or queried raises instead.
        """
        cursor = self.client.cursor()
        try:
            cursor.execute("SELECT 1").fetchall()
        finally:
            cursor.close()
        return True
