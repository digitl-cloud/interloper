"""SQL connection resource holding a SQLAlchemy database URL."""

from __future__ import annotations

from functools import cached_property

import sqlalchemy
from interloper.connection import Connection, connection
from interloper.resource.fields import SecretField, fetch_field_provider
from pydantic_settings import SettingsConfigDict
from sqlalchemy.engine import Engine


@connection(
    key="sql_connection",
    name="SQL database",
    icon="icon:sql",
    tags=["Database"],
    maturity="alpha",
)
class SQLConnection(Connection):
    """Connection resource holding the URL of a SQL database.

    Any SQLAlchemy dialect whose driver is installed works; PostgreSQL through
    psycopg ships with the package.
    """

    model_config = SettingsConfigDict(env_prefix="sql_")

    url: str = SecretField(
        label="Database URL",
        description="SQLAlchemy URL, e.g. postgresql+psycopg://user:password@host:5432/db",
        info=(
            "The URL carries the password, so it is stored as a secret. PostgreSQL works out of the box "
            "(postgresql+psycopg://); other databases need their SQLAlchemy driver installed, e.g. "
            "mysql+pymysql:// or mssql+pyodbc://."
        ),
    )

    @cached_property
    def engine(self) -> Engine:
        """The engine every statement goes through.

        ``pool_pre_ping`` tests a pooled connection before handing it out, so
        a connection the server dropped between runs is replaced instead of
        failing the next write.

        Returns:
            The engine, cached per connection instance.
        """
        return sqlalchemy.create_engine(self.url, pool_pre_ping=True)

    @fetch_field_provider
    def schemas(self) -> list[dict[str, str]]:
        """List the schemas of the database.

        Not wired to a field yet: the destination's ``default_dataset`` is
        optional, and a ``FetchField`` is a required pick. It is kept for a
        future schema picker.

        Returns:
            Schema options with ``name``, sorted case-insensitively.
        """
        names = sqlalchemy.inspect(self.engine).get_schema_names()
        return sorted(({"name": name} for name in names), key=lambda s: s["name"].lower())

    def check(self) -> bool:
        """Prove the URL works by running ``SELECT 1``.

        Returns:
            True; an unreachable database or a rejected login raises instead.
        """
        with self.engine.connect() as conn:
            conn.execute(sqlalchemy.text("SELECT 1"))
        return True
