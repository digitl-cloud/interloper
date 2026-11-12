"""The database package's settings sections: the database, the encryption key and the quota defaults."""

from __future__ import annotations

from typing import ClassVar
from urllib.parse import quote

from interloper.settings import Settings
from pydantic import Field


class PostgresSettings(Settings):
    """PostgreSQL connection settings."""

    public: ClassVar[frozenset[str]] = frozenset({"host", "port", "database", "statement_timeout"})

    host: str = Field(default="localhost", description="Database host.")
    port: int = Field(default=5432, description="Database port.")
    user: str = Field(default="", description="Database user.")
    password: str = Field(default="", description="Database password.")
    database: str = Field(default="interloper", description="Database name.")
    statement_timeout: float | None = Field(
        default=None,
        gt=0,
        description="Seconds after which the server cancels a statement of the process engine, sent as the libpq "
        "`statement_timeout` option; unset leaves the server's own setting untouched.",
    )

    @property
    def dsn(self) -> str:
        """The PostgreSQL connection string, user and password percent-encoded.

        Returns:
            ``postgresql://user:password@host:port/database``.
        """
        credentials = f"{quote(self.user, safe='')}:{quote(self.password, safe='')}"
        return f"postgresql://{credentials}@{self.host}:{self.port}/{self.database}"

    @property
    def location(self) -> str:
        """The database this connects to, safe to print: no credentials.

        Returns:
            ``host:port/database``.
        """
        return f"{self.host}:{self.port}/{self.database}"


class EncryptionSettings(Settings):
    """The key the platform encrypts and signs with.

    Encryption is the default, so a key is required to persist resources:
    with none set, writes fail closed rather than storing plaintext. Only
    the processes holding the store read it (the API, the scheduler, the MCP
    server, ``interloper db``); a run's process never does, since what it
    loads arrives decrypted and its token is opaque to it.
    """

    key: str = Field(
        default="",
        description=(
            "Fernet key encrypting stored resource payloads and run tokens, and signing the Google login's state; "
            "required by the platform to persist resources, to dispatch runs and for Google login."
        ),
    )


class QuotaSettings(Settings):
    """Default per-organisation quota limits; unset means unlimited.

    Per-organisation overrides live in the ``quotas`` table and win over
    these defaults.
    """

    public: ClassVar[frozenset[str]] = frozenset(
        {"max_sources", "max_assets_per_source", "max_successful_runs_per_month", "max_backfill_partitions"}
    )

    max_sources: int | None = Field(default=None, description="Sources an organisation may hold.")
    max_assets_per_source: int | None = Field(default=None, description="Assets a source may enable.")
    max_successful_runs_per_month: int | None = Field(
        default=None, description="Successful runs an organisation may complete per UTC month."
    )
    max_backfill_partitions: int | None = Field(default=None, description="Partitions one backfill may span.")
