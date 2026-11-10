"""Tokens: the bearer secrets a user authenticates with, one row per secret."""

from datetime import datetime
from enum import Enum
from typing import Any, ClassVar
from uuid import UUID

from sqlmodel import AutoString, Column, SQLModel, text
from sqlmodel import Field as SQLField

from interloper_db.models.columns import TZDateTime, timestamp_column


class TokenKind(str, Enum):
    """What issued a token, which decides the surfaces that accept it.

    The column stores the value, so a row read back carries the plain string,
    which compares equal to its member.
    """

    SESSION = "session"
    PERSONAL = "personal"
    ACCESS = "access"


class Audience(str, Enum):
    """A surface a token is presented to.

    ``api`` and ``mcp`` double as the resource an access token is issued for:
    an access token reaches only the surface its ``resource`` names.
    """

    APP = "app"
    API = "api"
    MCP = "mcp"

    @property
    def kinds(self) -> tuple[TokenKind, ...]:
        """The token kinds this surface accepts.

        Returns:
            ``session`` for the app's cookie; ``personal`` and ``access`` for
            the API and MCP bearers.
        """
        if self is Audience.APP:
            return (TokenKind.SESSION,)
        return (TokenKind.PERSONAL, TokenKind.ACCESS)


class Token(SQLModel, table=True):
    """A bearer secret standing for its user, in every organisation they belong to.

    Only the SHA-256 hash persists; the raw secret is shown once at creation.
    A token carries no organisation and no role: the caller names the
    organisation and the role is read live from the membership, so a
    membership change or removal applies to every token at once.
    Migration 015 renamed ``personal_access_tokens`` to this table, keeping
    the hashes, which is why ``create_all`` skips it (see the
    ``migration_owned`` marker).
    """

    __tablename__: ClassVar[str] = "tokens"
    __table_args__: ClassVar[dict[str, Any]] = {"info": {"migration_owned": True}}

    id: UUID = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    user_id: UUID = SQLField(foreign_key="profiles.id", index=True)
    kind: TokenKind = SQLField(
        default=TokenKind.PERSONAL, sa_type=AutoString, sa_column_kwargs={"server_default": TokenKind.PERSONAL.value}
    )
    client_id: str | None = None
    name: str | None = None
    resource: str | None = None
    token_prefix: str
    token_hash: str = SQLField(index=True, unique=True)
    expires_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    last_used_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    revoked_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    created_at: datetime | None = timestamp_column()
