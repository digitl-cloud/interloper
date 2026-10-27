"""OAuth: the clients that act for a user, and the grants that let them.

interloper is its own authorization server. A client is registered once
(dynamically, or by an operator for a confidential backend); a user's
approval gives it a single-use authorization code, exchanged for an access
token (a ``tokens`` row of kind ``access``) and a refresh token, which
rotates on every use. Every secret is stored as its SHA-256 hash only.

A refresh token belongs to a family: the chain of rotations that started
with one authorization code. The code carries the family id its first
refresh token takes, so replaying a spent code or a rotated refresh token
revokes the whole chain. ``resource`` names the surface the grant is for, an
:class:`~interloper_db.models.tokens.Audience` value, never a URL.
"""

from datetime import datetime
from typing import ClassVar, Literal
from uuid import UUID

from sqlmodel import AutoString, Column, SQLModel
from sqlmodel import Field as SQLField

from interloper_db.models.columns import PortableJSON, TZDateTime, timestamp_column


class OAuthClient(SQLModel, table=True):
    """An application allowed to act for the users who approve it.

    A confidential client authenticates with a secret, kept as its hash; a
    public client (a native or command-line app registered dynamically) has
    none and relies on PKCE alone.
    """

    __tablename__: ClassVar[str] = "oauth_clients"

    client_id: str = SQLField(primary_key=True)
    secret_hash: str | None = None
    name: str
    redirect_uris: list[str] = SQLField(default_factory=list, sa_column=Column(PortableJSON, nullable=False))
    registered_by: Literal["dynamic", "operator"] = SQLField(sa_type=AutoString)
    created_at: datetime | None = timestamp_column()


class AuthorizationCode(SQLModel, table=True):
    """A user's approval of a client, exchanged once for tokens within minutes."""

    __tablename__: ClassVar[str] = "oauth_authorization_codes"

    code_hash: str = SQLField(primary_key=True)
    family_id: UUID
    client_id: str = SQLField(foreign_key="oauth_clients.client_id")
    user_id: UUID = SQLField(foreign_key="profiles.id", index=True)
    redirect_uri: str
    code_challenge: str
    resource: str
    expires_at: datetime = SQLField(sa_column=Column(TZDateTime, nullable=False))
    used_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    created_at: datetime | None = timestamp_column()


class RefreshToken(SQLModel, table=True):
    """A client's long-lived credential for a user, replaced on every use."""

    __tablename__: ClassVar[str] = "oauth_refresh_tokens"

    token_hash: str = SQLField(primary_key=True)
    family_id: UUID = SQLField(index=True)
    client_id: str = SQLField(foreign_key="oauth_clients.client_id")
    user_id: UUID = SQLField(foreign_key="profiles.id", index=True)
    resource: str
    expires_at: datetime = SQLField(sa_column=Column(TZDateTime, nullable=False))
    rotated_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    revoked_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    created_at: datetime | None = timestamp_column()
