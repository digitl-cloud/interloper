"""People, the organisations they belong to, and how they sign in."""

from datetime import datetime
from enum import Enum
from typing import ClassVar, Optional
from uuid import UUID

from interloper.errors import ConfigError
from sqlmodel import Column, Relationship, SQLModel, text
from sqlmodel import Field as SQLField

from interloper_db.models.columns import TZDateTime, timestamp_column


class Role(str, Enum):
    """A member's role in an organisation, from least to most privileged.

    Each role grants everything the roles before it grant, so a gate names the
    lowest role it admits rather than a set.
    """

    VIEWER = "viewer"
    EDITOR = "editor"
    ADMIN = "admin"

    @classmethod
    def parse(cls, value: str) -> "Role":
        """The role a caller-supplied name denotes.

        Args:
            value: The role name.

        Returns:
            The role.

        Raises:
            ConfigError: If the name is not a role.
        """
        try:
            return cls(value)
        except ValueError:
            known = ", ".join(role.value for role in cls)
            raise ConfigError(f"Unknown role {value!r}; expected one of {known}") from None

    @classmethod
    def at_least(cls, role: str | None, minimum: str) -> bool:
        """Whether a held role admits what *minimum* gates.

        Args:
            role: The role held, as stored on a membership; ``None`` (no
                membership) or a name outside the vocabulary admits nothing.
            minimum: The lowest role the gate admits.

        Returns:
            True when *role* ranks at or above *minimum*.
        """
        try:
            held = cls(role)
        except ValueError:
            return False
        ranks: list[Role] = list(cls)
        return ranks.index(held) >= ranks.index(cls.parse(minimum))


class UserOrganisation(SQLModel, table=True):
    """Junction: user membership in an organisation with a role.

    ``profile`` reads the member's profile, so a membership listing names its
    members without a second lookup. Declared ahead of :class:`Profile`,
    whose organisations are reached through this table.
    """

    __tablename__: ClassVar[str] = "user_organisations"

    user_id: UUID = SQLField(foreign_key="profiles.id", primary_key=True)
    organisation_id: UUID = SQLField(foreign_key="organisations.id", primary_key=True)
    role: str = "viewer"
    created_at: datetime | None = timestamp_column()

    profile: Optional["Profile"] = Relationship(sa_relationship_kwargs={"viewonly": True})


class Profile(SQLModel, table=True):
    """An authenticated user profile (Google OAuth).

    ``organisations`` reads the organisations the profile is a member of.
    """

    __tablename__: ClassVar[str] = "profiles"

    id: UUID = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    email: str
    name: str | None = None
    google_id: str = SQLField(index=True, unique=True)
    avatar_url: str | None = None
    timezone: str | None = None
    is_super_admin: bool = SQLField(default=False, sa_column_kwargs={"server_default": text("false")})
    last_organisation_id: UUID | None = SQLField(default=None, foreign_key="organisations.id")
    created_at: datetime | None = timestamp_column()

    organisations: list["Organisation"] = Relationship(
        link_model=UserOrganisation, sa_relationship_kwargs={"viewonly": True}
    )


class Organisation(SQLModel, table=True):
    """A tenant organisation.

    Deletion is soft (``deleted_at``): the row survives so retained
    execution history and the usage ledger stay attributable for billing,
    while the org's sensitive payload (components, tokens, memberships)
    is purged.
    """

    __tablename__: ClassVar[str] = "organisations"

    id: UUID = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    name: str
    deleted_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    created_at: datetime | None = timestamp_column()


class Invitation(SQLModel, table=True):
    """A pending invitation for a user to join an organisation."""

    __tablename__: ClassVar[str] = "invitations"

    id: UUID = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    organisation_id: UUID = SQLField(foreign_key="organisations.id", index=True)
    email: str
    role: str = "viewer"
    token: str = SQLField(index=True, unique=True)
    invited_by: UUID = SQLField(foreign_key="profiles.id")
    created_at: datetime | None = timestamp_column()
    expires_at: datetime = SQLField(sa_column=Column(TZDateTime))


class AuthSession(SQLModel, table=True):
    """A cookie-based user login session with optional org context.

    Named ``AuthSession`` (table stays ``sessions``) so it can never be
    confused with the SQLAlchemy ``Session`` used throughout the store.
    """

    __tablename__: ClassVar[str] = "sessions"

    id: UUID = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    user_id: UUID = SQLField(foreign_key="profiles.id", index=True)
    organisation_id: UUID | None = SQLField(default=None, foreign_key="organisations.id")
    token_hash: str = SQLField(index=True, unique=True)
    expires_at: datetime = SQLField(sa_column=Column(TZDateTime))
    created_at: datetime | None = timestamp_column()
