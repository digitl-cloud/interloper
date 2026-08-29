"""Personal access tokens, the non-interactive counterpart to a session."""

from datetime import datetime
from typing import ClassVar
from uuid import UUID

from sqlmodel import Column, SQLModel, text
from sqlmodel import Field as SQLField

from interloper_db.models.columns import TZDateTime, timestamp_column


class PersonalAccessToken(SQLModel, table=True):
    """A long-lived API token for programmatic access (MCP, CLI), org-scoped.

    Only the SHA-256 hash persists; the raw token is shown once at creation.
    The role is deliberately not snapshotted: it is resolved live from
    ``UserOrganisation`` at each use, so membership changes and offboarding
    invalidate tokens immediately.
    """

    __tablename__: ClassVar[str] = "personal_access_tokens"

    id: UUID = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    user_id: UUID = SQLField(foreign_key="profiles.id", index=True)
    organisation_id: UUID = SQLField(foreign_key="organisations.id", index=True)
    name: str
    token_prefix: str
    token_hash: str = SQLField(index=True, unique=True)
    expires_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    last_used_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    revoked_at: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
    created_at: datetime | None = timestamp_column()
