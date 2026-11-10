"""Agent conversations: a user's chat with the assistant, as message history."""

from datetime import datetime
from typing import Any, ClassVar
from uuid import UUID

from sqlalchemy import Index
from sqlmodel import Column, SQLModel, text
from sqlmodel import Field as SQLField

from interloper_db.models.columns import PortableJSON, timestamp_column


class Conversation(SQLModel, table=True):
    """One conversation with the agent, owned by a member of an organisation.

    ``messages`` is the agent framework's own serialised message history
    (pydantic-ai's ``ModelMessage`` list), replaced whole after every turn:
    the run already holds the complete list, so there is nothing to reconcile.
    Tool arguments and results ride inside it, which is safe because no tool
    accepts a credential on the conversational path.

    A conversation an OAuth client started through an access token records
    that client in ``client_id``; it belongs to the client's use of the
    user's access, so the app's own list leaves it out. A conversation the
    app or a personal token started has no client.
    """

    __tablename__: ClassVar[str] = "conversations"
    __table_args__: ClassVar[tuple[Any, ...]] = (
        Index("ix_conversations_owner_updated", "org_id", "user_id", "updated_at"),
        Index("ix_conversations_client_updated", "client_id", "updated_at"),
    )

    id: UUID = SQLField(
        default=None,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    org_id: UUID = SQLField(foreign_key="organisations.id")
    user_id: UUID = SQLField(foreign_key="profiles.id")
    client_id: str | None = SQLField(default=None, foreign_key="oauth_clients.client_id")
    title: str | None = None
    messages: list[Any] = SQLField(default_factory=list, sa_column=Column(PortableJSON, nullable=False))
    created_at: datetime | None = timestamp_column()
    updated_at: datetime | None = timestamp_column(onupdate=text("CURRENT_TIMESTAMP"))
