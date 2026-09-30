"""Agent conversations: create, read back, save a turn, delete.

A conversation belongs to the member who started it, in the organisation it
was started in; every read takes both, and a row that matches neither reads
as missing, the store's idiom for keeping ids from acting as oracles.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from interloper.errors import NotFoundError
from sqlalchemy import Engine
from sqlmodel import col, select

from interloper_db.models import Conversation
from interloper_db.session import commit, session_scope

TITLE_LENGTH = 80


class ConversationStore:
    """Store methods for agent conversations."""

    def __init__(self, engine: Engine) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
        """
        self._engine = engine

    def create(self, org_id: UUID, user_id: UUID) -> Conversation:
        """Start an empty conversation.

        Args:
            org_id: Organisation the conversation belongs to.
            user_id: Profile UUID of the member starting it.

        Returns:
            The created row.
        """
        with session_scope(self._engine) as session:
            row = Conversation(org_id=org_id, user_id=user_id)
            session.add(row)
            commit(session)
            session.refresh(row)
            return row

    def get(self, conversation_id: UUID, *, org_id: UUID, user_id: UUID) -> Conversation:
        """Load one of a member's conversations.

        Args:
            conversation_id: The conversation UUID.
            org_id: Organisation the conversation must belong to.
            user_id: Member the conversation must belong to.

        Returns:
            The row.

        Raises:
            NotFoundError: If no conversation with this id belongs to that
                member in that organisation.
        """
        with session_scope(self._engine) as session:
            row = session.get(Conversation, conversation_id)
            if row is None or row.org_id != org_id or row.user_id != user_id:
                raise NotFoundError(f"Conversation {conversation_id} not found")
            return row

    def list_all(self, org_id: UUID, user_id: UUID) -> list[Conversation]:
        """A member's conversations in an organisation, most recently updated first.

        Args:
            org_id: Organisation UUID.
            user_id: Profile UUID.

        Returns:
            The rows.
        """
        with session_scope(self._engine) as session:
            statement = (
                select(Conversation)
                .where(Conversation.org_id == org_id, Conversation.user_id == user_id)
                .order_by(col(Conversation.updated_at).desc(), col(Conversation.created_at).desc())
            )
            return list(session.exec(statement).all())

    def save(self, conversation_id: UUID, messages: Sequence[Any], *, title: str | None = None) -> Conversation:
        """Replace a conversation's history after a turn.

        Args:
            conversation_id: The conversation UUID.
            messages: The whole history as the agent framework serialises it.
            title: A title to set when the conversation has none yet, clipped
                to ``TITLE_LENGTH``; ``None`` leaves the title alone.

        Returns:
            The updated row.

        Raises:
            NotFoundError: If the conversation is not found.
        """
        with session_scope(self._engine) as session:
            row = session.get(Conversation, conversation_id)
            if row is None:
                raise NotFoundError(f"Conversation {conversation_id} not found")
            row.messages = list(messages)
            if title and not row.title:
                row.title = title[:TITLE_LENGTH]
            session.add(row)
            commit(session)
            session.refresh(row)
            return row

    def delete(self, conversation_id: UUID) -> None:
        """Delete a conversation.

        Args:
            conversation_id: The conversation UUID.

        Raises:
            NotFoundError: If the conversation is not found.
        """
        with session_scope(self._engine) as session:
            row = session.get(Conversation, conversation_id)
            if row is None:
                raise NotFoundError(f"Conversation {conversation_id} not found")
            session.delete(row)
            commit(session)
