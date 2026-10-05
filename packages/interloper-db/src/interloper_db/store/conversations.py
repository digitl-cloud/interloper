"""Agent conversations: create, read back, update after a turn, delete.

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
from interloper_db.session import commit, save, session_scope
from interloper_db.store.page import Page, PageQuery

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
            save(session, row)
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

    def list(self, org_id: UUID, user_id: UUID, query: PageQuery) -> Page[Conversation]:
        """A member's conversations in an organisation, most recently updated first.

        Args:
            org_id: Organisation UUID.
            user_id: Profile UUID.
            query: The window to read.

        Returns:
            The page of conversations.
        """
        statement = (
            select(Conversation)
            .where(Conversation.org_id == org_id, Conversation.user_id == user_id)
            .order_by(col(Conversation.updated_at).desc(), col(Conversation.created_at).desc(), col(Conversation.id))
        )
        with session_scope(self._engine) as session:
            return Page.read(session, statement, query)

    def update(
        self, conversation_id: UUID, *, messages: Sequence[Any] | None = None, title: str | None = None
    ) -> Conversation:
        """Replace a conversation's history after a turn, and title it once.

        Args:
            conversation_id: The conversation UUID.
            messages: The whole history as the agent framework serialises it;
                ``None`` leaves the history alone.
            title: A title to set when the conversation has none yet, clipped
                to ``TITLE_LENGTH``; ``None`` leaves the title alone.

        Returns:
            The updated row.
        """
        with session_scope(self._engine) as session:
            row = self._lock(conversation_id)
            if messages is not None:
                row.messages = [*messages]
            if title and not row.title:
                row.title = title[:TITLE_LENGTH]
            save(session, row)
            return row

    def delete(self, conversation_id: UUID) -> None:
        """Delete a conversation.

        Args:
            conversation_id: The conversation UUID.
        """
        with session_scope(self._engine) as session:
            session.delete(self._lock(conversation_id))
            commit(session)

    def _lock(self, conversation_id: UUID) -> Conversation:
        """Load a conversation for a write, holding its row for the rest of the transaction.

        Args:
            conversation_id: The conversation UUID.

        Returns:
            The row.

        Raises:
            NotFoundError: If the conversation is not found.
        """
        with session_scope(self._engine) as session:
            row = session.get(Conversation, conversation_id, with_for_update=True)
            if row is None:
                raise NotFoundError(f"Conversation {conversation_id} not found")
            return row
