"""Agent API: conversations with the assistant, streamed the Vercel AI SDK way.

A conversation is a member's own: its history lives in the store and the
server, not the client, is its source of truth. Each turn is one
:meth:`~pydantic_ai.ui.vercel_ai.VercelAIAdapter.dispatch_request`: the
stored history is passed as ``message_history``, the client's request adds
the new prompt (or the tool approvals and answers the app collected), and the
whole history is saved back when the run completes.

Available when ``interloper-agent`` is installed.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Query, Request, Response
from interloper_agent import TURN_LIMITS, summary_of
from interloper_db import Page, PageQuery
from interloper_db.models import Conversation
from interloper_toolkit import ToolkitContext
from pydantic import BaseModel
from pydantic_ai import DeferredToolResults
from pydantic_ai.agent import AgentRunResult
from pydantic_ai.messages import (
    ModelMessage,
    ModelMessagesTypeAdapter,
    ModelRequest,
    RetryPromptPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.ui import UIEventStream
from pydantic_ai.ui.vercel_ai import VercelAIAdapter, VercelAIEventStream
from pydantic_ai.ui.vercel_ai.request_types import RequestData, UIMessage
from pydantic_ai.ui.vercel_ai.response_types import BaseChunk

from interloper_api.dependencies import AgentDep, CatalogDep, EditorDep, OrgIdDep, StoreDep, ViewerDep

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/agent", tags=["agent"])

SDK_VERSION = 7
"""The AI SDK major version the app speaks; tool approvals need at least 6."""


class LoggedEventStream(VercelAIEventStream[ToolkitContext, Any]):
    """The Vercel event stream, with a run's failure in the server log too.

    The adapter answers a failed run with a 200 and an error chunk, which the
    app renders as a generic message; without this, the cause would exist
    nowhere on the server side.
    """

    async def on_error(self, error: Exception) -> AsyncIterator[BaseChunk]:
        """Log the failure, then encode it for the client as the base class does.

        Args:
            error: The exception that ended the run.

        Yields:
            The protocol's error chunks.
        """
        logger.error("Agent turn failed: %s", error, exc_info=error)
        async for chunk in super().on_error(error):
            yield chunk


@dataclass
class ConversationAdapter(VercelAIAdapter[ToolkitContext, Any]):
    """The Vercel adapter for a stored conversation.

    The app sends its whole message list on every turn, while the stored
    ``history`` already holds all of it but what the turn adds. Taking the
    client's copies too would store every earlier message again, so only the
    additions are kept: the final message's prompt, and the answers to calls
    the history leaves pending. Approvals reach the run as deferred results.
    It also builds :class:`LoggedEventStream`.
    """

    history: Sequence[ModelMessage] = ()

    def sanitize_messages(
        self, messages: Sequence[ModelMessage], *, deferred_tool_results: DeferredToolResults | None = None
    ) -> list[ModelMessage]:
        """Keep what the client's messages add to the stored history, sanitized.

        Args:
            messages: The client's messages, loaded from the request.
            deferred_tool_results: The approvals the request carries, if any.

        Returns:
            One request holding the new answers and prompt, or nothing when the
            client added neither.
        """
        additions = self._additions(messages)
        return super().sanitize_messages(additions, deferred_tool_results=deferred_tool_results)

    def _additions(self, messages: Sequence[ModelMessage]) -> list[ModelMessage]:
        """The client's answers to pending calls, then its new prompt.

        Args:
            messages: The client's messages, loaded from the request.

        Returns:
            One request holding the additions, or an empty list.
        """
        called = {part.tool_call_id for part in self._parts(self.history, ToolCallPart)}
        answered = {part.tool_call_id for part in self._parts(self.history, (ToolReturnPart, RetryPromptPart))}
        answers = [
            part
            for part in self._parts(messages, (ToolReturnPart, RetryPromptPart))
            if part.tool_call_id in called - answered
        ]
        final = messages[-1] if messages else None
        prompts = (
            [part for part in final.parts if isinstance(part, UserPromptPart)]
            if isinstance(final, ModelRequest)
            else []
        )
        parts = [*answers, *prompts]
        return [ModelRequest(parts=parts)] if parts else []

    @staticmethod
    def _parts(messages: Sequence[ModelMessage], part_type: type | tuple[type, ...]) -> list[Any]:
        """The parts of a type, across the messages.

        Args:
            messages: The messages to read.
            part_type: The part class, or classes, to collect.

        Returns:
            The matching parts, in order.
        """
        return [part for message in messages for part in message.parts if isinstance(part, part_type)]

    def build_event_stream(self) -> UIEventStream[RequestData, BaseChunk, ToolkitContext, Any]:
        """Build the event stream that also logs failures.

        Returns:
            The stream transformer for this request.
        """
        return LoggedEventStream(
            self.run_input, accept=self.accept, sdk_version=self.sdk_version, server_message_id=self.server_message_id
        )


class ConversationResponse(BaseModel):
    """A conversation as the list shows it."""

    id: UUID
    title: str | None
    created_at: datetime | None
    updated_at: datetime | None

    @classmethod
    def from_conversation(cls, conversation: Conversation) -> ConversationResponse:
        """Project a conversation row.

        Args:
            conversation: The row.

        Returns:
            The response model.
        """
        return cls(
            id=conversation.id,
            title=conversation.title,
            created_at=conversation.created_at,
            updated_at=conversation.updated_at,
        )


class ConversationDetailResponse(ConversationResponse):
    """A conversation with its history in the AI SDK's ``UIMessage`` shape.

    ``summary`` is what the older turns were compacted into, when they were;
    the messages then start where the summary ends.
    """

    summary: str | None
    messages: list[dict[str, Any]]

    @classmethod
    def from_conversation(cls, conversation: Conversation) -> ConversationDetailResponse:
        """Project a conversation row and render its history for the app.

        Args:
            conversation: The row.

        Returns:
            The response model, its messages as the app's ``useChat`` loads them.
        """
        history = ModelMessagesTypeAdapter.validate_python(conversation.messages)
        dumped = VercelAIAdapter.dump_messages(history, sdk_version=SDK_VERSION)
        messages = _fold_turns([message for message in dumped if message.role != "system"])
        return cls(
            **ConversationResponse.from_conversation(conversation).model_dump(),
            summary=summary_of(history),
            messages=[message.model_dump(by_alias=True, exclude_none=True) for message in messages],
        )


@router.post("/conversations", status_code=201)
def create_conversation(user: EditorDep, org_id: OrgIdDep, store: StoreDep) -> ConversationResponse:
    """Start a conversation in the active organisation.

    Args:
        user: The authenticated user, required to hold at least the ``editor`` role.
        org_id: The active organisation's UUID.
        store: The Store instance.

    Returns:
        The new, empty conversation.
    """
    return ConversationResponse.from_conversation(store.conversations.create(org_id, user.id))


@router.get("/conversations")
def list_conversations(
    user: ViewerDep, org_id: OrgIdDep, store: StoreDep, query: Annotated[PageQuery, Query()]
) -> Page[ConversationResponse]:
    """List the caller's conversations in the active organisation, newest first.

    Args:
        user: The authenticated user, required to hold at least the ``viewer`` role.
        org_id: The active organisation's UUID.
        store: The Store instance.
        query: The window to read.

    Returns:
        The page of conversations.
    """
    return store.conversations.list(org_id, user.id, query).map(ConversationResponse.from_conversation)


@router.get("/conversations/{conversation_id}")
def get_conversation(
    conversation_id: UUID, user: ViewerDep, org_id: OrgIdDep, store: StoreDep
) -> ConversationDetailResponse:
    """Read one of the caller's conversations with its history.

    Args:
        conversation_id: The conversation UUID.
        user: The authenticated user, required to hold at least the ``viewer`` role.
        org_id: The active organisation's UUID.
        store: The Store instance.

    Returns:
        The conversation and its messages, as response models.
    """
    row = store.conversations.get(conversation_id, org_id=org_id, user_id=user.id)
    return ConversationDetailResponse.from_conversation(row)


@router.delete("/conversations/{conversation_id}", status_code=204)
def delete_conversation(conversation_id: UUID, user: EditorDep, org_id: OrgIdDep, store: StoreDep) -> Response:
    """Delete one of the caller's conversations.

    Args:
        conversation_id: The conversation UUID.
        user: The authenticated user, required to hold at least the ``editor`` role.
        org_id: The active organisation's UUID.
        store: The Store instance.

    Returns:
        An empty response.
    """
    row = store.conversations.get(conversation_id, org_id=org_id, user_id=user.id)
    store.conversations.delete(row.id)
    return Response(status_code=204)


@router.post("/conversations/{conversation_id}/chat")
async def chat(
    conversation_id: UUID,
    request: Request,
    user: EditorDep,
    org_id: OrgIdDep,
    store: StoreDep,
    catalog: CatalogDep,
    agent: AgentDep,
) -> Response:
    """Run one turn of a conversation and stream it as the AI SDK expects.

    The request body is the AI SDK's: the client's message list, of which the
    adapter takes the new user message and any approvals or tool answers.
    Everything before comes from the stored history, saved back whole once
    the run completes.

    Args:
        conversation_id: The conversation UUID.
        request: The raw request, handed to the adapter.
        user: The authenticated user, required to hold at least the ``editor`` role.
        org_id: The active organisation's UUID.
        store: The Store instance.
        catalog: The Catalog instance.
        agent: The assistant.

    Returns:
        The turn as a ``text/event-stream`` response.
    """
    conversation = store.conversations.get(conversation_id, org_id=org_id, user_id=user.id)
    context = ToolkitContext(
        store=store,
        catalog=catalog.dump(),
        org_id=org_id,
        role=store.members.role(org_id, user.id) or "viewer",
    )

    def save(result: AgentRunResult[Any]) -> None:
        messages = result.all_messages()
        history = ModelMessagesTypeAdapter.dump_python(messages, mode="json")
        store.conversations.update(conversation.id, messages=history, title=_first_prompt(messages))
        usage = result.usage
        logger.info(
            "Agent turn on conversation %s: %d requests, %d input tokens, %d output tokens, %d messages stored",
            conversation.id,
            usage.requests,
            usage.input_tokens,
            usage.output_tokens,
            len(messages),
        )

    history = ModelMessagesTypeAdapter.validate_python(conversation.messages)
    return await ConversationAdapter.dispatch_request(
        request,
        agent=agent,
        sdk_version=SDK_VERSION,
        deps=context,
        history=history,
        message_history=history,
        conversation_id=str(conversation.id),
        usage_limits=TURN_LIMITS,
        on_complete=save,
    )


def _fold_turns(messages: list[UIMessage]) -> list[UIMessage]:
    """Merge each turn's assistant messages into one, as the stream builds them.

    ``dump_messages`` yields one assistant message per model response, so a
    turn with several tool rounds restores as several messages; the app
    renders each with its own avatar. Live, the SDK accumulates a turn into
    one message, and a reload should read the same.

    Args:
        messages: The history as the adapter dumps it.

    Returns:
        The history with consecutive assistant messages folded into the first.
    """
    folded: list[UIMessage] = []
    for message in messages:
        last = folded[-1] if folded else None
        if last is not None and last.role == "assistant" and message.role == "assistant":
            last.parts.extend(message.parts)
        else:
            folded.append(message.model_copy(update={"parts": list(message.parts)}))
    return folded


def _first_prompt(messages: list[ModelMessage]) -> str | None:
    """The first thing the user said, which titles the conversation.

    Args:
        messages: The conversation's history.

    Returns:
        The first user prompt's text, or ``None`` when there is none yet.
    """
    for message in messages:
        if isinstance(message, ModelRequest):
            for part in message.parts:
                if isinstance(part, UserPromptPart) and isinstance(part.content, str):
                    return part.content
    return None
