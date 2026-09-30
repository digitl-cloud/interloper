"""Tests for ``interloper_agent.history``."""

from __future__ import annotations

from interloper_toolkit import ToolkitContext
from pydantic_ai import Agent, RunContext
from pydantic_ai.capabilities import ProcessHistory
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    SystemPromptPart,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.usage import RequestUsage, RunUsage

from interloper_agent.history import (
    COMPACTION_THRESHOLD,
    ELISION_THRESHOLD,
    RETAINED_TURNS,
    SUMMARY_LEAD,
    elide_tool_returns,
    summarise,
    summary_of,
)

BULK = {"status": "success", "rows": [{"id": i, "error": "x" * 40} for i in range(100)]}
SMALL = {"status": "error", "error": "Requires editor role or higher"}


def turn(prompt: str, content: object, call_id: str, input_tokens: int = 0) -> list[ModelMessage]:
    """One turn: the prompt, a tool call, its return and the answer.

    Returns:
        The four messages; the answer reports ``input_tokens`` as its usage.
    """
    return [
        ModelRequest(parts=[UserPromptPart(prompt)]),
        ModelResponse(parts=[ToolCallPart("list_failures", {}, tool_call_id=call_id)]),
        ModelRequest(parts=[ToolReturnPart("list_failures", content, tool_call_id=call_id)]),
        ModelResponse(parts=[TextPart(f"answer to {prompt}")], usage=RequestUsage(input_tokens=input_tokens)),
    ]


def turns(count: int, input_tokens: int = 0) -> list[ModelMessage]:
    """Consecutive bulky turns, numbered.

    Returns:
        The turns' messages, in order.
    """
    return [message for i in range(count) for message in turn(str(i), BULK, f"c{i}", input_tokens)]


def returns(messages: list[ModelMessage]) -> list[ToolReturnPart]:
    return [p for m in messages if isinstance(m, ModelRequest) for p in m.parts if isinstance(p, ToolReturnPart)]


def prompts(messages: list[ModelMessage]) -> list[str]:
    return [
        str(p.content)
        for m in messages
        if isinstance(m, ModelRequest)
        for p in m.parts
        if isinstance(p, UserPromptPart)
    ]


def summariser(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    """A model that answers a summary request with the transcript it was given.

    Returns:
        The transcript, prefixed, as the summary.
    """
    prompt = messages[-1].parts[-1]
    assert isinstance(prompt, UserPromptPart)
    return ModelResponse(parts=[TextPart(f"SUMMARY OF: {prompt.content}")])


def run_context(ctx: ToolkitContext) -> RunContext[ToolkitContext]:
    return RunContext(deps=ctx, model=FunctionModel(summariser), usage=RunUsage())


class TestElideToolReturns:
    def test_old_bulky_returns_become_a_stub_and_recent_turns_stay_whole(self):
        history = turns(RETAINED_TURNS + 1)

        elided = elide_tool_returns(history)

        old, *recent = returns(elided)
        assert old.content == {
            "status": "elided",
            "message": "The list_failures result was dropped from the history; call the tool again if you need it.",
        }
        assert old.tool_call_id == "c0"
        assert [p.content for p in recent] == [BULK] * RETAINED_TURNS
        assert len(elided) == len(history)

    def test_small_returns_are_kept_however_old(self):
        history = [*turn("first", SMALL, "c1"), *turns(RETAINED_TURNS)]

        assert returns(elide_tool_returns(history))[0].content == SMALL
        assert len(str(SMALL)) < ELISION_THRESHOLD < len(str(BULK))

    def test_a_short_history_is_returned_as_is(self):
        history = turns(RETAINED_TURNS)

        assert elide_tool_returns(history) == history

    def test_the_input_is_not_mutated_and_a_stub_is_never_re_elided(self):
        history = turns(RETAINED_TURNS + 1)

        once = elide_tool_returns(history)
        twice = elide_tool_returns(once)

        assert returns(history)[0].content == BULK
        assert twice == once


class TestSummarise:
    async def test_below_the_threshold_the_history_is_left_alone(self, ctx: ToolkitContext):
        history = turns(RETAINED_TURNS + 2, input_tokens=COMPACTION_THRESHOLD - 1)

        assert await summarise(run_context(ctx), history) == history

    async def test_over_the_threshold_the_older_turns_become_a_summary_ahead_of_the_retained_ones(
        self, ctx: ToolkitContext
    ):
        history = turns(RETAINED_TURNS + 2, input_tokens=COMPACTION_THRESHOLD)

        compacted = await summarise(run_context(ctx), history)

        head, *tail = compacted
        assert isinstance(head, ModelRequest) and isinstance(head.parts[0], SystemPromptPart)
        assert head.parts[0].content.startswith(SUMMARY_LEAD + "SUMMARY OF: User: 0\n")
        assert "list_failures returned" in head.parts[0].content and "Assistant: answer to 1" in head.parts[0].content
        assert "User: 2" not in head.parts[0].content
        assert tail == history[-RETAINED_TURNS * 4 :]
        assert prompts(compacted) == ["2", "3", "4"]

    async def test_a_previous_summary_feeds_the_next_one(self, ctx: ToolkitContext):
        earlier = ModelRequest(parts=[SystemPromptPart(content=SUMMARY_LEAD + "earlier facts")])
        history = [earlier, *turns(RETAINED_TURNS + 1, input_tokens=COMPACTION_THRESHOLD)]

        compacted = await summarise(run_context(ctx), history)

        head = compacted[0]
        assert isinstance(head, ModelRequest) and isinstance(head.parts[0], SystemPromptPart)
        assert "earlier facts" in head.parts[0].content
        summary = summary_of(compacted)
        assert summary is not None and summary.startswith("SUMMARY OF:")

    async def test_a_history_short_enough_to_retain_whole_is_never_summarised(self, ctx: ToolkitContext):
        history = turns(RETAINED_TURNS, input_tokens=COMPACTION_THRESHOLD * 2)

        assert await summarise(run_context(ctx), history) == history

    def test_summary_of_reads_the_carrier_and_nothing_else(self):
        assert summary_of([]) is None
        assert summary_of(turns(1)) is None
        carrier = ModelRequest(parts=[SystemPromptPart(content=SUMMARY_LEAD + "the facts")])
        assert summary_of([carrier, *turns(1)]) == "the facts"


class TestInTheRun:
    def test_the_model_receives_the_processed_history_and_the_run_keeps_it(self, ctx: ToolkitContext):
        seen: list[list[ModelMessage]] = []

        def model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            first = messages[-1].parts[-1]
            if isinstance(first, UserPromptPart) and str(first.content).startswith("User: "):
                return summariser(messages, info)
            seen.append(messages)
            return ModelResponse(parts=[TextPart("ok")])

        agent = Agent(
            FunctionModel(model),
            deps_type=ToolkitContext,
            capabilities=[ProcessHistory(elide_tool_returns), ProcessHistory(summarise)],
        )
        history = turns(RETAINED_TURNS + 1, input_tokens=COMPACTION_THRESHOLD)

        result = agent.run_sync("again", deps=ctx, message_history=history)

        # The new prompt opens a turn, so two old turns fall outside the retained window and are summarised.
        sent = seen[0]
        summary = summary_of(sent)
        assert summary is not None and "User: 1" in summary
        assert prompts(sent) == ["2", "3", "again"]
        assert [p.content["status"] for p in returns(sent) if isinstance(p.content, dict)] == ["success", "success"]
        assert summary_of(result.all_messages()) == summary_of(sent)
