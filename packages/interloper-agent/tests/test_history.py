"""Tests for ``interloper_agent.history``."""

from __future__ import annotations

from interloper_toolkit import ToolkitContext
from pydantic_ai import Agent
from pydantic_ai.capabilities import ProcessHistory
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel

from interloper_agent.history import ELISION_THRESHOLD, RETAINED_TURNS, elide_tool_returns

BULK = {"status": "success", "rows": [{"id": i, "error": "x" * 40} for i in range(100)]}
SMALL = {"status": "error", "error": "Requires editor role or higher"}


def turn(prompt: str, content: object, call_id: str) -> list[ModelMessage]:
    """One turn: the prompt, a tool call, its return and the answer.

    Returns:
        The four messages.
    """
    return [
        ModelRequest(parts=[UserPromptPart(prompt)]),
        ModelResponse(parts=[ToolCallPart("list_failures", {}, tool_call_id=call_id)]),
        ModelRequest(parts=[ToolReturnPart("list_failures", content, tool_call_id=call_id)]),
        ModelResponse(parts=[TextPart("3 runs failed")]),
    ]


def turns(count: int) -> list[ModelMessage]:
    """Consecutive bulky turns, numbered.

    Returns:
        The turns' messages, in order.
    """
    return [message for i in range(count) for message in turn(str(i), BULK, f"c{i}")]


def returns(messages: list[ModelMessage]) -> list[ToolReturnPart]:
    return [p for m in messages if isinstance(m, ModelRequest) for p in m.parts if isinstance(p, ToolReturnPart)]


class TestElideToolReturns:
    def test_old_bulky_returns_become_a_stub_and_recent_turns_stay_whole(self):
        history = [*turn("first", BULK, "c1"), *turn("second", BULK, "c2"), *turn("third", BULK, "c3")]
        history += turn("fourth", BULK, "c4")

        elided = elide_tool_returns(history)

        old, *recent = returns(elided)
        assert old.content == "(list_failures result elided from the history; call the tool again if you need it)"
        assert old.tool_call_id == "c1"
        assert [p.content for p in recent] == [BULK] * RETAINED_TURNS
        assert len(elided) == len(history)

    def test_small_returns_are_kept_however_old(self):
        history = [*turn("first", SMALL, "c1"), *turn("second", BULK, "c2")] + turn("third", BULK, "c3")
        history += turn("fourth", BULK, "c4")

        assert returns(elide_tool_returns(history))[0].content == SMALL
        assert len(str(SMALL)) < ELISION_THRESHOLD < len(str(BULK))

    def test_a_short_history_is_returned_as_is(self):
        history = [*turn("first", BULK, "c1"), *turn("second", BULK, "c2")]

        assert elide_tool_returns(history) == history

    def test_the_input_is_not_mutated_and_a_stub_is_never_re_elided(self):
        history = turns(RETAINED_TURNS + 1)

        once = elide_tool_returns(history)
        twice = elide_tool_returns(once)

        assert returns(history)[0].content == BULK
        assert twice == once


class TestInTheRun:
    def test_the_model_receives_the_elided_history_and_the_run_keeps_it(self, ctx: ToolkitContext):
        seen: list[list[ModelMessage]] = []

        def answer(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            seen.append(messages)
            return ModelResponse(parts=[TextPart("ok")])

        agent = Agent(
            FunctionModel(answer), deps_type=ToolkitContext, capabilities=[ProcessHistory(elide_tool_returns)]
        )
        history = turns(RETAINED_TURNS + 1)

        result = agent.run_sync("again", deps=ctx, message_history=history)

        # The new prompt opens a turn of its own, so two old turns fall outside the retained window.
        assert [isinstance(p.content, str) for p in returns(seen[0])] == [True, True, False, False]
        assert [isinstance(p.content, str) for p in returns(result.all_messages())] == [True, True, False, False]
