"""Tests for ``interloper_agent.toolset``."""

from __future__ import annotations

from interloper_db.store import ComponentQuery, Store
from interloper_toolkit import TOOLS as TOOLKIT_TOOLS
from interloper_toolkit import ToolkitContext
from pydantic_ai import Agent, DeferredToolRequests, RunContext
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from interloper_agent.toolset import TOOLS, toolset


def agent_calling(tool_name: str, args: dict) -> Agent[ToolkitContext, str | DeferredToolRequests]:
    """An agent whose model calls one tool, then answers.

    Returns:
        The agent, over the real toolset.
    """

    def call(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        if len(messages) == 1:
            return ModelResponse(parts=[ToolCallPart(tool_name, args)])
        return ModelResponse(parts=[TextPart("done")])

    return Agent[ToolkitContext, str | DeferredToolRequests](
        FunctionModel(call), deps_type=ToolkitContext, output_type=[str, DeferredToolRequests], toolsets=[toolset()]
    )


class TestRegistry:
    def test_the_agent_registers_the_toolkit_table_plus_the_selection_card(self):
        assert TOOLS[: len(TOOLKIT_TOOLS)] == TOOLKIT_TOOLS
        assert [tool.name for tool in TOOLS[len(TOOLKIT_TOOLS) :]] == ["request_user_selection"]

    async def test_the_toolset_exposes_every_tool_with_its_schema(self, ctx: ToolkitContext):
        run_context = RunContext(deps=ctx, model=TestModel(), usage=RunUsage())

        tools = await toolset().get_tools(run_context)

        assert set(tools) == {tool.name for tool in TOOLS}
        schema = tools["list_recent_runs"].tool_def.parameters_json_schema
        assert set(schema["properties"]) == {"component_id", "status", "limit", "offset"}
        assert "Filter by job UUID" in schema["properties"]["component_id"]["description"]


class TestApproval:
    def test_a_create_stops_the_run_until_approved(self, ctx: ToolkitContext, store: Store):
        agent = agent_calling("create_job", {"name": "Daily", "cron": "0 6 * * *", "target_source_ids": []})

        result = agent.run_sync("schedule it", deps=ctx)

        assert isinstance(result.output, DeferredToolRequests)
        assert [call.tool_name for call in result.output.approvals] == ["create_job"]
        assert store.components.list(ctx.org_id, ComponentQuery(kind=["job"])).total == 0


class TestDeferral:
    def test_a_selection_stops_the_run_for_the_app_to_answer(self, ctx: ToolkitContext):
        agent = agent_calling("request_user_selection", {"prompt": "Which?", "options": [{"label": "A", "value": "1"}]})

        result = agent.run_sync("pick", deps=ctx)

        assert isinstance(result.output, DeferredToolRequests)
        assert [call.tool_name for call in result.output.calls] == ["request_user_selection"]

    def test_a_connection_setup_stops_the_run_only_when_the_form_is_presented(self, ctx: ToolkitContext, store: Store):
        agent = agent_calling("request_connection_setup", {"connection_key": "demo_connection"})

        fresh = agent.run_sync("connect", deps=ctx)
        store.components.create(ctx.org_id, kind="connection", key="demo_connection", config={})
        existing = agent.run_sync("connect", deps=ctx)

        assert isinstance(fresh.output, DeferredToolRequests)
        assert [call.tool_name for call in fresh.output.calls] == ["request_connection_setup"]
        assert existing.output == "done"
