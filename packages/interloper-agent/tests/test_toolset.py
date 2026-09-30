"""Tests for ``interloper_agent.toolset``."""

from __future__ import annotations

import inspect
from typing import Any

from interloper_db.store import Store
from interloper_toolkit import ToolkitContext, collection, scheduling, sources
from interloper_toolkit.models import ToolError
from pydantic_ai import Agent, DeferredToolRequests, RunContext
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel

from interloper_agent.toolset import TOOLS, bind, toolset


class TestBind:
    def test_keeps_the_name_docstring_and_parameters_minus_the_context(self):
        tool = bind(scheduling.list_recent_runs)

        assert tool.__name__ == "list_recent_runs"
        assert tool.__doc__ == scheduling.list_recent_runs.__doc__
        parameters = inspect.signature(tool).parameters
        assert list(parameters) == ["run_context", "component_id", "status", "limit", "offset"]
        assert parameters["run_context"].annotation == RunContext[ToolkitContext]
        assert parameters["limit"].default == 20
        assert parameters["status"].annotation == (str | None)

    def test_forwards_the_deps_as_the_toolkit_context(self, ctx: ToolkitContext):
        run_context: Any = type("Ctx", (), {"deps": ctx})()

        result = bind(scheduling.list_jobs)(run_context)

        assert result.status == "success"
        assert result.total == 0

    async def test_binds_async_tools_too(self, ctx: ToolkitContext):
        run_context: Any = type("Ctx", (), {"deps": ctx})()

        result = await bind(collection.check_connection)(run_context, "not-a-uuid")

        assert isinstance(result, ToolError)


class TestRegistry:
    def test_every_toolkit_tool_is_registered_once(self):
        names = [fn.__name__ for fn, _ in TOOLS]

        assert len(names) == len(set(names))
        for module in (collection, scheduling, sources):
            public = {
                name
                for name, member in vars(module).items()
                if callable(member) and not name.startswith("_") and member.__module__ == module.__name__
            }
            helpers = {"categorise", "normalized_asset_keys", "source_relations", "unresolved_requirements"}
            missing = public - set(names) - helpers - {"request_connection_setup"}
            assert missing == set(), f"{module.__name__}: {missing}"

    def test_only_creates_and_cancels_require_approval(self):
        approved = {fn.__name__ for fn, requires_approval in TOOLS if requires_approval}

        assert approved == {"create_connections", "create_source", "create_sources", "create_job", "cancel_backfill"}

    def test_the_toolset_exposes_every_tool_with_its_schema(self):
        tools = toolset().tools

        assert set(tools) == {fn.__name__ for fn, _ in TOOLS}
        schema = tools["list_recent_runs"].function_schema.json_schema
        assert set(schema["properties"]) == {"component_id", "status", "limit", "offset"}
        assert "Filter by job UUID" in schema["properties"]["component_id"]["description"]


class TestApproval:
    def test_a_create_stops_the_run_until_approved(self, ctx: ToolkitContext, store: Store):
        def call_create_job(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            if len(messages) == 1:
                return ModelResponse(
                    parts=[ToolCallPart("create_job", {"name": "Daily", "cron": "0 6 * * *", "target_source_ids": []})]
                )
            return ModelResponse(parts=[TextPart("done")])

        agent = Agent[ToolkitContext, str | DeferredToolRequests](
            FunctionModel(call_create_job),
            deps_type=ToolkitContext,
            output_type=[str, DeferredToolRequests],
            toolsets=[toolset()],
        )

        result = agent.run_sync("schedule it", deps=ctx)

        assert isinstance(result.output, DeferredToolRequests)
        assert [call.tool_name for call in result.output.approvals] == ["create_job"]
        assert store.components.count(ctx.org_id, kinds=["job"]) == 0
