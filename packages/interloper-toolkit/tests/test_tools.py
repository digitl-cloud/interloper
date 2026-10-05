"""Tests for ``interloper_toolkit.tools``."""

from __future__ import annotations

import inspect
import typing
from typing import Any

from interloper_db import RunStatus

from interloper_toolkit import ToolkitContext, analytics, catalog, collection, jobs, lineage, scheduling, sources
from interloper_toolkit.models import ToolError
from interloper_toolkit.tools import TOOLS, Effect, Tool

MODULE_HELPERS = {"categorise", "normalized_asset_keys", "source_relations", "unresolved_requirements"}


class TestTable:
    def test_every_public_toolkit_function_is_listed_once(self):
        names = [tool.name for tool in TOOLS]

        assert len(names) == len(set(names))
        for module in (analytics, catalog, collection, jobs, lineage, scheduling, sources):
            public = {
                name
                for name, member in vars(module).items()
                if callable(member) and not name.startswith("_") and member.__module__ == module.__name__
            }
            missing = public - set(names) - MODULE_HELPERS
            assert missing == set(), f"{module.__name__}: {missing}"

    def test_creates_and_cancels_need_approval(self):
        approved = {tool.name for tool in TOOLS if tool.needs_approval}

        assert approved == {
            "create_connections",
            "create_source",
            "create_sources",
            "create_job",
            "cancel_run",
            "cancel_backfill",
        }

    def test_the_credential_taking_tool_carries_secrets(self):
        assert {tool.name for tool in TOOLS if tool.carries_secrets} == {"create_connections"}

    def test_reads_are_the_tools_without_a_role_gate(self):
        reads = {tool.name for tool in TOOLS if tool.effect in (Effect.READ, Effect.READ_PROVIDER)}
        ungated = {tool.name for tool in TOOLS if not hasattr(tool.fn, "__wrapped__")}

        assert reads == ungated


class TestBind:
    def test_keeps_the_name_docstring_and_parameters_minus_the_context(self):
        leading = inspect.Parameter("run", inspect.Parameter.POSITIONAL_ONLY, annotation=int)

        bound = Tool(scheduling.list_recent_runs, Effect.READ).bind(lambda run: run, leading)

        assert bound.__name__ == "list_recent_runs"
        assert bound.__doc__ == scheduling.list_recent_runs.__doc__
        parameters = inspect.signature(bound).parameters
        assert list(parameters) == ["run", "component_id", "status", "limit", "offset"]
        assert parameters["run"].annotation is int
        assert parameters["limit"].default == 20
        assert parameters["status"].annotation == (RunStatus | None)
        assert (
            inspect.signature(bound).return_annotation == typing.get_type_hints(scheduling.list_recent_runs)["return"]
        )

    def test_derives_the_context_from_the_leading_arguments(self, ctx: ToolkitContext):
        run: Any = type("Run", (), {"deps": ctx})()
        leading = inspect.Parameter("run", inspect.Parameter.POSITIONAL_ONLY)

        result = Tool(scheduling.list_jobs, Effect.READ).bind(lambda run: run.deps, leading)(run)

        assert result.status == "success"
        assert result.total == 0

    def test_takes_the_context_from_a_plain_callable_without_leading_parameters(self, ctx: ToolkitContext):
        bound = Tool(scheduling.list_jobs, Effect.READ).bind(lambda: ctx)

        assert list(inspect.signature(bound).parameters) == ["limit", "offset"]
        assert bound(limit=5).status == "success"

    async def test_binds_async_tools_too(self, ctx: ToolkitContext):
        bound = Tool(collection.check_connection, Effect.READ_PROVIDER).bind(lambda: ctx)

        assert inspect.iscoroutinefunction(bound)
        assert isinstance(await bound("not-a-uuid"), ToolError)
