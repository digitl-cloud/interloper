"""Tests for interloper_agent.tools.collection: the wrappers pass through to the toolkit."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest
from google.adk.tools.tool_context import ToolContext
from interloper_toolkit import collection as toolkit_collection
from interloper_toolkit import sources as toolkit_sources
from interloper_toolkit.models import ToolError

from interloper_agent import context
from interloper_agent.tools import collection

ORG_ID = uuid4()


@pytest.fixture
def ctx(monkeypatch: pytest.MonkeyPatch) -> ToolContext:
    monkeypatch.setattr(context, "_store", SimpleNamespace())
    monkeypatch.setattr(context, "_catalog", SimpleNamespace(dump=dict))
    return cast(ToolContext, SimpleNamespace(state={"org_id": str(ORG_ID), "role": "editor"}))


def test_the_wrappers_pass_the_context_and_arguments_through(monkeypatch: pytest.MonkeyPatch, ctx: ToolContext):
    seen: dict[str, Any] = {}

    def fake(tk_ctx: Any, *args: Any, **kwargs: Any) -> ToolError:
        seen.update(org_id=tk_ctx.org_id, role=tk_ctx.role, args=args)
        return ToolError(error="seen")

    monkeypatch.setattr(toolkit_collection, "update_component", fake)

    result = collection.update_component("cid", name="New", tool_context=ctx)

    assert result == {"status": "error", "error": "seen", "valid_values": None, "category": None}
    assert seen == {"org_id": ORG_ID, "role": "editor", "args": ("cid", "New", None, None)}


async def test_async_wrappers_await_the_toolkit(monkeypatch: pytest.MonkeyPatch, ctx: ToolContext):
    async def fake(tk_ctx: Any, *args: Any) -> ToolError:
        return ToolError(error=f"resolved {args}")

    monkeypatch.setattr(toolkit_sources, "resolve_source_field_options", fake)

    result = await collection.resolve_source_field_options("shop_source", "cid", tool_context=ctx)

    assert result["error"] == "resolved ('shop_source', 'cid', None)"


def test_the_wrappers_adopt_the_toolkit_docstrings():
    assert collection.create_source.__doc__ == toolkit_sources.create_source.__doc__
    assert collection.check_connection.__doc__ == toolkit_collection.check_connection.__doc__
