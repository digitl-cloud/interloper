"""Tests for interloper_agent.tools.scheduling: the wrappers pass through to the toolkit."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest
from google.adk.tools.tool_context import ToolContext
from interloper_toolkit import scheduling as toolkit_scheduling
from interloper_toolkit.models import ToolError

from interloper_agent import context
from interloper_agent.tools import scheduling

ORG_ID = uuid4()


@pytest.fixture
def ctx(monkeypatch: pytest.MonkeyPatch) -> ToolContext:
    monkeypatch.setattr(context, "_store", SimpleNamespace())
    monkeypatch.setattr(context, "_catalog", SimpleNamespace(dump=dict))
    return cast(ToolContext, SimpleNamespace(state={"org_id": str(ORG_ID), "role": "editor"}))


@pytest.mark.parametrize(
    ("name", "call", "expected_args"),
    [
        ("toggle_job", lambda ctx: scheduling.toggle_job("jid", False, tool_context=ctx), ("jid", False)),
        ("toggle_asset", lambda ctx: scheduling.toggle_asset("aid", True, tool_context=ctx), ("aid", True)),
        (
            "trigger_run",
            lambda ctx: scheduling.trigger_run("jid", "2026-07-01", tool_context=ctx),
            ("jid", "2026-07-01"),
        ),
        ("retry_run", lambda ctx: scheduling.retry_run("rid", "failed", tool_context=ctx), ("rid", "failed")),
        ("cancel_backfill", lambda ctx: scheduling.cancel_backfill("bid", tool_context=ctx), ("bid",)),
        (
            "trigger_backfill",
            lambda ctx: scheduling.trigger_backfill("jid", "2026-07-01", "2026-07-02", tool_context=ctx),
            ("jid", "2026-07-01", "2026-07-02", 1, False),
        ),
    ],
)
def test_the_write_wrappers_pass_the_context_and_arguments_through(
    monkeypatch: pytest.MonkeyPatch, ctx: ToolContext, name: str, call: Any, expected_args: tuple[Any, ...]
):
    seen: dict[str, Any] = {}

    def fake(tk_ctx: Any, *args: Any) -> ToolError:
        seen.update(org_id=tk_ctx.org_id, role=tk_ctx.role, args=args)
        return ToolError(error="seen")

    monkeypatch.setattr(toolkit_scheduling, name, fake)

    result = call(ctx)

    assert result["error"] == "seen"
    assert seen == {"org_id": ORG_ID, "role": "editor", "args": expected_args}


def test_a_session_without_a_role_fails_closed(monkeypatch: pytest.MonkeyPatch, ctx: ToolContext):
    roles: list[str] = []
    monkeypatch.setattr(
        toolkit_scheduling, "toggle_job", lambda tk_ctx, *a: roles.append(tk_ctx.role) or ToolError(error="x")
    )

    scheduling.toggle_job("jid", False, tool_context=cast(ToolContext, SimpleNamespace(state={"org_id": str(ORG_ID)})))

    assert roles == ["viewer"]
