"""Tests for ``interloper_toolkit.authz``."""

from __future__ import annotations

import dataclasses

import pytest

from interloper_toolkit import ToolkitContext
from interloper_toolkit.authz import denied, requires_role
from interloper_toolkit.models import ToolError


@requires_role("editor")
def write(ctx: ToolkitContext, value: int) -> dict[str, int]:
    """Write something.

    Returns:
        The value written.
    """
    return {"value": value}


@requires_role("editor")
async def write_async(ctx: ToolkitContext, value: int) -> dict[str, int]:
    return {"value": value}


class TestRequiresRole:
    @pytest.mark.parametrize("role", ["editor", "admin"])
    def test_a_sufficient_role_runs_the_body(self, ctx: ToolkitContext, role: str):
        assert write(dataclasses.replace(ctx, role=role), 1) == {"value": 1}

    @pytest.mark.parametrize("role", ["viewer", "owner", ""])
    def test_a_lower_or_unknown_role_is_refused_before_the_body(self, ctx: ToolkitContext, role: str):
        result = write(dataclasses.replace(ctx, role=role), 1)

        assert isinstance(result, ToolError)
        assert result.error == "Requires editor role or higher"

    def test_the_default_context_role_fails_closed(self, ctx: ToolkitContext):
        bare = ToolkitContext(store=ctx.store, catalog=ctx.catalog, org_id=ctx.org_id)

        assert bare.role == "viewer"
        assert isinstance(write(bare, 1), ToolError)

    async def test_async_tools_are_gated_the_same_way(self, ctx: ToolkitContext):
        assert isinstance(await write_async(dataclasses.replace(ctx, role="viewer"), 1), ToolError)
        assert await write_async(ctx, 1) == {"value": 1}

    def test_the_wrapper_keeps_the_docstring_the_surfaces_adopt(self):
        assert write.__name__ == "write"
        assert write.__doc__ is not None
        assert write.__doc__.startswith("Write something.")

    def test_an_unknown_minimum_is_a_programming_error(self):
        with pytest.raises(ValueError):
            requires_role("owner")

    def test_denied_ranks_viewer_editor_admin(self):
        assert denied("admin", "editor") is None
        assert denied("viewer", "viewer") is None
        assert denied("editor", "admin") is not None
