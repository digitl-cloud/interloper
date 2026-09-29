"""Tests for interloper_agent.tools.scheduling."""

from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest
from google.adk.tools.tool_context import ToolContext

from interloper_agent import context
from interloper_agent.tools import scheduling

ORG_ID = uuid4()


class FakeStore:
    """Serves one component row and records every write the tools attempt."""

    def __init__(self, component: Any):
        """Bind the fake store to the one component row it serves."""
        self.component = component
        self.writes: list[str] = []
        self.components = SimpleNamespace(get=self._get, update=self._record("update"))
        self.runs = SimpleNamespace(create=self._record("create"), create_backfill=self._record("create_backfill"))

    def _get(self, component_id: Any, *, kind: str | None = None) -> Any:
        return self.component

    def _record(self, name: str) -> Any:
        def write(*args: Any, **kwargs: Any) -> Any:
            self.writes.append(name)
            return self.component

        return write


def _component(org_id: Any) -> Any:
    return SimpleNamespace(id=uuid4(), org_id=org_id, kind="job", key="daily", name="Daily", config={})


@pytest.fixture
def ctx() -> ToolContext:
    return cast(ToolContext, SimpleNamespace(state={"org_id": str(ORG_ID)}))


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch) -> FakeStore:
    fake = FakeStore(_component(ORG_ID))
    monkeypatch.setattr(context, "_store", fake)
    monkeypatch.setattr(context, "_catalog", SimpleNamespace(dump=dict))
    return fake


WRITES = [
    lambda cid, ctx: scheduling.toggle_job(cid, False, tool_context=ctx),
    lambda cid, ctx: scheduling.toggle_asset(cid, False, tool_context=ctx),
    lambda cid, ctx: scheduling.trigger_run(cid, tool_context=ctx),
    lambda cid, ctx: scheduling.trigger_backfill(cid, "2026-07-01", "2026-07-02", tool_context=ctx),
]


@pytest.mark.parametrize("write", WRITES)
def test_writes_to_own_components_go_through(store: FakeStore, ctx: ToolContext, write: Any):
    result = write(str(store.component.id), ctx)

    assert result["status"] == "success"
    assert len(store.writes) == 1


@pytest.mark.parametrize("write", WRITES)
def test_writes_to_another_orgs_component_are_refused(store: FakeStore, ctx: ToolContext, write: Any):
    store.component = _component(uuid4())

    result = write(str(store.component.id), ctx)

    assert result["status"] == "error"
    assert "not found" in result["error"]
    assert store.writes == []
