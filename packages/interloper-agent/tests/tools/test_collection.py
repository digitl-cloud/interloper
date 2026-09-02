"""Tests for interloper_agent.tools.collection."""

from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest
from google.adk.tools.tool_context import ToolContext

from interloper_agent import context
from interloper_agent.tools import collection

ORG_ID = uuid4()

CATALOG = {
    "facebook_ads": {
        "kind": "source",
        "assets": [
            {"key": "ads"},
            {"key": "ads_stats", "requires": {"campaigns": "facebook_ads.campaigns"}},
        ],
    },
    "facebook_ads_connection": {"kind": "connection"},
}


class FakeComponentStore:
    """Captures update calls; get returns the store's one row."""

    def __init__(self, store: "FakeStore"):
        """Bind the fake facet to the store holding the row."""
        self._store = store

    def get(self, component_id: Any, *, kind: str | None = None) -> Any:
        return self._store.component

    def update(self, component_id: Any, **kwargs: Any) -> Any:
        self._store.update_kwargs = kwargs
        component = self._store.component
        if kwargs.get("name") is not None:
            component.name = kwargs["name"]
        if kwargs.get("config") is not None:
            component.config = kwargs["config"]
        if kwargs.get("children") is not None:
            component.children = [SimpleNamespace(key=k) for k in kwargs["children"]]
        return component


class FakeStore:
    """Presents the ``components`` facet the collection tools reach for."""

    def __init__(self, component: Any):
        """Bind the fake store to the one component row it serves."""
        self.component = component
        self.update_kwargs: dict[str, Any] | None = None
        self.components = FakeComponentStore(self)


def _component(**overrides: Any) -> Any:
    defaults: dict[str, Any] = {
        "id": uuid4(),
        "org_id": ORG_ID,
        "kind": "source",
        "key": "facebook_ads",
        "name": "FB",
        "config": {"account_id": "1", "dataset": "raw"},
        "children": [SimpleNamespace(key="ads"), SimpleNamespace(key="ads_stats")],
    }
    return SimpleNamespace(**{**defaults, **overrides})


@pytest.fixture
def ctx() -> ToolContext:
    return cast(ToolContext, SimpleNamespace(state={"org_id": str(ORG_ID)}))


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch) -> FakeStore:
    fake = FakeStore(_component())
    monkeypatch.setattr(context, "_store", fake)
    monkeypatch.setattr(context, "_catalog", SimpleNamespace(dump=lambda: CATALOG))
    return fake


def test_update_component_merges_partial_config(store: FakeStore, ctx: ToolContext):
    result = collection.update_component(
        str(store.component.id), config_updates={"dataset": "clean", "account_id": None}, tool_context=ctx
    )
    assert result["status"] == "success"
    assert result["changed_fields"] == ["account_id", "dataset"]
    assert store.update_kwargs is not None
    assert store.update_kwargs["config"] == {"dataset": "clean"}


def test_update_component_renames_without_touching_config(store: FakeStore, ctx: ToolContext):
    result = collection.update_component(str(store.component.id), name="Meta Ads", tool_context=ctx)
    assert result["status"] == "success"
    assert result["component"]["name"] == "Meta Ads"
    assert store.update_kwargs is not None
    assert store.update_kwargs["config"] is None


def test_update_component_replaces_asset_selection(store: FakeStore, ctx: ToolContext):
    result = collection.update_component(str(store.component.id), asset_keys=["ads_stats"], tool_context=ctx)
    assert result["status"] == "success"
    assert store.update_kwargs is not None
    assert store.update_kwargs["children"] == ["ads_stats"]
    assert result["component"]["asset_count"] == 1
    assert result["unresolved_requirements"] == ["ads_stats: campaigns"]


def test_update_component_rejects_unknown_asset_keys(store: FakeStore, ctx: ToolContext):
    result = collection.update_component(str(store.component.id), asset_keys=["nope"], tool_context=ctx)
    assert result["status"] == "error"
    assert result["valid_asset_keys"] == ["ads", "ads_stats"]
    assert store.update_kwargs is None


def test_update_component_rejects_assets_on_non_source(store: FakeStore, ctx: ToolContext):
    store.component = _component(kind="job", key="cron_job", config={"cron": "0 6 * * *"})
    result = collection.update_component(str(store.component.id), asset_keys=["ads"], tool_context=ctx)
    assert result["status"] == "error"
    assert "have no assets" in result["error"]


def test_update_component_refuses_connection_config(store: FakeStore, ctx: ToolContext):
    store.component = _component(kind="connection", key="facebook_ads_connection", config=None)
    result = collection.update_component(
        str(store.component.id), config_updates={"access_token": "x"}, tool_context=ctx
    )
    assert result["status"] == "error"
    assert "credentials" in result["error"]
    assert store.update_kwargs is None


def test_update_component_allows_connection_rename(store: FakeStore, ctx: ToolContext):
    store.component = _component(kind="connection", key="facebook_ads_connection", config=None)
    result = collection.update_component(str(store.component.id), name="Meta main", tool_context=ctx)
    assert result["status"] == "success"
    assert store.update_kwargs is not None
    assert store.update_kwargs["config"] is None


def test_update_component_hides_other_orgs_components(store: FakeStore, ctx: ToolContext):
    store.component = _component(org_id=uuid4())
    result = collection.update_component(str(store.component.id), name="Hijack", tool_context=ctx)
    assert result["status"] == "error"
    assert "not found" in result["error"]
    assert store.update_kwargs is None


def test_update_component_requires_a_change(store: FakeStore, ctx: ToolContext):
    result = collection.update_component(str(store.component.id), tool_context=ctx)
    assert result["status"] == "error"
    assert store.update_kwargs is None
