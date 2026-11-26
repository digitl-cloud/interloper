"""Tests for ``interloper_api.routes.relations``: the organisation-wide relation listing."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper_db import Organisation, Page, Profile, RelationQuery, Token

from interloper_api.app import ORG_PREFIX, install_error_handlers
from interloper_api.dependencies import CallerCredential, OrgScope, get_org_scope, get_store
from interloper_api.routes import relations as relations_module

_ORG_ID = uuid4()
_BASE_URL = f"http://testserver/organisations/{_ORG_ID}"


def _viewer_scope(org_id: UUID) -> OrgScope:
    """Stand in for ``get_org_scope``: the caller is a viewer of the path's organisation.

    Args:
        org_id: The organisation the path names.

    Returns:
        The scope.
    """
    caller = CallerCredential(user=cast(Profile, SimpleNamespace(id=uuid4())), token=cast(Token, SimpleNamespace()))
    return OrgScope(credential=caller, organisation=cast(Organisation, SimpleNamespace(id=org_id)), role="viewer")


def _relation(relation: str, from_kind: str, to_kind: str) -> SimpleNamespace:
    return SimpleNamespace(from_id=uuid4(), relation=relation, to_id=uuid4(), from_kind=from_kind, to_kind=to_kind)


class RelationStore:
    """Fake store exposing ``relations.list``, filtering the way the store does."""

    def __init__(self) -> None:
        """Set up the rows and the recorder."""
        self.rows: list[Any] = []
        self.listed: list[tuple[UUID, RelationQuery]] = []
        self.relations = SimpleNamespace(list=self._list)

    def _list(self, org_id: UUID, query: RelationQuery) -> Page[Any]:
        self.listed.append((org_id, query))
        rows = [
            row
            for row in self.rows
            if (query.relation is None or row.relation == query.relation)
            and (query.from_kind is None or row.from_kind == query.from_kind)
            and (query.to_kind is None or row.to_kind == query.to_kind)
        ]
        return Page.window(rows, query)


@pytest.fixture
def store() -> RelationStore:
    """A fresh fake store.

    Returns:
        The fake store.
    """
    return RelationStore()


@pytest.fixture
def client(store: RelationStore) -> TestClient:
    """Mount the relations router with the viewer gate satisfied.

    Args:
        store: The fake store the route reads.

    Returns:
        A client for the probe app.
    """
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(relations_module.org_router, prefix=ORG_PREFIX)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_org_scope] = _viewer_scope
    return TestClient(app, base_url=_BASE_URL)


class TestListRelations:
    """``GET /relations``, optionally narrowed by relation and kinds."""

    def test_lists_every_relation(self, client: TestClient, store: RelationStore) -> None:
        relation = _relation("connection", "source", "connection")
        store.rows = [relation]

        response = client.get("/relations")

        assert response.status_code == 200
        assert response.json() == {
            "items": [
                {
                    "from_id": str(relation.from_id),
                    "relation": "connection",
                    "to_id": str(relation.to_id),
                    "from_kind": "source",
                    "to_kind": "connection",
                }
            ],
            "total": 1,
        }
        ((org_id, query),) = store.listed
        assert org_id == _ORG_ID
        assert query == RelationQuery()

    def test_the_relation_filter_narrows_the_result(self, client: TestClient, store: RelationStore) -> None:
        store.rows = [
            _relation("connection", "source", "connection"),
            _relation("destinations", "source", "destination"),
        ]

        response = client.get("/relations?relation=destinations")

        assert [row["relation"] for row in response.json()["items"]] == ["destinations"]
        ((_, query),) = store.listed
        assert query.relation == "destinations"

    def test_the_kind_filters_narrow_the_result(self, client: TestClient, store: RelationStore) -> None:
        store.rows = [
            _relation("upstreams", "asset", "asset"),
            _relation("connection", "source", "connection"),
        ]

        response = client.get("/relations", params={"from_kind": "asset", "to_kind": "asset"})

        rows = response.json()["items"]
        assert all(row["to_kind"] == "asset" for row in rows)
        assert [row["relation"] for row in rows] == ["upstreams"]
        ((_, query),) = store.listed
        assert (query.from_kind, query.to_kind) == ("asset", "asset")

    def test_the_page_window_is_forwarded(self, client: TestClient, store: RelationStore) -> None:
        store.rows = [_relation(f"r{i}", "asset", "asset") for i in range(3)]

        body = client.get("/relations", params={"limit": 1, "offset": 2}).json()

        ((_, query),) = store.listed
        assert (query.limit, query.offset) == (1, 2)
        assert [row["relation"] for row in body["items"]] == ["r2"]
        assert body["total"] == 3

    def test_no_limit_reads_the_whole_set_in_one_page(self, client: TestClient, store: RelationStore) -> None:
        store.rows = [_relation(f"r{i}", "asset", "asset") for i in range(3)]

        body = client.get("/relations").json()

        ((_, query),) = store.listed
        assert query.limit is None
        assert len(body["items"]) == body["total"] == 3

    def test_a_page_larger_than_the_cap_is_a_422(self, client: TestClient, store: RelationStore) -> None:
        assert client.get("/relations", params={"limit": 501}).status_code == 422
        assert store.listed == []

    def test_the_listing_requires_a_viewer(self, store: RelationStore) -> None:
        app = FastAPI()
        install_error_handlers(app)
        app.include_router(relations_module.org_router, prefix=ORG_PREFIX)
        app.dependency_overrides[get_store] = lambda: store

        assert TestClient(app, base_url=_BASE_URL).get("/relations").status_code == 401
