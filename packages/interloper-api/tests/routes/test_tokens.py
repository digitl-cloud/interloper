"""Tests for ``interloper_api.routes.tokens`` — PAT mint/list/revoke.

The store is faked in memory; the properties under test are the API
contract: a token is its user's, with no organisation; the raw token appears
exactly once (creation), listings never carry secret material, and only the
owner revokes, anyone else getting a 404 that doesn't leak token existence.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper_db import NotFoundError, Page, PageQuery

from interloper_api.app import install_error_handlers
from interloper_api.dependencies import get_current_user, get_store
from interloper_api.routes import tokens as tokens_module

USER_ID = uuid4()


def _row(*, user_id: UUID = USER_ID, name: str = "t") -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        user_id=user_id,
        name=name,
        token_prefix="ilp_12345678",
        token_hash="deadbeef" * 8,
        created_at=datetime.now(timezone.utc),
        expires_at=None,
        last_used_at=None,
        revoked_at=None,
    )


class FakeStore:
    """In-memory stand-in for the store facets the token routes could reach for.

    Every caller administers every organisation, so a route that let an
    organisation admin act on someone else's token would show it.
    """

    def __init__(self) -> None:
        self.rows: dict[UUID, SimpleNamespace] = {}
        self.listed: list[tuple[UUID, PageQuery]] = []
        self.members = SimpleNamespace(role=lambda org_id, user_id: "admin")
        self.tokens = SimpleNamespace(
            create_personal=self._create_token,
            list=self._list_tokens,
            get=self._get_token,
            revoke=self._revoke_token,
        )

    def _create_token(self, user_id: UUID, name: str, expires_at=None):
        row = _row(user_id=user_id, name=name)
        row.expires_at = expires_at
        self.rows[row.id] = row
        return row, "ilp_raw-secret-token"

    def _list_tokens(self, user_id: UUID, query: PageQuery) -> Page:
        self.listed.append((user_id, query))
        return Page.window([r for r in self.rows.values() if r.user_id == user_id], query)

    def _get_token(self, token_id: UUID):
        if token_id not in self.rows:
            raise NotFoundError(f"Token {token_id} not found")
        return self.rows[token_id]

    def _revoke_token(self, token_id: UUID):
        row = self.rows[token_id]
        row.revoked_at = datetime.now(timezone.utc)
        return row


def _client(store: FakeStore, user_id: UUID = USER_ID) -> TestClient:
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(tokens_module.router)
    user = SimpleNamespace(id=user_id, is_super_admin=False)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_current_user] = lambda: user
    return TestClient(app)


class TestCreate:
    def test_create_returns_raw_token_once(self) -> None:
        store = FakeStore()
        resp = _client(store).post("/tokens", json={"name": "laptop"})

        assert resp.status_code == 201
        body = resp.json()
        assert body["token"] == "ilp_raw-secret-token"
        assert body["name"] == "laptop"
        assert body["expires_at"] is not None  # default 90 days

    def test_create_token_needs_no_organisation(self) -> None:
        store = FakeStore()

        resp = _client(store).post("/tokens", json={"name": "laptop"})

        assert resp.status_code == 201
        assert "organisation_id" not in resp.json()
        (row,) = store.rows.values()
        assert row.user_id == USER_ID

    def test_create_without_expiry(self) -> None:
        store = FakeStore()
        resp = _client(store).post("/tokens", json={"name": "forever", "expires_in_days": None})

        assert resp.status_code == 201
        assert resp.json()["expires_at"] is None


class TestList:
    def test_list_never_contains_secret_material(self) -> None:
        store = FakeStore()
        client = _client(store)
        client.post("/tokens", json={"name": "laptop"})

        resp = client.get("/tokens")

        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 1
        (item,) = body["items"]
        assert "token" not in item
        assert "token_hash" not in item
        assert item["token_prefix"] == "ilp_12345678"

    def test_list_returns_only_own_tokens(self) -> None:
        store = FakeStore()
        mine, theirs = _row(name="mine"), _row(user_id=uuid4(), name="theirs")
        store.rows = {mine.id: mine, theirs.id: theirs}

        resp = _client(store).get("/tokens")

        ((user_id, _),) = store.listed
        assert user_id == USER_ID
        assert [item["name"] for item in resp.json()["items"]] == ["mine"]

    def test_the_page_window_is_forwarded(self) -> None:
        store = FakeStore()

        resp = _client(store).get("/tokens", params={"limit": 10, "offset": 20})

        assert resp.status_code == 200
        ((_, query),) = store.listed
        assert (query.limit, query.offset) == (10, 20)

    def test_a_page_larger_than_the_cap_is_refused(self) -> None:
        store = FakeStore()

        resp = _client(store).get("/tokens", params={"limit": 501})

        assert resp.status_code == 422
        assert store.listed == []


class TestRevoke:
    def test_owner_can_revoke_own_token(self) -> None:
        store = FakeStore()
        row = _row()
        store.rows[row.id] = row

        resp = _client(store).delete(f"/tokens/{row.id}")

        assert resp.status_code == 204
        assert resp.content == b""
        assert row.revoked_at is not None

    def test_revoke_other_users_token_404s(self) -> None:
        store = FakeStore()
        stranger_id = uuid4()
        row = _row()
        store.rows[row.id] = row

        denied = _client(store, user_id=stranger_id).delete(f"/tokens/{row.id}")
        missing = _client(store, user_id=stranger_id).delete(f"/tokens/{uuid4()}")

        assert denied.status_code == 404
        assert missing.status_code == 404
        assert row.revoked_at is None
