"""Tests for ``interloper_api.routes.tokens`` — PAT mint/list/revoke.

The store is faked in memory; the properties under test are the API
contract: the raw token appears exactly once (creation), listings never
carry secret material, and revocation authorizes owner-or-org-admin with a
404 that doesn't leak token existence.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from interloper_api.dependencies import get_current_user, get_org_id, get_store, require_viewer
from interloper_api.routes import tokens as tokens_module

USER_ID = uuid4()
ORG_ID = uuid4()


def _row(*, user_id: UUID = USER_ID, org_id: UUID = ORG_ID, name: str = "t") -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        user_id=user_id,
        organisation_id=org_id,
        name=name,
        token_prefix="ilp_12345678",
        token_hash="deadbeef" * 8,
        created_at=datetime.now(timezone.utc),
        expires_at=None,
        last_used_at=None,
        revoked_at=None,
    )


class FakeStore:
    """In-memory stand-in implementing only what the token routes call."""

    def __init__(self) -> None:
        self.rows: dict[UUID, SimpleNamespace] = {}
        self.roles: dict[tuple[UUID, UUID], str] = {}

    def create_token(self, user_id: UUID, org_id: UUID, name: str, expires_at=None):
        row = _row(user_id=user_id, org_id=org_id, name=name)
        row.expires_at = expires_at
        self.rows[row.id] = row
        return row, "ilp_raw-secret-token"

    def list_tokens(self, user_id: UUID, org_id: UUID | None = None):
        return [
            r
            for r in self.rows.values()
            if r.user_id == user_id and (org_id is None or r.organisation_id == org_id)
        ]

    def get_token(self, token_id: UUID):
        return self.rows.get(token_id)

    def revoke_token(self, token_id: UUID):
        row = self.rows[token_id]
        row.revoked_at = datetime.now(timezone.utc)
        return row

    def get_user_role(self, user_id: UUID, org_id: UUID):
        return self.roles.get((user_id, org_id))


def _client(store: FakeStore, user_id: UUID = USER_ID) -> TestClient:
    app = FastAPI()
    app.include_router(tokens_module.router)
    user = SimpleNamespace(id=user_id, is_super_admin=False)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_current_user] = lambda: user
    app.dependency_overrides[require_viewer] = lambda: user
    app.dependency_overrides[get_org_id] = lambda: ORG_ID
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
        (item,) = resp.json()
        assert "token" not in item
        assert "token_hash" not in item
        assert item["token_prefix"] == "ilp_12345678"


class TestRevoke:
    def test_owner_can_revoke_own_token(self) -> None:
        store = FakeStore()
        row = _row()
        store.rows[row.id] = row

        resp = _client(store).delete(f"/tokens/{row.id}")

        assert resp.status_code == 200
        assert row.revoked_at is not None

    def test_org_admin_can_revoke_others_token(self) -> None:
        store = FakeStore()
        admin_id = uuid4()
        store.roles[(admin_id, ORG_ID)] = "admin"
        row = _row()  # owned by USER_ID
        store.rows[row.id] = row

        resp = _client(store, user_id=admin_id).delete(f"/tokens/{row.id}")

        assert resp.status_code == 200
        assert row.revoked_at is not None

    def test_non_owner_non_admin_gets_same_404_as_missing(self) -> None:
        store = FakeStore()
        stranger_id = uuid4()
        store.roles[(stranger_id, ORG_ID)] = "editor"
        row = _row()
        store.rows[row.id] = row

        denied = _client(store, user_id=stranger_id).delete(f"/tokens/{row.id}")
        missing = _client(store, user_id=stranger_id).delete(f"/tokens/{uuid4()}")

        assert denied.status_code == 404
        assert missing.status_code == 404
        assert row.revoked_at is None
