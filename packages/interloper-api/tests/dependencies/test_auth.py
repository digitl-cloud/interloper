"""Tests for ``interloper_api.dependencies.auth``.

The dependencies are exercised through a router so the ``Cookie`` default
and FastAPI's dependency chaining are the real ones, not hand-passed
arguments.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import APIRouter, Depends, FastAPI
from fastapi.testclient import TestClient

from interloper_api.dependencies import (
    get_current_org,
    get_current_user,
    get_org_id,
    get_session_context,
    get_store,
)

_ORG_ID = uuid4()
_USER_ID = uuid4()


class FakeStore:
    """Stand-in exposing only the ``auth`` and ``organisations`` facets these read."""

    def __init__(self, *, session: tuple[Any, Any] | None = None, org: Any = None) -> None:
        """Set up the fake.

        Args:
            session: What ``auth.resolve_session`` returns; ``None`` means the
                token is unknown or expired.
            org: What ``organisations.get`` returns.
        """
        self.resolved_tokens: list[str] = []
        self.requested_orgs: list[UUID] = []
        self._session = session
        self._org = org
        self.auth = SimpleNamespace(resolve_session=self._resolve_session)
        self.organisations = SimpleNamespace(get=self._get_org)

    def _resolve_session(self, token: str) -> tuple[Any, Any] | None:
        self.resolved_tokens.append(token)
        return self._session

    def _get_org(self, org_id: UUID) -> Any:
        self.requested_orgs.append(org_id)
        return self._org


def _profile() -> SimpleNamespace:
    return SimpleNamespace(id=_USER_ID, email="user@example.com", is_super_admin=False)


def _session_row(organisation_id: UUID | None = _ORG_ID) -> SimpleNamespace:
    return SimpleNamespace(id=uuid4(), organisation_id=organisation_id)


def _client(store: FakeStore) -> TestClient:
    """Mount every auth dependency behind a probe route.

    Args:
        store: The fake store the dependencies resolve against.

    Returns:
        A client for the probe app.
    """
    router = APIRouter()

    @router.get("/user")
    def read_user(user: Any = Depends(get_current_user)) -> dict[str, str]:
        return {"email": user.email}

    @router.get("/session")
    def read_session(context: Any = Depends(get_session_context)) -> dict[str, str]:
        profile, session_row = context
        return {"email": profile.email, "session": str(session_row.id)}

    @router.get("/org")
    def read_org(org: Any = Depends(get_current_org)) -> dict[str, str]:
        return {"name": org.name}

    @router.get("/org-id")
    def read_org_id(org_id: UUID = Depends(get_org_id)) -> dict[str, str]:
        return {"org_id": str(org_id)}

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_store] = lambda: store
    return TestClient(app)


class TestGetCurrentUser:
    """Resolving the caller from the session cookie."""

    def test_a_valid_session_yields_the_profile(self) -> None:
        store = FakeStore(session=(_profile(), _session_row()))
        client = _client(store)
        client.cookies.set("session_token", "tok")

        response = client.get("/user")

        assert response.status_code == 200
        assert response.json() == {"email": "user@example.com"}
        assert store.resolved_tokens == ["tok"]

    def test_no_cookie_is_a_401(self) -> None:
        response = _client(FakeStore()).get("/user")

        assert response.status_code == 401
        assert response.json()["detail"] == "Not authenticated"

    def test_an_unresolvable_token_is_a_401(self) -> None:
        client = _client(FakeStore(session=None))
        client.cookies.set("session_token", "stale")

        response = client.get("/user")

        assert response.status_code == 401
        assert response.json()["detail"] == "Invalid or expired session"


class TestGetSessionContext:
    """The routes that need the session row itself, not just the profile."""

    def test_returns_both_halves(self) -> None:
        session_row = _session_row()
        client = _client(FakeStore(session=(_profile(), session_row)))
        client.cookies.set("session_token", "tok")

        response = client.get("/session")

        assert response.json() == {"email": "user@example.com", "session": str(session_row.id)}

    def test_no_cookie_is_a_401(self) -> None:
        assert _client(FakeStore()).get("/session").status_code == 401

    def test_an_unresolvable_token_is_a_401(self) -> None:
        client = _client(FakeStore(session=None))
        client.cookies.set("session_token", "stale")

        assert client.get("/session").status_code == 401


class TestGetCurrentOrg:
    """The active organisation comes off the session row, not the request."""

    def test_returns_the_sessions_organisation(self) -> None:
        store = FakeStore(
            session=(_profile(), _session_row()),
            org=SimpleNamespace(id=_ORG_ID, name="Dev Org"),
        )
        client = _client(store)
        client.cookies.set("session_token", "tok")

        response = client.get("/org")

        assert response.json() == {"name": "Dev Org"}
        assert store.requested_orgs == [_ORG_ID]

    def test_a_session_without_an_organisation_is_a_400(self) -> None:
        # A freshly signed-up user has no org selected yet; that is a
        # different failure from being unauthenticated.
        client = _client(FakeStore(session=(_profile(), _session_row(organisation_id=None))))
        client.cookies.set("session_token", "tok")

        response = client.get("/org")

        assert response.status_code == 400
        assert response.json()["detail"] == "No organisation selected"

    def test_no_cookie_is_a_401(self) -> None:
        assert _client(FakeStore()).get("/org").status_code == 401

    def test_an_unresolvable_token_is_a_401(self) -> None:
        client = _client(FakeStore(session=None))
        client.cookies.set("session_token", "stale")

        assert client.get("/org").status_code == 401


class TestGetOrgId:
    """The shorthand every org-scoped route depends on."""

    def test_hands_back_just_the_uuid(self) -> None:
        store = FakeStore(
            session=(_profile(), _session_row()),
            org=SimpleNamespace(id=_ORG_ID, name="Dev Org"),
        )
        client = _client(store)
        client.cookies.set("session_token", "tok")

        response = client.get("/org-id")

        assert response.json() == {"org_id": str(_ORG_ID)}

    @pytest.mark.parametrize(
        ("store", "expected"),
        [
            (FakeStore(), 401),
            (FakeStore(session=(_profile(), _session_row(organisation_id=None))), 400),
        ],
    )
    def test_it_inherits_the_org_resolution_failures(self, store: FakeStore, expected: int) -> None:
        client = _client(store)
        client.cookies.set("session_token", "tok")

        assert client.get("/org-id").status_code == expected
