"""Tests for ``interloper_api.dependencies.auth``.

The dependencies are exercised through a router so the ``Cookie`` and
``Header`` defaults, the path parameter and FastAPI's dependency chaining are
the real ones, not hand-passed arguments.
"""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from typing import Annotated, Any
from uuid import UUID, uuid4

import pytest
from fastapi import APIRouter, Depends, FastAPI
from fastapi.testclient import TestClient
from interloper_db import Audience, NotFoundError, TokenKind

from interloper_api.app import ORG_PREFIX
from interloper_api.dependencies import (
    OrgScope,
    get_caller,
    get_org_scope,
    get_session_caller,
    get_settings,
    get_store,
    require_run,
)

_ORG_ID = uuid4()
_OTHER_ORG_ID = uuid4()


class FakeStore:
    """Stand-in exposing the ``tokens``, ``members``, ``organisations`` and ``runs`` facets these read.

    Tokens resolve by kind against the surface they are presented to, as the
    real resolver does, so the tests prove which surface each credential is
    resolved for.
    """

    def __init__(
        self,
        *,
        tokens: dict[str, TokenKind] | None = None,
        roles: dict[UUID, str] | None = None,
        live_orgs: set[UUID] | None = None,
        is_super_admin: bool = False,
        run: Any = None,
    ) -> None:
        """Set up the fake.

        Args:
            tokens: Raw secrets the caller holds, by kind.
            roles: The caller's role per organisation; absent means not a member.
            live_orgs: Organisations ``organisations.get`` finds; ``None``
                means every organisation exists.
            is_super_admin: Whether the caller is a super-admin.
            run: What ``runs.resolve_token`` returns; ``None`` means the run
                token is unknown or dead.
        """
        self.profile = SimpleNamespace(id=uuid4(), email="user@example.com", is_super_admin=is_super_admin)
        self.presented: list[tuple[str, Audience]] = []
        self._tokens = tokens or {}
        self._roles = roles or {}
        self._live_orgs = live_orgs
        self.tokens = SimpleNamespace(resolve=self._resolve)
        self.members = SimpleNamespace(role_of=self._role_of)
        self.organisations = SimpleNamespace(get=self._get_organisation)
        self.resolved_run_tokens: list[tuple[str, timedelta]] = []
        self._run = run
        self.runs = SimpleNamespace(resolve_token=self._resolve_run_token)

    def _role_of(self, org_id: UUID, user: Any) -> str | None:
        return "admin" if user.is_super_admin else self._roles.get(org_id)

    def _resolve(self, raw: str, audience: Audience) -> tuple[Any, Any] | None:
        self.presented.append((raw, audience))
        kind = self._tokens.get(raw)
        if kind is None or kind not in audience.kinds:
            return None
        return self.profile, SimpleNamespace(kind=kind)

    def _resolve_run_token(self, raw: str, *, grace: timedelta) -> Any:
        self.resolved_run_tokens.append((raw, grace))
        return self._run

    def _get_organisation(self, org_id: UUID) -> Any:
        if self._live_orgs is not None and org_id not in self._live_orgs:
            raise NotFoundError(f"Organisation {org_id} not found")
        return SimpleNamespace(id=org_id, name="Dev Org")


def _client(store: FakeStore) -> TestClient:
    """Mount the auth dependencies behind probe routes.

    Args:
        store: The fake store the dependencies resolve against.

    Returns:
        A client for the probe app.
    """
    router = APIRouter()

    @router.get("/user")
    def read_user(user: Annotated[Any, Depends(get_caller)]) -> dict[str, str]:
        return {"email": user.email}

    @router.get("/session-user")
    def read_session_user(user: Annotated[Any, Depends(get_session_caller)]) -> dict[str, str]:
        return {"email": user.email}

    org_router = APIRouter()

    @org_router.get("/scope")
    def read_scope(scope: Annotated[OrgScope, Depends(get_org_scope)]) -> dict[str, str]:
        return {"org": str(scope.organisation.id), "role": scope.role}

    app = FastAPI()
    app.include_router(router)
    app.include_router(org_router, prefix=ORG_PREFIX)
    app.dependency_overrides[get_store] = lambda: store
    return TestClient(app)


class TestGetCaller:
    """Resolving the caller from the session cookie or a bearer token."""

    def test_cookie_and_bearer_both_authenticate(self) -> None:
        store = FakeStore(tokens={"cookie": TokenKind.SESSION, "ilp_pat": TokenKind.PERSONAL})
        client = _client(store)

        by_bearer = client.get("/user", headers={"Authorization": "Bearer ilp_pat"})
        client.cookies.set("session_token", "cookie")
        by_cookie = client.get("/user")

        assert by_bearer.status_code == 200
        assert by_cookie.status_code == 200
        assert by_cookie.json() == {"email": "user@example.com"}
        assert store.presented == [("ilp_pat", Audience.API), ("cookie", Audience.APP)]

    def test_bearer_session_value_refused(self) -> None:
        client = _client(FakeStore(tokens={"cookie": TokenKind.SESSION}))

        response = client.get("/user", headers={"Authorization": "Bearer cookie"})

        assert response.status_code == 401

    def test_cookie_pat_value_refused(self) -> None:
        client = _client(FakeStore(tokens={"ilp_pat": TokenKind.PERSONAL}))
        client.cookies.set("session_token", "ilp_pat")

        response = client.get("/user")

        assert response.status_code == 401

    def test_a_failing_bearer_is_not_rescued_by_the_cookie(self) -> None:
        client = _client(FakeStore(tokens={"cookie": TokenKind.SESSION}))
        client.cookies.set("session_token", "cookie")

        response = client.get("/user", headers={"Authorization": "Bearer unknown"})

        assert response.status_code == 401

    @pytest.mark.parametrize("header", ["Basic dXNlcg==", "token"])
    def test_a_header_that_is_not_a_bearer_token_leaves_the_cookie_to_authenticate(self, header: str) -> None:
        store = FakeStore(tokens={"cookie": TokenKind.SESSION})
        client = _client(store)
        client.cookies.set("session_token", "cookie")

        response = client.get("/user", headers={"Authorization": header})

        assert response.status_code == 200
        assert store.presented == [("cookie", Audience.APP)]

    @pytest.mark.parametrize("header", ["Basic dXNlcg==", "token"])
    def test_a_header_that_is_not_a_bearer_token_alone_is_a_401(self, header: str) -> None:
        response = _client(FakeStore()).get("/user", headers={"Authorization": header})

        assert response.status_code == 401
        assert response.json()["detail"] == "Not authenticated"

    def test_an_empty_bearer_is_a_401_not_rescued_by_the_cookie(self) -> None:
        store = FakeStore(tokens={"cookie": TokenKind.SESSION})
        client = _client(store)
        client.cookies.set("session_token", "cookie")

        response = client.get("/user", headers={"Authorization": "Bearer"})

        assert response.status_code == 401
        assert store.presented == []

    def test_no_credential_is_a_401(self) -> None:
        response = _client(FakeStore()).get("/user")

        assert response.status_code == 401
        assert response.json()["detail"] == "Not authenticated"

    def test_an_unresolvable_cookie_is_a_401(self) -> None:
        client = _client(FakeStore())
        client.cookies.set("session_token", "stale")

        response = client.get("/user")

        assert response.status_code == 401
        assert response.json()["detail"] == "Invalid or expired credentials"


class TestGetSessionCaller:
    """The cookie-only caller of the credential and membership routes."""

    def test_the_session_cookie_authenticates(self) -> None:
        store = FakeStore(tokens={"cookie": TokenKind.SESSION})
        client = _client(store)
        client.cookies.set("session_token", "cookie")

        assert client.get("/session-user").json() == {"email": "user@example.com"}
        assert store.presented == [("cookie", Audience.APP)]

    def test_a_bearer_is_refused_without_being_resolved(self) -> None:
        store = FakeStore(tokens={"ilp_pat": TokenKind.PERSONAL, "cookie": TokenKind.SESSION})
        client = _client(store)
        client.cookies.set("session_token", "cookie")

        response = client.get("/session-user", headers={"Authorization": "Bearer ilp_pat"})

        assert response.status_code == 401
        assert response.json()["detail"] == "This route accepts only the session cookie"
        assert store.presented == []

    def test_a_header_that_is_not_a_bearer_token_leaves_the_cookie_to_authenticate(self) -> None:
        client = _client(FakeStore(tokens={"cookie": TokenKind.SESSION}))
        client.cookies.set("session_token", "cookie")

        response = client.get("/session-user", headers={"Authorization": "Basic dXNlcg=="})

        assert response.status_code == 200

    def test_a_pat_as_the_cookie_is_refused(self) -> None:
        client = _client(FakeStore(tokens={"ilp_pat": TokenKind.PERSONAL}))
        client.cookies.set("session_token", "ilp_pat")

        assert client.get("/session-user").status_code == 401

    def test_no_cookie_is_a_401(self) -> None:
        response = _client(FakeStore()).get("/session-user")

        assert response.status_code == 401
        assert response.json()["detail"] == "Not authenticated"


class TestGetOrgScope:
    """The organisation comes from the path, the role from the caller's membership there."""

    def _scope(self, store: FakeStore, org_id: UUID) -> Any:
        client = _client(store)
        client.cookies.set("session_token", "cookie")
        return client.get(f"/organisations/{org_id}/scope")

    def test_a_member_gets_their_role_in_the_path_org(self) -> None:
        store = FakeStore(tokens={"cookie": TokenKind.SESSION}, roles={_ORG_ID: "editor", _OTHER_ORG_ID: "viewer"})

        assert self._scope(store, _ORG_ID).json() == {"org": str(_ORG_ID), "role": "editor"}
        assert self._scope(store, _OTHER_ORG_ID).json() == {"org": str(_OTHER_ORG_ID), "role": "viewer"}

    def test_non_member_path_org_404(self) -> None:
        store = FakeStore(tokens={"cookie": TokenKind.SESSION}, roles={_ORG_ID: "admin"})

        response = self._scope(store, _OTHER_ORG_ID)

        assert response.status_code == 404
        assert response.json()["detail"] == f"Organisation {_OTHER_ORG_ID} not found"

    def test_super_admin_any_org(self) -> None:
        store = FakeStore(tokens={"cookie": TokenKind.SESSION}, is_super_admin=True)

        response = self._scope(store, _OTHER_ORG_ID)

        assert response.json() == {"org": str(_OTHER_ORG_ID), "role": "admin"}

    def test_a_super_admin_member_still_acts_as_admin(self) -> None:
        store = FakeStore(tokens={"cookie": TokenKind.SESSION}, roles={_ORG_ID: "viewer"}, is_super_admin=True)

        assert self._scope(store, _ORG_ID).json()["role"] == "admin"

    @pytest.mark.parametrize("is_super_admin", [True, False])
    def test_a_deleted_organisation_is_a_404_for_everyone(self, is_super_admin: bool) -> None:
        store = FakeStore(
            tokens={"cookie": TokenKind.SESSION},
            roles={_ORG_ID: "admin"},
            live_orgs=set(),
            is_super_admin=is_super_admin,
        )

        assert self._scope(store, _ORG_ID).status_code == 404

    def test_an_unauthenticated_caller_is_a_401_before_any_organisation(self) -> None:
        response = _client(FakeStore()).get(f"/organisations/{_ORG_ID}/scope")

        assert response.status_code == 401


def _run_client(store: FakeStore) -> TestClient:
    """Mount ``require_run`` behind a probe route.

    Args:
        store: The fake store the dependency resolves against.

    Returns:
        A client for the probe app.
    """
    router = APIRouter()

    @router.get("/runs/{run_id}/whoami")
    def whoami(run: Annotated[Any, Depends(require_run)]) -> dict[str, str]:
        return {"run": str(run.id)}

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_settings] = lambda: SimpleNamespace(reaper=SimpleNamespace(heartbeat_timeout=90))
    return TestClient(app)


class TestRequireRun:
    """A run's pod authenticates with its run's token, and only for that run."""

    def test_a_live_token_resolves_its_run_with_the_heartbeat_grace(self) -> None:
        run = SimpleNamespace(id=uuid4())
        store = FakeStore(run=run)

        response = _run_client(store).get(f"/runs/{run.id}/whoami", headers={"Authorization": "Bearer ilr_x"})

        assert response.status_code == 200 and response.json() == {"run": str(run.id)}
        assert store.resolved_run_tokens == [("ilr_x", timedelta(seconds=90))]

    def test_a_missing_or_dead_token_is_a_401(self) -> None:
        client = _run_client(FakeStore(run=None))

        assert client.get(f"/runs/{uuid4()}/whoami").status_code == 401
        assert client.get(f"/runs/{uuid4()}/whoami", headers={"Authorization": "Bearer ilr_x"}).status_code == 401

    def test_a_session_cookie_is_not_a_run_token(self) -> None:
        store = FakeStore(tokens={"cookie": TokenKind.SESSION}, run=None)
        client = _run_client(store)
        client.cookies.set("session_token", "cookie")

        assert client.get(f"/runs/{uuid4()}/whoami").status_code == 401
        assert store.presented == []

    def test_a_token_for_another_run_is_a_404(self) -> None:
        client = _run_client(FakeStore(run=SimpleNamespace(id=uuid4())))

        response = client.get(f"/runs/{uuid4()}/whoami", headers={"Authorization": "Bearer ilr_x"})

        assert response.status_code == 404
