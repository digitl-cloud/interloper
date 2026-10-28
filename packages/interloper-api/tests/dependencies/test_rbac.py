"""Tests for ``interloper_api.dependencies.rbac``.

The role gates are the API's authorization boundary, so the 404-vs-403
distinction matters: an entity of another organisation must read as missing,
so an id is no existence oracle.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from fastapi import APIRouter, Depends, FastAPI, HTTPException
from fastapi.testclient import TestClient
from interloper_db import NotFoundError, Organisation, Profile

from interloper_api.app import ORG_PREFIX
from interloper_api.dependencies import (
    OrgScope,
    get_caller,
    get_session_caller,
    get_store,
    load_authorized,
    require_admin,
    require_editor,
    require_super_admin,
    require_viewer,
)

_ORG_ID = uuid4()
_OTHER_ORG_ID = uuid4()


class FakeStore:
    """Stand-in exposing ``members.role_of`` per organisation and ``organisations.get``."""

    def __init__(self, roles: dict[UUID, str] | None = None) -> None:
        """Set up the fake.

        Args:
            roles: The caller's role per organisation; absent means not a member.
        """
        self.asked: list[UUID] = []
        self._roles = roles or {}
        self.members = SimpleNamespace(role_of=self._role_of)
        self.tokens = SimpleNamespace(resolve=lambda raw, audience: None)
        self.organisations = SimpleNamespace(get=lambda org_id: SimpleNamespace(id=org_id))

    def _role_of(self, org_id: UUID, user: Any) -> str | None:
        if user.is_super_admin:
            return "admin"
        self.asked.append(org_id)
        return self._roles.get(org_id)


def _profile(is_super_admin: bool = False) -> Profile:
    """Build a profile stand-in.

    Args:
        is_super_admin: Whether the profile carries platform-wide privileges.

    Returns:
        The stand-in, typed as a ``Profile`` for the helpers under test.
    """
    return cast(Profile, SimpleNamespace(id=uuid4(), email="user@example.com", is_super_admin=is_super_admin))


def _scope(role: str, org_id: UUID = _ORG_ID) -> OrgScope:
    """Build the scope a request in *org_id* resolves to.

    Args:
        role: The caller's role there.
        org_id: The organisation the path names.

    Returns:
        The scope.
    """
    return OrgScope(organisation=cast(Organisation, SimpleNamespace(id=org_id)), role=role)


class TestLoadAuthorized:
    """The shared ID-addressed fetch-then-authorize pattern."""

    def test_returns_the_entity_of_the_path_org(self) -> None:
        entity = SimpleNamespace(id=uuid4(), org_id=_ORG_ID)

        loaded = load_authorized(lambda entity_id: entity, entity.id, _scope("viewer"), label="Asset")

        assert loaded is entity

    def test_a_missing_entity_is_a_404_naming_the_label(self) -> None:
        entity_id = uuid4()

        def fetch(_id: UUID) -> Any:
            raise NotFoundError("gone")

        with pytest.raises(HTTPException) as excinfo:
            load_authorized(fetch, entity_id, _scope("admin"), label="Asset")

        assert excinfo.value.status_code == 404
        assert excinfo.value.detail == f"Asset {entity_id} not found"

    def test_an_entity_of_another_org_is_indistinguishable_from_missing(self) -> None:
        # Even an admin of the path's organisation, who may be a member of
        # the entity's own, reads it as missing here.
        entity_id = uuid4()
        entity = SimpleNamespace(id=entity_id, org_id=_OTHER_ORG_ID)

        with pytest.raises(HTTPException) as excinfo:
            load_authorized(lambda _id: entity, entity_id, _scope("admin"), label="Asset")

        assert excinfo.value.status_code == 404
        assert excinfo.value.detail == f"Asset {entity_id} not found"

    def test_a_role_below_the_minimum_is_a_403(self) -> None:
        entity_id = uuid4()

        with pytest.raises(HTTPException) as excinfo:
            load_authorized(
                lambda _id: SimpleNamespace(id=entity_id, org_id=_ORG_ID),
                entity_id,
                _scope("viewer"),
                label="Asset",
                minimum="editor",
            )

        assert excinfo.value.status_code == 403
        assert excinfo.value.detail == "Requires editor role or higher"


def _gate_client(gate: Any, store: FakeStore, user: Profile) -> TestClient:
    """Mount one role gate behind a probe route under the organisation prefix.

    Args:
        gate: The ``require_*`` dependency under test.
        store: The fake store the scope resolves roles against.
        user: The authenticated profile.

    Returns:
        A client for the probe app.
    """
    org_router = APIRouter()

    @org_router.get("/gated")
    def gated(passed: Any = Depends(gate)) -> dict[str, str]:  # noqa: FAST002 - `gate` is a closure variable
        return {"role": getattr(passed, "role", None) or "super-admin"}

    app = FastAPI()
    app.include_router(org_router, prefix=ORG_PREFIX)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_caller] = lambda: user
    app.dependency_overrides[get_session_caller] = lambda: user
    return TestClient(app)


class TestRoleGates:
    """``require_viewer`` / ``require_editor`` / ``require_admin`` rank the same ladder."""

    @pytest.mark.parametrize(
        ("gate", "role", "allowed"),
        [
            (require_viewer, "viewer", True),
            (require_viewer, "editor", True),
            (require_viewer, "admin", True),
            (require_editor, "viewer", False),
            (require_editor, "editor", True),
            (require_editor, "admin", True),
            (require_admin, "viewer", False),
            (require_admin, "editor", False),
            (require_admin, "admin", True),
        ],
    )
    def test_the_role_ladder(self, gate: Any, role: str, allowed: bool) -> None:
        response = _gate_client(gate, FakeStore({_ORG_ID: role}), _profile()).get(f"/organisations/{_ORG_ID}/gated")

        assert response.status_code == (200 if allowed else 403)

    def test_role_from_path_org(self) -> None:
        # Editor in one organisation, viewer in another: a write is allowed
        # only under the organisation the caller edits.
        client = _gate_client(require_editor, FakeStore({_ORG_ID: "editor", _OTHER_ORG_ID: "viewer"}), _profile())

        assert client.get(f"/organisations/{_ORG_ID}/gated").status_code == 200
        refused = client.get(f"/organisations/{_OTHER_ORG_ID}/gated")
        assert refused.status_code == 403
        assert refused.json()["detail"] == "Requires editor role or higher"

    @pytest.mark.parametrize("gate", [require_viewer, require_editor, require_admin])
    def test_a_non_member_reads_the_organisation_as_missing(self, gate: Any) -> None:
        response = _gate_client(gate, FakeStore(), _profile()).get(f"/organisations/{_ORG_ID}/gated")

        assert response.status_code == 404

    def test_an_unknown_role_name_is_below_every_minimum(self) -> None:
        response = _gate_client(require_viewer, FakeStore({_ORG_ID: "bogus"}), _profile()).get(
            f"/organisations/{_ORG_ID}/gated"
        )

        assert response.status_code == 403

    def test_a_super_admin_passes_the_admin_gate_anywhere(self) -> None:
        store = FakeStore()

        response = _gate_client(require_admin, store, _profile(is_super_admin=True)).get(
            f"/organisations/{_ORG_ID}/gated"
        )

        assert response.json() == {"role": "admin"}
        assert store.asked == []


class TestRequireSuperAdmin:
    """The cross-org admin surface is not bound to any organisation."""

    def test_a_super_admin_passes(self) -> None:
        response = _gate_client(require_super_admin, FakeStore(), _profile(is_super_admin=True)).get(
            f"/organisations/{_ORG_ID}/gated"
        )

        assert response.status_code == 200

    def test_a_plain_user_is_refused_even_as_an_org_admin(self) -> None:
        response = _gate_client(require_super_admin, FakeStore({_ORG_ID: "admin"}), _profile()).get(
            f"/organisations/{_ORG_ID}/gated"
        )

        assert response.status_code == 403
        assert response.json()["detail"] == "Requires super-admin privileges"

    def test_it_never_consults_org_membership(self) -> None:
        store = FakeStore({_ORG_ID: "admin"})

        _gate_client(require_super_admin, store, _profile(is_super_admin=True)).get(f"/organisations/{_ORG_ID}/gated")

        assert store.asked == []

    def test_only_the_session_cookie_reaches_it(self) -> None:
        # A super-admin's token acts in the organisations it names, never across them.
        store = FakeStore()
        store.tokens = SimpleNamespace(resolve=lambda raw, audience: (_profile(is_super_admin=True), SimpleNamespace()))
        app = FastAPI()
        app.add_api_route("/gated", lambda user=Depends(require_super_admin): {"email": user.email})
        app.dependency_overrides[get_store] = lambda: store
        client = TestClient(app)

        refused = client.get("/gated", headers={"Authorization": "Bearer ilp_x"})
        client.cookies.set("session_token", "cookie")
        passed = client.get("/gated")

        assert refused.status_code == 401
        assert passed.status_code == 200
