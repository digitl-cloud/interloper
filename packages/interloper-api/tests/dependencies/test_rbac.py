"""Tests for ``interloper_api.dependencies.rbac``.

The role gates are the API's authorization boundary, so the 404-vs-403
distinction matters: a non-member must not be able to use an ID as an
existence oracle.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from fastapi import APIRouter, Depends, FastAPI, HTTPException
from fastapi.testclient import TestClient
from interloper.errors import NotFoundError
from interloper_db import Profile, Store

from interloper_api.dependencies import (
    authorize_org_member,
    get_current_user,
    get_org_id,
    get_store,
    load_authorized,
    require_admin,
    require_editor,
    require_super_admin,
    require_viewer,
)

_ORG_ID = uuid4()
_OTHER_ORG_ID = uuid4()
_USER_ID = uuid4()


class FakeStore:
    """Stand-in exposing only ``organisations.member_role``."""

    def __init__(self, role: str | None) -> None:
        """Set up the fake.

        Args:
            role: Role the user holds in any organisation asked about;
                ``None`` means not a member.
        """
        self.asked: list[tuple[UUID, UUID]] = []
        self._role = role
        self.organisations = SimpleNamespace(member_role=self._member_role)

    def _member_role(self, user_id: UUID, org_id: UUID) -> str | None:
        self.asked.append((user_id, org_id))
        return self._role


def _profile(is_super_admin: bool = False) -> Profile:
    """Build a profile stand-in.

    Args:
        is_super_admin: Whether the profile carries platform-wide privileges.

    Returns:
        The stand-in, typed as a ``Profile`` for the helpers under test.
    """
    return cast(
        Profile, SimpleNamespace(id=_USER_ID, email="user@example.com", is_super_admin=is_super_admin)
    )


def _store(role: str | None) -> Store:
    """Build a store stand-in resolving every membership to *role*.

    Args:
        role: Role the user holds; ``None`` means not a member.

    Returns:
        The stand-in, typed as a ``Store`` for the helpers under test.
    """
    return cast(Store, FakeStore(role))


class TestAuthorizeOrgMember:
    """The direct helper, checked against the resource's org rather than the session's."""

    def test_a_member_at_the_minimum_passes(self) -> None:
        store = FakeStore("viewer")

        authorize_org_member(_profile(), _ORG_ID, cast(Store, store), minimum="viewer")

        assert store.asked == [(_USER_ID, _ORG_ID)]

    def test_a_member_above_the_minimum_passes(self) -> None:
        authorize_org_member(_profile(), _ORG_ID, _store("admin"), minimum="editor")

    def test_a_non_member_gets_the_missing_resource_404(self) -> None:
        # Same detail as a genuinely missing resource, so the ID reveals nothing.
        with pytest.raises(HTTPException) as excinfo:
            authorize_org_member(
                _profile(),
                _ORG_ID,
                _store(None),
                detail="Asset 1 not found",
            )

        assert excinfo.value.status_code == 404
        assert excinfo.value.detail == "Asset 1 not found"

    def test_an_insufficient_role_gets_a_403(self) -> None:
        with pytest.raises(HTTPException) as excinfo:
            authorize_org_member(_profile(), _ORG_ID, _store("viewer"), minimum="admin")

        assert excinfo.value.status_code == 403
        assert excinfo.value.detail == "Requires admin role or higher"

    def test_an_unknown_role_name_is_treated_as_below_every_minimum(self) -> None:
        with pytest.raises(HTTPException) as excinfo:
            authorize_org_member(_profile(), _ORG_ID, _store("bogus"), minimum="viewer")

        assert excinfo.value.status_code == 403


class TestLoadAuthorized:
    """The shared ID-addressed fetch-then-authorize pattern."""

    def test_returns_the_entity_for_a_member(self) -> None:
        entity = SimpleNamespace(id=uuid4(), org_id=_ORG_ID)

        loaded = load_authorized(
            lambda entity_id: entity,
            entity.id,
            _profile(),
            _store("editor"),
            label="Asset",
        )

        assert loaded is entity

    def test_a_missing_entity_is_a_404_naming_the_label(self) -> None:
        entity_id = uuid4()

        with pytest.raises(HTTPException) as excinfo:
            load_authorized(
                lambda _id: (_ for _ in ()).throw(NotFoundError("gone")),
                entity_id,
                _profile(),
                _store("admin"),
                label="Asset",
            )

        assert excinfo.value.status_code == 404
        assert excinfo.value.detail == f"Asset {entity_id} not found"

    def test_an_entity_in_another_org_is_indistinguishable_from_missing(self) -> None:
        entity_id = uuid4()
        entity = SimpleNamespace(id=entity_id, org_id=_OTHER_ORG_ID)

        with pytest.raises(HTTPException) as excinfo:
            load_authorized(
                lambda _id: entity,
                entity_id,
                _profile(),
                _store(None),
                label="Asset",
            )

        assert excinfo.value.status_code == 404
        assert excinfo.value.detail == f"Asset {entity_id} not found"

    def test_a_member_with_too_low_a_role_gets_a_403(self) -> None:
        entity_id = uuid4()

        with pytest.raises(HTTPException) as excinfo:
            load_authorized(
                lambda _id: SimpleNamespace(id=entity_id, org_id=_ORG_ID),
                entity_id,
                _profile(),
                _store("viewer"),
                label="Asset",
                minimum="editor",
            )

        assert excinfo.value.status_code == 403

    def test_it_authorizes_against_the_entitys_org_not_the_session(self) -> None:
        # An ID-addressed endpoint works for a member of the owning org
        # regardless of which org the session currently has selected.
        store = FakeStore("admin")
        entity = SimpleNamespace(id=uuid4(), org_id=_OTHER_ORG_ID)

        load_authorized(lambda _id: entity, entity.id, _profile(), cast(Store, store), label="Asset")

        assert store.asked == [(_USER_ID, _OTHER_ORG_ID)]


def _gate_client(gate: Any, store: FakeStore | Store, user: Profile) -> TestClient:
    """Mount one role gate behind a probe route.

    Args:
        gate: The ``require_*`` dependency under test.
        store: The fake store the gate resolves roles against.
        user: The authenticated profile the gate receives.

    Returns:
        A client for the probe app.
    """
    router = APIRouter()

    @router.get("/gated")
    def gated(caller: Any = Depends(gate)) -> dict[str, str]:
        return {"email": caller.email}

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_current_user] = lambda: user
    app.dependency_overrides[get_org_id] = lambda: _ORG_ID
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
        response = _gate_client(gate, FakeStore(role), _profile()).get("/gated")

        assert (response.status_code == 200) is allowed
        if not allowed:
            assert response.status_code == 403

    @pytest.mark.parametrize("gate", [require_viewer, require_editor, require_admin])
    def test_a_non_member_is_refused_by_every_gate(self, gate: Any) -> None:
        response = _gate_client(gate, _store(None), _profile()).get("/gated")

        assert response.status_code == 403
        assert response.json()["detail"] == "Not a member of this organisation"

    def test_the_profile_passes_through_for_chaining(self) -> None:
        response = _gate_client(require_viewer, _store("viewer"), _profile()).get("/gated")

        assert response.json() == {"email": "user@example.com"}


class TestRequireSuperAdmin:
    """The cross-org admin surface is not bound to the active organisation."""

    def test_a_super_admin_passes(self) -> None:
        response = _gate_client(
            require_super_admin, _store(None), _profile(is_super_admin=True)
        ).get("/gated")

        assert response.status_code == 200

    def test_a_plain_user_is_refused_even_as_an_org_admin(self) -> None:
        response = _gate_client(require_super_admin, _store("admin"), _profile()).get("/gated")

        assert response.status_code == 403
        assert response.json()["detail"] == "Requires super-admin privileges"

    def test_it_never_consults_org_membership(self) -> None:
        store = FakeStore("admin")

        _gate_client(require_super_admin, cast(Store, store), _profile(is_super_admin=True)).get("/gated")

        assert store.asked == []
