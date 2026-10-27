"""Which organisation a tool call acts in, and with which role.

The properties under test: the caller names the organisation, or leaves it
out when they belong to exactly one; a non-member is refused; a super-admin
acts as admin in any live organisation but must name it; membership is read
live on every call.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import interloper as il
from interloper_db import engine as engine_module
from interloper_db.models import Organisation, Profile
from interloper_db.store import Store
from interloper_toolkit import ToolError, ToolkitContext
from sqlmodel import Session

from interloper_mcp.context import get_ctx, init_context, set_static_user


def _act_as(store: Store, catalog: il.Catalog, profile: Profile) -> None:
    init_context(store, catalog)
    set_static_user(store.profiles.get(profile.id))


def _second_org(store: Store, profile: Profile) -> Organisation:
    return store.organisations.create(name="Globex", creator_id=profile.id)


def test_single_membership_used_when_omitted(store: Store, catalog: il.Catalog, seeded: dict[str, Any]):
    store.members.update(seeded["org"].id, seeded["profile"].id, "editor")
    _act_as(store, catalog, seeded["profile"])

    context = get_ctx()

    assert isinstance(context, ToolkitContext)
    assert context.org_id == seeded["org"].id
    assert context.role == "editor"


def test_named_membership_uses_its_role(store: Store, catalog: il.Catalog, seeded: dict[str, Any]):
    other = _second_org(store, seeded["profile"])
    store.members.update(other.id, seeded["profile"].id, "viewer")
    _act_as(store, catalog, seeded["profile"])

    context = get_ctx(str(other.id))

    assert isinstance(context, ToolkitContext)
    assert context.org_id == other.id
    assert context.role == "viewer"


def test_several_memberships_require_naming(store: Store, catalog: il.Catalog, seeded: dict[str, Any]):
    other = _second_org(store, seeded["profile"])
    _act_as(store, catalog, seeded["profile"])

    refused = get_ctx()

    assert isinstance(refused, ToolError)
    assert "organisation" in refused.error
    assert sorted(refused.valid_values or []) == sorted([str(seeded["org"].id), str(other.id)])


def test_non_member_refused(store: Store, catalog: il.Catalog, seeded: dict[str, Any]):
    stranger = store.profiles.upsert(google_id="g-stranger", email="stranger@example.com")
    other = _second_org(store, stranger)
    _act_as(store, catalog, seeded["profile"])

    refused = get_ctx(str(other.id))

    assert isinstance(refused, ToolError)
    assert refused.error == f"Organisation {other.id} not found"
    assert refused.valid_values == [str(seeded["org"].id)]


def test_malformed_organisation_refused(store: Store, catalog: il.Catalog, seeded: dict[str, Any]):
    _act_as(store, catalog, seeded["profile"])

    refused = get_ctx("acme")

    assert isinstance(refused, ToolError)
    assert refused.error == "Organisation acme not found"


def test_super_admin_any_org_as_admin(store: Store, catalog: il.Catalog, seeded: dict[str, Any]):
    admin = store.profiles.upsert(google_id="g-root", email="root@example.com")
    store.profiles.set_super_admin(admin.id, value=True)
    _act_as(store, catalog, admin)

    context = get_ctx(str(seeded["org"].id))

    assert isinstance(context, ToolkitContext)
    assert context.org_id == seeded["org"].id
    assert context.role == "admin"


def test_super_admin_refused_on_a_deleted_organisation(store: Store, catalog: il.Catalog, seeded: dict[str, Any]):
    admin = store.profiles.upsert(google_id="g-root", email="root@example.com")
    store.profiles.set_super_admin(admin.id, value=True)
    with Session(engine_module.get_engine()) as session:
        org = session.get(Organisation, seeded["org"].id)
        assert org is not None
        org.deleted_at = datetime.now(timezone.utc)
        session.add(org)
        session.commit()
    _act_as(store, catalog, admin)

    assert isinstance(get_ctx(str(seeded["org"].id)), ToolError)


def test_super_admin_without_memberships_must_name(store: Store, catalog: il.Catalog, seeded: dict[str, Any]):
    admin = store.profiles.upsert(google_id="g-root", email="root@example.com")
    store.profiles.set_super_admin(admin.id, value=True)
    _act_as(store, catalog, admin)

    refused = get_ctx()

    assert isinstance(refused, ToolError)
    assert "Name the organisation" in refused.error
    assert refused.valid_values == []


def test_member_removed_is_refused_next_call(store: Store, catalog: il.Catalog, seeded: dict[str, Any]):
    org, profile = seeded["org"], seeded["profile"]
    keeper = store.profiles.upsert(google_id="g-keeper", email="keeper@example.com")
    store.members.add(org.id, keeper.id, "admin")
    _act_as(store, catalog, profile)
    assert isinstance(get_ctx(str(org.id)), ToolkitContext)

    store.members.delete(org.id, profile.id)

    assert isinstance(get_ctx(str(org.id)), ToolError)
    assert isinstance(get_ctx(), ToolError)
