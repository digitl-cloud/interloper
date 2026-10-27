"""Organisation routes: the tenant, its members and its invitations.

Two routers: ``router`` holds the collection; ``org_router`` is mounted under
the organisation's path and serves its own members, by role, and the platform's
super-admins, who act as ``admin`` in every organisation
(:func:`~interloper_api.dependencies.get_org_scope`). Deleting or joining an
organisation is for super-admins only.
Platform-wide views (every organisation, every user, quotas) live in
:mod:`interloper_api.routes.admin`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request, Response
from interloper_db import Organisation, OrganisationQuery, Page, PageQuery, Role
from interloper_db.models import UserOrganisation
from pydantic import BaseModel

from interloper_api.dependencies import (
    AdminDep,
    CurrentUserDep,
    StoreDep,
    SuperAdminDep,
    ViewerDep,
    get_smtp_config,
)
from interloper_api.notifications import InvitationEmail

router = APIRouter(prefix="/organisations", tags=["organisations"])
org_router = APIRouter(tags=["organisations"])


# -- Request & response models -------------------------------------------------


class OrganisationResponse(BaseModel):
    """Organisation summary."""

    id: UUID
    name: str
    slug: str
    created_at: datetime | None = None

    @classmethod
    def from_organisation(cls, organisation: Organisation) -> OrganisationResponse:
        """Describe an organisation row.

        Args:
            organisation: The organisation row.

        Returns:
            The response model.
        """
        return cls(
            id=organisation.id, name=organisation.name, slug=organisation.slug, created_at=organisation.created_at
        )


class CreateOrganisationRequest(BaseModel):
    """Request body for creating an organisation."""

    name: str


class UpdateOrganisationRequest(BaseModel):
    """Request body for renaming an organisation or changing its slug; omitted fields stay untouched."""

    name: str | None = None
    slug: str | None = None


class DeleteOrganisationRequest(BaseModel):
    """Confirmation body for deleting an organisation: it must repeat the exact name."""

    name: str


class MemberResponse(BaseModel):
    """Organisation member."""

    id: UUID
    email: str
    name: str | None = None
    avatar_url: str | None = None
    role: str

    @classmethod
    def from_membership(cls, membership: UserOrganisation) -> MemberResponse:
        """Describe a membership by its member's profile and role.

        Args:
            membership: The membership row, its ``profile`` loaded.

        Returns:
            The response model.
        """
        profile = membership.profile
        assert profile is not None
        return cls(
            id=profile.id,
            email=profile.email,
            name=profile.name,
            avatar_url=profile.avatar_url,
            role=membership.role,
        )


class MemberCreateRequest(BaseModel):
    """Request body for a super-admin joining an organisation without an invitation."""

    role: Role = Role.ADMIN


class MemberUpdateRequest(BaseModel):
    """Request body for changing a member's role."""

    role: Role


class InvitationCreateRequest(BaseModel):
    """Request body for inviting a user."""

    email: str
    role: Role = Role.VIEWER


class InvitationResponse(BaseModel):
    """Pending invitation."""

    id: UUID
    email: str
    role: str
    created_at: datetime | None = None
    expires_at: datetime


# -- Organisations -------------------------------------------------------------


@router.get("")
def list_organisations(
    user: CurrentUserDep,
    store: StoreDep,
    query: Annotated[PageQuery, Query()],
) -> Page[OrganisationResponse]:
    """List the organisations the caller belongs to.

    Args:
        user: The authenticated caller.
        store: The database store.
        query: The window to read.

    Returns:
        The page of the caller's organisations.
    """
    organisations = store.organisations.list(OrganisationQuery(user_id=user.id, **query.model_dump()))
    return organisations.map(OrganisationResponse.from_organisation)


@router.post("", status_code=201)
def create_organisation(body: CreateOrganisationRequest, user: CurrentUserDep, store: StoreDep) -> OrganisationResponse:
    """Create an organisation; the creator becomes its admin.

    Args:
        body: The name of the organisation to create.
        user: The authenticated caller.
        store: The database store.

    Returns:
        The created organisation.
    """
    return OrganisationResponse.from_organisation(store.organisations.create(name=body.name, creator_id=user.id))


@org_router.patch("")
def update_organisation(body: UpdateOrganisationRequest, scope: AdminDep, store: StoreDep) -> OrganisationResponse:
    """Rename an organisation or change its slug. Requires its admin role, or super-admin.

    A slug that is taken answers 409 and one that is malformed or reserved 400,
    through the store's errors.

    Args:
        body: The new name and/or slug.
        scope: The organisation the path names, the caller an admin there.
        store: The database store.

    Returns:
        The updated organisation.
    """
    updated = store.organisations.update(scope.organisation.id, name=body.name, slug=body.slug)
    return OrganisationResponse.from_organisation(updated)


@org_router.delete("", status_code=204)
def delete_organisation(
    org_id: UUID,
    body: DeleteOrganisationRequest,
    user: SuperAdminDep,
    store: StoreDep,
) -> Response:
    """Soft-delete an organisation: purge its data, keep its execution history and usage ledger.

    The body must repeat the exact name. Deleted organisations stay in the
    admin list (read-only) and read as missing everywhere else.

    Args:
        org_id: The organisation to soft-delete.
        body: The confirmation, repeating the organisation's exact name.
        user: The calling super-admin.
        store: The database store.

    Returns:
        An empty 204 response.

    Raises:
        HTTPException: 400 when the confirmation name does not match.
    """
    if body.name != store.organisations.get(org_id).name:
        raise HTTPException(status_code=400, detail="Organisation name does not match")
    store.organisations.delete(org_id)
    return Response(status_code=204)


# -- Members -------------------------------------------------------------------


@org_router.get("/members")
def list_members(
    scope: ViewerDep,
    store: StoreDep,
    query: Annotated[PageQuery, Query()],
) -> Page[MemberResponse]:
    """List an organisation's members with their roles. Any member, or super-admin.

    Args:
        scope: The organisation the path names.
        store: The database store.
        query: The window to read.

    Returns:
        The page of members.
    """
    return store.members.list(scope.organisation.id, query).map(MemberResponse.from_membership)


@org_router.post("/members", status_code=201)
def join_organisation(
    org_id: UUID,
    body: MemberCreateRequest,
    user: SuperAdminDep,
    store: StoreDep,
) -> MemberResponse:
    """Add the calling super-admin to an organisation, without an invitation.

    Args:
        org_id: The organisation to join.
        body: The role to take, ``admin`` unless overridden.
        user: The calling super-admin.
        store: The database store.

    Returns:
        The caller's new membership.

    Raises:
        HTTPException: 409 when the caller already belongs to the organisation.
    """
    store.organisations.get(org_id)
    if not store.members.add(org_id, user.id, body.role):
        raise HTTPException(status_code=409, detail="Already a member of this organisation")
    return MemberResponse(id=user.id, email=user.email, name=user.name, avatar_url=user.avatar_url, role=body.role)


@org_router.patch("/members/{user_id}")
def update_member(
    user_id: UUID,
    body: MemberUpdateRequest,
    scope: AdminDep,
    store: StoreDep,
) -> MemberResponse:
    """Change a member's role. Requires the organisation's admin role, or super-admin.

    Args:
        user_id: The member whose role changes.
        body: The new role.
        scope: The organisation the path names, the caller an admin there.
        store: The database store.

    Returns:
        The updated member.
    """
    return MemberResponse.from_membership(store.members.update(scope.organisation.id, user_id, body.role))


@org_router.delete("/members/{user_id}", status_code=204)
def remove_member(
    user_id: UUID,
    user: CurrentUserDep,
    scope: AdminDep,
    store: StoreDep,
) -> Response:
    """Remove a member. Requires the organisation's admin role, or super-admin.

    Args:
        user_id: The member to remove.
        user: The authenticated caller.
        scope: The organisation the path names, the caller an admin there.
        store: The database store.

    Returns:
        An empty 204 response.

    Raises:
        HTTPException: 400 when the caller targets their own membership, which
            would let an admin lock the organisation out of its admin seat.
    """
    if user_id == user.id:
        raise HTTPException(status_code=400, detail="Cannot remove yourself")
    store.members.delete(scope.organisation.id, user_id)
    return Response(status_code=204)


# -- Invitations ---------------------------------------------------------------


@org_router.get("/invitations")
def list_invitations(
    scope: AdminDep,
    store: StoreDep,
    query: Annotated[PageQuery, Query()],
) -> Page[InvitationResponse]:
    """List an organisation's pending invitations. Requires its admin role, or super-admin.

    Args:
        scope: The organisation the path names, the caller an admin there.
        store: The database store.
        query: The window to read.

    Returns:
        The page of invitations.
    """
    return store.invitations.list(scope.organisation.id, query).map(
        lambda invitation: InvitationResponse.model_validate(invitation, from_attributes=True)
    )


@org_router.post("/invitations", status_code=201)
def create_invitation(
    body: InvitationCreateRequest,
    request: Request,
    user: CurrentUserDep,
    scope: AdminDep,
    store: StoreDep,
) -> InvitationResponse:
    """Invite someone by email. Requires the organisation's admin role, or super-admin.

    The invitation is stored first and mailed best-effort: an unconfigured or
    failing mailer only means the recipient has to be handed the link another way.

    Args:
        body: The address to invite and the role granted on acceptance.
        request: The incoming request, whose base URL the invite link is built on.
        user: The authenticated caller, the inviter.
        scope: The organisation the path names, the caller an admin there.
        store: The database store.

    Returns:
        The created invitation.
    """
    organisation = scope.organisation
    invitation = store.invitations.create(organisation.id, email=body.email.strip(), role=body.role, invited_by=user.id)
    InvitationEmail.from_invitation(
        invitation, org_name=organisation.name, inviter=user, base_url=str(request.base_url)
    ).deliver(get_smtp_config(), invitation.email)
    return InvitationResponse.model_validate(invitation, from_attributes=True)


@org_router.delete("/invitations/{invitation_id}", status_code=204)
def delete_invitation(
    invitation_id: UUID,
    scope: AdminDep,
    store: StoreDep,
) -> Response:
    """Withdraw a pending invitation. Requires the organisation's admin role, or super-admin.

    Args:
        invitation_id: The invitation to withdraw; another organisation's
            invitation reads as missing.
        scope: The organisation the path names, the caller an admin there.
        store: The database store.

    Returns:
        An empty 204 response.
    """
    store.invitations.delete(invitation_id, org_id=scope.organisation.id)
    return Response(status_code=204)


@org_router.post("/invitations/{invitation_id}/resend", status_code=201)
def resend_invitation(
    invitation_id: UUID,
    request: Request,
    user: CurrentUserDep,
    scope: AdminDep,
    store: StoreDep,
) -> InvitationResponse:
    """Reissue an invitation with a fresh token and expiry, and mail it again.

    The previously mailed link stops working. Requires the organisation's
    admin role, or super-admin.

    Args:
        invitation_id: The invitation to reissue.
        request: The incoming request, whose base URL the invite link is built on.
        user: The authenticated caller, the inviter.
        scope: The organisation the path names, the caller an admin there.
        store: The database store.

    Returns:
        The reissued invitation.
    """
    organisation = scope.organisation
    invitation = store.invitations.reissue(invitation_id, org_id=organisation.id, invited_by=user.id)
    InvitationEmail.from_invitation(
        invitation, org_name=organisation.name, inviter=user, base_url=str(request.base_url)
    ).deliver(get_smtp_config(), invitation.email)
    return InvitationResponse.model_validate(invitation, from_attributes=True)
