"""Organisation routes: the tenant, its members and its invitations.

Every route below the collection takes the organisation from the path and
serves both its own members, by role, and the platform's super-admins through
the same endpoints (:func:`~interloper_api.dependencies.authorize_organisation`).
Platform-wide views (every organisation, every user, quotas) live in
:mod:`interloper_api.routes.admin`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Cookie, HTTPException, Query, Request, Response
from interloper_db import Organisation, OrganisationQuery, Page, PageQuery, Role
from interloper_db.models import UserOrganisation
from pydantic import BaseModel

from interloper_api.dependencies import (
    CurrentUserDep,
    StoreDep,
    SuperAdminDep,
    authorize_organisation,
    get_smtp_config,
)
from interloper_api.notifications import InvitationEmail

router = APIRouter(prefix="/organisations", tags=["organisations"])


# -- Request & response models -------------------------------------------------


class OrganisationResponse(BaseModel):
    """Organisation summary."""

    id: UUID
    name: str
    created_at: datetime | None = None

    @classmethod
    def from_organisation(cls, organisation: Organisation) -> OrganisationResponse:
        """Describe an organisation row.

        Args:
            organisation: The organisation row.

        Returns:
            The response model.
        """
        return cls(id=organisation.id, name=organisation.name, created_at=organisation.created_at)


class CreateOrganisationRequest(BaseModel):
    """Request body for creating an organisation."""

    name: str


class UpdateOrganisationRequest(BaseModel):
    """Request body for renaming an organisation."""

    name: str


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
def create_organisation(
    body: CreateOrganisationRequest,
    user: CurrentUserDep,
    store: StoreDep,
    session_token: Annotated[str | None, Cookie()] = None,
) -> OrganisationResponse:
    """Create an organisation; the creator becomes its admin.

    Args:
        body: The name of the organisation to create.
        user: The authenticated caller.
        store: The database store.
        session_token: The session cookie; when present, the new organisation
            also becomes the session's active one.

    Returns:
        The created organisation.
    """
    organisation = store.organisations.create(name=body.name, creator_id=user.id)
    if session_token:
        store.sessions.switch_org(session_token, organisation.id, user.id)
    return OrganisationResponse.from_organisation(organisation)


@router.patch("/{org_id}")
def update_organisation(
    org_id: UUID,
    body: UpdateOrganisationRequest,
    user: CurrentUserDep,
    store: StoreDep,
) -> OrganisationResponse:
    """Rename an organisation. Requires its admin role, or super-admin.

    Args:
        org_id: The organisation to rename.
        body: The new name.
        user: The authenticated caller.
        store: The database store.

    Returns:
        The renamed organisation.
    """
    authorize_organisation(user, org_id, store, minimum="admin")
    return OrganisationResponse.from_organisation(store.organisations.update(org_id, name=body.name))


@router.delete("/{org_id}", status_code=204)
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


@router.get("/{org_id}/members")
def list_members(
    org_id: UUID,
    user: CurrentUserDep,
    store: StoreDep,
    query: Annotated[PageQuery, Query()],
) -> Page[MemberResponse]:
    """List an organisation's members with their roles. Any member, or super-admin.

    Args:
        org_id: The organisation whose members are listed.
        user: The authenticated caller.
        store: The database store.
        query: The window to read.

    Returns:
        The page of members.
    """
    authorize_organisation(user, org_id, store)
    return store.members.list(org_id, query).map(MemberResponse.from_membership)


@router.post("/{org_id}/members", status_code=201)
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


@router.patch("/{org_id}/members/{user_id}")
def update_member(
    org_id: UUID,
    user_id: UUID,
    body: MemberUpdateRequest,
    user: CurrentUserDep,
    store: StoreDep,
) -> MemberResponse:
    """Change a member's role. Requires the organisation's admin role, or super-admin.

    Args:
        org_id: The organisation the membership belongs to.
        user_id: The member whose role changes.
        body: The new role.
        user: The authenticated caller.
        store: The database store.

    Returns:
        The updated member.
    """
    authorize_organisation(user, org_id, store, minimum="admin")
    return MemberResponse.from_membership(store.members.update(org_id, user_id, body.role))


@router.delete("/{org_id}/members/{user_id}", status_code=204)
def remove_member(
    org_id: UUID,
    user_id: UUID,
    user: CurrentUserDep,
    store: StoreDep,
) -> Response:
    """Remove a member. Requires the organisation's admin role, or super-admin.

    Args:
        org_id: The organisation the membership belongs to.
        user_id: The member to remove.
        user: The authenticated caller.
        store: The database store.

    Returns:
        An empty 204 response.

    Raises:
        HTTPException: 400 when the caller targets their own membership, which
            would let an admin lock the organisation out of its admin seat.
    """
    authorize_organisation(user, org_id, store, minimum="admin")
    if user_id == user.id:
        raise HTTPException(status_code=400, detail="Cannot remove yourself")
    store.members.delete(org_id, user_id)
    return Response(status_code=204)


# -- Invitations ---------------------------------------------------------------


@router.get("/{org_id}/invitations")
def list_invitations(
    org_id: UUID,
    user: CurrentUserDep,
    store: StoreDep,
    query: Annotated[PageQuery, Query()],
) -> Page[InvitationResponse]:
    """List an organisation's pending invitations. Requires its admin role, or super-admin.

    Args:
        org_id: The organisation whose invitations are listed.
        user: The authenticated caller.
        store: The database store.
        query: The window to read.

    Returns:
        The page of invitations.
    """
    authorize_organisation(user, org_id, store, minimum="admin")
    return store.invitations.list(org_id, query).map(
        lambda invitation: InvitationResponse.model_validate(invitation, from_attributes=True)
    )


@router.post("/{org_id}/invitations", status_code=201)
def create_invitation(
    org_id: UUID,
    body: InvitationCreateRequest,
    request: Request,
    user: CurrentUserDep,
    store: StoreDep,
) -> InvitationResponse:
    """Invite someone by email. Requires the organisation's admin role, or super-admin.

    The invitation is stored first and mailed best-effort: an unconfigured or
    failing mailer only means the recipient has to be handed the link another way.

    Args:
        org_id: The organisation to invite into.
        body: The address to invite and the role granted on acceptance.
        request: The incoming request, whose base URL the invite link is built on.
        user: The authenticated caller.
        store: The database store.

    Returns:
        The created invitation.
    """
    authorize_organisation(user, org_id, store, minimum="admin")
    invitation = store.invitations.create(org_id, email=body.email.strip(), role=body.role, invited_by=user.id)
    InvitationEmail.from_invitation(
        invitation, org_name=store.organisations.get(org_id).name, inviter=user, base_url=str(request.base_url)
    ).deliver(get_smtp_config(), invitation.email)
    return InvitationResponse.model_validate(invitation, from_attributes=True)


@router.delete("/{org_id}/invitations/{invitation_id}", status_code=204)
def delete_invitation(
    org_id: UUID,
    invitation_id: UUID,
    user: CurrentUserDep,
    store: StoreDep,
) -> Response:
    """Withdraw a pending invitation. Requires the organisation's admin role, or super-admin.

    Args:
        org_id: The organisation the invitation belongs to; another
            organisation's invitation reads as missing.
        invitation_id: The invitation to withdraw.
        user: The authenticated caller.
        store: The database store.

    Returns:
        An empty 204 response.
    """
    authorize_organisation(user, org_id, store, minimum="admin")
    store.invitations.delete(invitation_id, org_id=org_id)
    return Response(status_code=204)


@router.post("/{org_id}/invitations/{invitation_id}/resend", status_code=201)
def resend_invitation(
    org_id: UUID,
    invitation_id: UUID,
    request: Request,
    user: CurrentUserDep,
    store: StoreDep,
) -> InvitationResponse:
    """Reissue an invitation with a fresh token and expiry, and mail it again.

    The previously mailed link stops working. Requires the organisation's
    admin role, or super-admin.

    Args:
        org_id: The organisation the invitation belongs to.
        invitation_id: The invitation to reissue.
        request: The incoming request, whose base URL the invite link is built on.
        user: The authenticated caller.
        store: The database store.

    Returns:
        The reissued invitation.
    """
    authorize_organisation(user, org_id, store, minimum="admin")
    invitation = store.invitations.reissue(invitation_id, org_id=org_id, invited_by=user.id)
    InvitationEmail.from_invitation(
        invitation, org_name=store.organisations.get(org_id).name, inviter=user, base_url=str(request.base_url)
    ).deliver(get_smtp_config(), invitation.email)
    return InvitationResponse.model_validate(invitation, from_attributes=True)
