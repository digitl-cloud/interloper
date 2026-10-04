"""Organisation routes — CRUD, membership, and invitation management.

The member and invitation models here are the API's one shape for both, shared
with the super-admin surface in :mod:`interloper_api.routes.admin`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Cookie, HTTPException, Request
from interloper_db import Profile, Role
from pydantic import BaseModel

from interloper_api.dependencies import (
    AdminDep,
    CurrentUserDep,
    OrgIdDep,
    StoreDep,
    ViewerDep,
    get_smtp_config,
)
from interloper_api.notifications import InvitationEmail

router = APIRouter(prefix="/organisations", tags=["organisations"])


# -- Response / Request models -------------------------------------------------


class OrganisationResponse(BaseModel):
    """Organisation summary."""

    id: UUID
    name: str
    created_at: datetime | None = None


class CreateOrganisationRequest(BaseModel):
    """Request body for creating an organisation."""

    name: str


class MemberResponse(BaseModel):
    """Organisation member."""

    id: UUID
    email: str
    name: str | None = None
    avatar_url: str | None = None
    role: str

    @classmethod
    def from_profile(cls, profile: Profile, role: str) -> MemberResponse:
        """Describe a profile as a member holding *role*.

        Args:
            profile: The member's profile.
            role: The role the membership grants.

        Returns:
            The response model.
        """
        return cls(id=profile.id, email=profile.email, name=profile.name, avatar_url=profile.avatar_url, role=role)


class InviteRequest(BaseModel):
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


# -- Organisation CRUD ---------------------------------------------------------


@router.post("", status_code=201)
def create_organisation(
    body: CreateOrganisationRequest,
    user: CurrentUserDep,
    store: StoreDep,
    session_token: Annotated[str | None, Cookie()] = None,
) -> OrganisationResponse:
    """Create a new organisation. The creating user becomes its admin.

    Args:
        body: The name of the organisation to create.
        user: The authenticated caller.
        store: The database store.
        session_token: The session cookie; when present, the new organisation
            also becomes the session's active one.

    Returns:
        The created organisation.
    """
    org = store.organisations.create(name=body.name, creator_id=user.id)

    if session_token:
        store.auth.set_session_org(session_token, org.id, user_id=user.id)

    return OrganisationResponse.model_validate(org, from_attributes=True)


@router.get("")
def list_organisations(
    user: CurrentUserDep,
    store: StoreDep,
) -> list[OrganisationResponse]:
    """List all organisations the user belongs to.

    Args:
        user: The authenticated caller.
        store: The database store.

    Returns:
        Every organisation the caller is a member of.
    """
    orgs = store.organisations.list_for_user(user.id)
    return [OrganisationResponse.model_validate(o, from_attributes=True) for o in orgs]


# -- Members -------------------------------------------------------------------


@router.get("/members")
def list_members(
    user: ViewerDep,
    org_id: OrgIdDep,
    store: StoreDep,
) -> list[MemberResponse]:
    """List all members of the current organisation.

    Args:
        user: The authenticated caller, required to hold at least the viewer role.
        org_id: The active organisation, resolved from the session.
        store: The database store.

    Returns:
        Every member of the organisation with the role they hold in it.
    """
    return [MemberResponse.from_profile(profile, role) for profile, role in store.organisations.list_members(org_id)]


@router.delete("/members/{user_id}")
def remove_member(
    user_id: UUID,
    user: AdminDep,
    org_id: OrgIdDep,
    store: StoreDep,
) -> dict[str, str]:
    """Remove a member from the organisation. Requires admin role.

    Args:
        user_id: The member to remove.
        user: The authenticated caller, required to hold the admin role.
        org_id: The active organisation, resolved from the session.
        store: The database store.

    Returns:
        A status acknowledgement.

    Raises:
        HTTPException: 400 when the caller targets their own membership.
    """
    if user_id == user.id:
        raise HTTPException(status_code=400, detail="Cannot remove yourself")

    store.organisations.remove_member(org_id, user_id)

    return {"status": "ok"}


# -- Invitations ---------------------------------------------------------------


@router.get("/invitations")
def list_invitations(
    user: AdminDep,
    org_id: OrgIdDep,
    store: StoreDep,
) -> list[InvitationResponse]:
    """List pending invitations for the current organisation. Requires admin role.

    Args:
        user: The authenticated caller, required to hold the admin role.
        org_id: The active organisation, resolved from the session.
        store: The database store.

    Returns:
        The invitations that are still outstanding for the organisation.
    """
    return [
        InvitationResponse.model_validate(invitation, from_attributes=True)
        for invitation in store.organisations.list_invitations(org_id)
    ]


@router.post("/invite", status_code=201)
def invite_member(
    body: InviteRequest,
    request: Request,
    user: AdminDep,
    org_id: OrgIdDep,
    store: StoreDep,
) -> InvitationResponse:
    """Invite a user to the organisation by email. Requires admin role.

    The invitation is created whether or not email is configured; a missing SMTP
    config only means the recipient has to be handed the link by other means.

    Args:
        body: The address to invite and the role to grant on acceptance.
        request: The incoming request, used to build the invite URL.
        user: The authenticated caller, required to hold the admin role.
        org_id: The active organisation, resolved from the session.
        store: The database store.

    Returns:
        The created invitation.
    """
    invitation = store.organisations.create_invitation(
        org_id=org_id,
        email=body.email.strip(),
        role=body.role,
        invited_by=user.id,
    )
    InvitationEmail.from_invitation(
        invitation, org_name=store.organisations.get(org_id).name, inviter=user, base_url=str(request.base_url)
    ).deliver(get_smtp_config(), invitation.email)
    return InvitationResponse.model_validate(invitation, from_attributes=True)


@router.delete("/invitations/{invitation_id}")
def cancel_invitation(
    invitation_id: UUID,
    user: AdminDep,
    org_id: OrgIdDep,
    store: StoreDep,
) -> dict[str, str]:
    """Cancel a pending invitation. Requires admin role.

    Args:
        invitation_id: The invitation to cancel.
        user: The authenticated caller, required to hold the admin role.
        org_id: The active organisation, resolved from the session.
        store: The database store.

    Returns:
        A status acknowledgement.
    """
    store.organisations.delete_invitation(invitation_id, org_id=org_id)
    return {"status": "ok"}


@router.post("/invitations/{invitation_id}/resend")
def resend_invitation(
    invitation_id: UUID,
    request: Request,
    user: AdminDep,
    org_id: OrgIdDep,
    store: StoreDep,
) -> dict[str, str]:
    """Resend an invitation (recreates with fresh expiry). Requires admin role.

    The old invitation is deleted and a new one issued, so the previously mailed
    link stops working.

    Args:
        invitation_id: The invitation to reissue.
        request: The incoming request, used to build the invite URL.
        user: The authenticated caller, required to hold the admin role.
        org_id: The active organisation, resolved from the session.
        store: The database store.

    Returns:
        A status acknowledgement.
    """
    previous = store.organisations.get_invitation(invitation_id, org_id=org_id)
    store.organisations.delete_invitation(invitation_id, org_id=org_id)
    invitation = store.organisations.create_invitation(
        org_id=org_id,
        email=previous.email,
        role=previous.role,
        invited_by=user.id,
    )
    InvitationEmail.from_invitation(
        invitation, org_name=store.organisations.get(org_id).name, inviter=user, base_url=str(request.base_url)
    ).deliver(get_smtp_config(), invitation.email)
    return {"status": "ok"}
