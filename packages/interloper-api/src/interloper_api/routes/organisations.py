"""Organisation routes — CRUD, membership, and invitation management."""

from __future__ import annotations

import logging
from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Cookie, Depends, HTTPException, Request
from interloper_db import Profile, Store
from pydantic import BaseModel

from interloper_api.dependencies import get_current_user, get_org_id, get_store, require_admin, require_viewer
from interloper_api.email import send_invite_email

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/organisations", tags=["organisations"])


# -- Response / Request models ------------------------------------------------


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


class InviteRequest(BaseModel):
    """Request body for inviting a user."""

    email: str
    role: str = "viewer"


class InvitationResponse(BaseModel):
    """Pending invitation."""

    id: UUID
    email: str
    role: str
    created_at: datetime | None = None
    expires_at: datetime


# -- Helpers ------------------------------------------------------------------


def _get_smtp_config() -> object | None:
    """Return the SMTP config if available, without raising."""
    from interloper_api.dependencies import get_smtp_config

    return get_smtp_config()


def _send_invitation_email(
    request: Request,
    smtp_config: object,
    invitation: object,
    org_name: str,
    inviter_name: str,
) -> None:
    """Send the invitation email, logging errors without failing the request.

    Args:
        request: The FastAPI request (for building the invite URL).
        smtp_config: SmtpConfig instance.
        invitation: Invitation row with .token and .email.
        org_name: Organisation name.
        inviter_name: Inviter display name.
    """
    token = invitation.token  # type: ignore[attr-defined]
    email = invitation.email  # type: ignore[attr-defined]
    base_url = str(request.base_url).rstrip("/")
    invite_url = f"{base_url}/invite/{token}"

    try:
        send_invite_email(
            smtp_config=smtp_config,
            to=email,
            org_name=org_name,
            inviter_name=inviter_name,
            invite_url=invite_url,
        )
    except Exception:
        logger.exception("Failed to send invitation email to %s", email)


# -- Organisation CRUD -------------------------------------------------------


@router.post("", status_code=201)
def create_organisation(
    body: CreateOrganisationRequest,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
    session_token: str | None = Cookie(default=None),
) -> OrganisationResponse:
    """Create a new organisation. The creating user becomes its admin."""
    org = store.create_organisation(name=body.name, creator_id=user.id)  # type: ignore[arg-type]

    # Set as active org in session
    if session_token:
        store.set_session_org(session_token, org.id, user_id=user.id)  # type: ignore[arg-type]

    return OrganisationResponse.model_validate(org, from_attributes=True)


@router.get("")
def list_organisations(
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> list[OrganisationResponse]:
    """List all organisations the user belongs to."""
    orgs = store.list_user_organisations(user.id)  # type: ignore[arg-type]
    return [OrganisationResponse.model_validate(o, from_attributes=True) for o in orgs]


# -- Members ------------------------------------------------------------------


@router.get("/members")
def list_members(
    user: Profile = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[MemberResponse]:
    """List all members of the current organisation."""
    members = store.list_org_members(org_id)
    return [
        MemberResponse(
            id=profile.id,  # type: ignore[arg-type]
            email=profile.email,
            name=profile.name,
            avatar_url=profile.avatar_url,
            role=role,
        )
        for profile, role in members
    ]


@router.delete("/members/{user_id}")
def remove_member(
    user_id: UUID,
    user: Profile = Depends(require_admin),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Remove a member from the organisation. Requires admin role."""
    if user_id == user.id:
        raise HTTPException(status_code=400, detail="Cannot remove yourself")

    if not store.remove_org_member(org_id, user_id):
        raise HTTPException(status_code=404, detail="Member not found")

    return {"status": "ok"}


# -- Invitations --------------------------------------------------------------


@router.get("/invitations")
def list_invitations(
    user: Profile = Depends(require_admin),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[InvitationResponse]:
    """List pending invitations for the current organisation. Requires admin role."""
    invitations = store.list_invitations(org_id)
    return [
        InvitationResponse(
            id=inv.id,  # type: ignore[arg-type]
            email=inv.email,
            role=inv.role,
            created_at=inv.created_at,
            expires_at=inv.expires_at,
        )
        for inv in invitations
    ]


@router.post("/invite", status_code=201)
def invite_member(
    body: InviteRequest,
    request: Request,
    user: Profile = Depends(require_admin),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> InvitationResponse:
    """Invite a user to the organisation by email. Requires admin role."""
    invitation = store.create_invitation(
        org_id=org_id,
        email=body.email.strip(),
        role=body.role,
        invited_by=user.id,  # type: ignore[arg-type]
    )

    # Send invitation email if SMTP is configured
    smtp_config = _get_smtp_config()
    if smtp_config and smtp_config.enabled:  # type: ignore[attr-defined]
        org = store.get_organisation(org_id)
        org_name = org.name if org else "Unknown"
        inviter_name = user.name or user.email
        _send_invitation_email(request, smtp_config, invitation, org_name, inviter_name)

    return InvitationResponse(
        id=invitation.id,  # type: ignore[arg-type]
        email=invitation.email,
        role=invitation.role,
        created_at=invitation.created_at,
        expires_at=invitation.expires_at,
    )


@router.delete("/invitations/{invitation_id}")
def cancel_invitation(
    invitation_id: UUID,
    user: Profile = Depends(require_admin),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Cancel a pending invitation. Requires admin role."""
    if not store.delete_invitation(invitation_id):
        raise HTTPException(status_code=404, detail="Invitation not found")

    return {"status": "ok"}


@router.post("/invitations/{invitation_id}/resend")
def resend_invitation(
    invitation_id: UUID,
    request: Request,
    user: Profile = Depends(require_admin),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Resend an invitation (recreates with fresh expiry). Requires admin role."""
    invitations = store.list_invitations(org_id)
    target = next((inv for inv in invitations if inv.id == invitation_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="Invitation not found")

    store.delete_invitation(invitation_id)
    new_invitation = store.create_invitation(
        org_id=org_id,
        email=target.email,
        role=target.role,
        invited_by=user.id,  # type: ignore[arg-type]
    )

    # Send invitation email if SMTP is configured
    smtp_config = _get_smtp_config()
    if smtp_config and smtp_config.enabled:  # type: ignore[attr-defined]
        org = store.get_organisation(org_id)
        org_name = org.name if org else "Unknown"
        inviter_name = user.name or user.email
        _send_invitation_email(request, smtp_config, new_invitation, org_name, inviter_name)

    return {"status": "ok"}
