"""Super-admin routes — cross-organisation management.

These endpoints are gated by :func:`require_super_admin` and are NOT bound to
the session's active organisation. They let a platform super-admin manage every
organisation's metadata, membership, and invitations. They deliberately grant
no access to org-scoped *data* (sources, jobs, runs, …).
"""

from __future__ import annotations

import logging
from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from interloper_db import Profile, Store
from pydantic import BaseModel

from interloper_api.dependencies import get_store, require_super_admin
from interloper_api.email import send_invite_email

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])

_ROLES = {"viewer", "editor", "admin"}


# -- Response / Request models ------------------------------------------------


class AdminOrganisationResponse(BaseModel):
    """Organisation summary with member count for the admin surface."""

    id: UUID
    name: str
    member_count: int
    created_at: datetime | None = None


class CreateOrganisationRequest(BaseModel):
    """Request body for creating an organisation."""

    name: str


class UpdateOrganisationRequest(BaseModel):
    """Request body for renaming an organisation."""

    name: str


class MemberResponse(BaseModel):
    """Organisation member."""

    id: UUID
    email: str
    name: str | None = None
    avatar_url: str | None = None
    role: str


class UpdateRoleRequest(BaseModel):
    """Request body for changing a member's role."""

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


def _require_org(store: Store, org_id: UUID) -> object:
    """Fetch an organisation or raise 404."""
    org = store.get_organisation(org_id)
    if not org:
        raise HTTPException(status_code=404, detail="Organisation not found")
    return org


def _validate_role(role: str) -> str:
    """Validate a role string or raise 400."""
    if role not in _ROLES:
        raise HTTPException(status_code=400, detail=f"Invalid role: {role}")
    return role


def _send_invitation_email(
    request: Request,
    invitation: object,
    org_name: str,
    inviter_name: str,
) -> None:
    """Send the invitation email if SMTP is configured, never failing the request."""
    from interloper_api.dependencies import get_smtp_config

    smtp_config = get_smtp_config()
    if not smtp_config or not smtp_config.enabled:  # type: ignore[attr-defined]
        return

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


# -- Organisations ------------------------------------------------------------


@router.get("/organisations")
def list_all_organisations(
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> list[AdminOrganisationResponse]:
    """List every organisation with its member count."""
    return [
        AdminOrganisationResponse(
            id=org.id,  # type: ignore[arg-type]
            name=org.name,
            member_count=count,
            created_at=org.created_at,
        )
        for org, count in store.list_all_organisations()
    ]


@router.post("/organisations", status_code=201)
def create_organisation(
    body: CreateOrganisationRequest,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> AdminOrganisationResponse:
    """Create an organisation. The super-admin is not added as a member."""
    org = store.create_organisation(name=body.name)
    return AdminOrganisationResponse(
        id=org.id,  # type: ignore[arg-type]
        name=org.name,
        member_count=0,
        created_at=org.created_at,
    )


@router.patch("/organisations/{org_id}")
def update_organisation(
    org_id: UUID,
    body: UpdateOrganisationRequest,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> AdminOrganisationResponse:
    """Rename an organisation."""
    org = store.update_organisation(org_id, body.name)
    if not org:
        raise HTTPException(status_code=404, detail="Organisation not found")
    members = store.list_org_members(org_id)
    return AdminOrganisationResponse(
        id=org.id,  # type: ignore[arg-type]
        name=org.name,
        member_count=len(members),
        created_at=org.created_at,
    )


# -- Members ------------------------------------------------------------------


@router.get("/organisations/{org_id}/members")
def list_members(
    org_id: UUID,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> list[MemberResponse]:
    """List all members of any organisation."""
    _require_org(store, org_id)
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


@router.patch("/organisations/{org_id}/members/{user_id}")
def update_member_role(
    org_id: UUID,
    user_id: UUID,
    body: UpdateRoleRequest,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Change a member's role in any organisation."""
    _validate_role(body.role)
    if not store.update_member_role(org_id, user_id, body.role):
        raise HTTPException(status_code=404, detail="Member not found")
    return {"status": "ok"}


@router.delete("/organisations/{org_id}/members/{user_id}")
def remove_member(
    org_id: UUID,
    user_id: UUID,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Remove a member from any organisation."""
    if not store.remove_org_member(org_id, user_id):
        raise HTTPException(status_code=404, detail="Member not found")
    return {"status": "ok"}


# -- Invitations --------------------------------------------------------------


@router.get("/organisations/{org_id}/invitations")
def list_invitations(
    org_id: UUID,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> list[InvitationResponse]:
    """List pending invitations for any organisation."""
    _require_org(store, org_id)
    return [
        InvitationResponse(
            id=inv.id,  # type: ignore[arg-type]
            email=inv.email,
            role=inv.role,
            created_at=inv.created_at,
            expires_at=inv.expires_at,
        )
        for inv in store.list_invitations(org_id)
    ]


@router.post("/organisations/{org_id}/invitations", status_code=201)
def invite_member(
    org_id: UUID,
    body: InviteRequest,
    request: Request,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> InvitationResponse:
    """Invite a user to any organisation by email."""
    org = _require_org(store, org_id)
    _validate_role(body.role)
    invitation = store.create_invitation(
        org_id=org_id,
        email=body.email.strip(),
        role=body.role,
        invited_by=user.id,  # type: ignore[arg-type]
    )

    inviter_name = user.name or user.email
    _send_invitation_email(request, invitation, org.name, inviter_name)  # type: ignore[attr-defined]

    return InvitationResponse(
        id=invitation.id,  # type: ignore[arg-type]
        email=invitation.email,
        role=invitation.role,
        created_at=invitation.created_at,
        expires_at=invitation.expires_at,
    )


@router.delete("/organisations/{org_id}/invitations/{invitation_id}")
def cancel_invitation(
    org_id: UUID,
    invitation_id: UUID,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Cancel a pending invitation in any organisation."""
    if not store.delete_invitation(invitation_id):
        raise HTTPException(status_code=404, detail="Invitation not found")
    return {"status": "ok"}
