"""Personal access token routes — mint, list, and revoke API tokens.

Tokens authenticate programmatic clients (the MCP server, CLIs) as their
holder in one organisation, with the holder's live role. Management is
session-cookie-only by design: a leaked token must not be able to mint
further tokens.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from interloper.errors import NotFoundError
from interloper_db import Profile, Store
from pydantic import BaseModel, Field

from interloper_api.dependencies import get_current_user, get_org_id, get_store, require_viewer

router = APIRouter(prefix="/tokens", tags=["tokens"])


# -- Response / Request models -------------------------------------------------


class CreateTokenRequest(BaseModel):
    """Request body for creating a token."""

    name: str = Field(min_length=1, max_length=100)
    expires_in_days: int | None = Field(default=90, ge=1, le=3650)


class TokenResponse(BaseModel):
    """Token metadata — never carries secret material."""

    id: UUID
    name: str
    token_prefix: str
    organisation_id: UUID
    created_at: datetime | None = None
    expires_at: datetime | None = None
    last_used_at: datetime | None = None
    revoked_at: datetime | None = None


class CreatedTokenResponse(TokenResponse):
    """Creation response: the only place the raw token ever appears."""

    token: str


# -- Routes --------------------------------------------------------------------


@router.post("", status_code=201)
def create_token(
    body: CreateTokenRequest,
    user: Profile = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> CreatedTokenResponse:
    """Create a personal access token scoped to the active organisation.

    Any org member may mint one: the token conveys only the holder's own
    live role, so no escalation is possible. The raw token is returned
    exactly once and cannot be recovered afterwards.

    Args:
        body: The token name and its lifetime in days; a null lifetime mints a
            token that never expires.
        user: The authenticated caller, required to hold at least the viewer role.
        org_id: The active organisation, resolved from the session.
        store: The database store.

    Returns:
        The token metadata together with the raw token, the one and only time it
        is disclosed.
    """
    expires_at = None
    if body.expires_in_days is not None:
        expires_at = datetime.now(timezone.utc) + timedelta(days=body.expires_in_days)

    row, raw = store.tokens.create(user.id, org_id, name=body.name, expires_at=expires_at)
    return CreatedTokenResponse(
        id=row.id,
        name=row.name,
        token_prefix=row.token_prefix,
        organisation_id=row.organisation_id,
        created_at=row.created_at,
        expires_at=row.expires_at,
        last_used_at=row.last_used_at,
        revoked_at=row.revoked_at,
        token=raw,
    )


@router.get("")
def list_tokens(
    user: Profile = Depends(require_viewer),
    org_id: UUID = Depends(get_org_id),
    store: Store = Depends(get_store),
) -> list[TokenResponse]:
    """List the caller's tokens in the active organisation.

    Args:
        user: The authenticated caller, required to hold at least the viewer role.
        org_id: The active organisation, resolved from the session.
        store: The database store.

    Returns:
        The caller's tokens, newest first, revoked and expired ones included,
        as metadata only.
    """
    rows = store.tokens.list_all(user.id, org_id)
    return [TokenResponse.model_validate(row, from_attributes=True) for row in rows]


@router.delete("/{token_id}")
def revoke_token(
    token_id: UUID,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Revoke a token.

    The owner may revoke their own tokens; an org admin may revoke any token
    scoped to their organisation. Missing and unauthorized get the same 404,
    so token IDs don't act as an existence oracle.

    Args:
        token_id: The token to revoke.
        user: The authenticated caller.
        store: The database store.

    Returns:
        A status acknowledgement.

    Raises:
        HTTPException: 404 when the token does not exist, or when the caller
            neither owns it nor administers its organisation.
    """
    detail = f"Token {token_id} not found"
    try:
        row = store.tokens.get(token_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail=detail) from None

    if row.user_id != user.id:
        role = store.organisations.member_role(user.id, row.organisation_id)
        if role != "admin":
            raise HTTPException(status_code=404, detail=detail)

    store.tokens.revoke(token_id)
    return {"status": "revoked"}
