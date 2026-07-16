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
from interloper_db import Profile, Store
from pydantic import BaseModel, Field

from interloper_api.dependencies import get_current_user, get_org_id, get_store, require_viewer

router = APIRouter(prefix="/tokens", tags=["tokens"])


# -- Response / Request models ------------------------------------------------


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
    """
    expires_at = None
    if body.expires_in_days is not None:
        expires_at = datetime.now(timezone.utc) + timedelta(days=body.expires_in_days)

    row, raw = store.create_token(user.id, org_id, name=body.name, expires_at=expires_at)
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
    """List the caller's tokens in the active organisation."""
    rows = store.list_tokens(user.id, org_id)
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
    """
    detail = f"Token {token_id} not found"
    row = store.get_token(token_id)
    if row is None:
        raise HTTPException(status_code=404, detail=detail)

    if row.user_id != user.id:
        role = store.get_user_role(user.id, row.organisation_id)
        if role != "admin":
            raise HTTPException(status_code=404, detail=detail)

    store.revoke_token(token_id)
    return {"status": "revoked"}
