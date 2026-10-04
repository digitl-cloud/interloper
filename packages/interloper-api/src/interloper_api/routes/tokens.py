"""Personal access token routes — mint, list, and revoke API tokens.

Tokens authenticate programmatic clients (the MCP server, CLIs) as their
holder in one organisation, with the holder's live role. Management is
session-cookie-only by design: a leaked token must not be able to mint
further tokens.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Response
from interloper.errors import NotFoundError
from interloper_db import Page, PageQuery, Role, TokenQuery
from pydantic import BaseModel, Field

from interloper_api.dependencies import (
    CurrentUserDep,
    OrgIdDep,
    StoreDep,
    ViewerDep,
)

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
    user: ViewerDep,
    org_id: OrgIdDep,
    store: StoreDep,
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
    user: ViewerDep,
    org_id: OrgIdDep,
    store: StoreDep,
    query: Annotated[PageQuery, Query()],
) -> Page[TokenResponse]:
    """List the caller's tokens in the active organisation.

    Args:
        user: The authenticated caller, required to hold at least the viewer role.
        org_id: The active organisation, resolved from the session.
        store: The database store.
        query: The window to read.

    Returns:
        The page of the caller's tokens, newest first, revoked and expired
        ones included, as metadata only.
    """
    tokens = store.tokens.list(user.id, TokenQuery(org_id=org_id, **query.model_dump()))
    return tokens.map(lambda row: TokenResponse.model_validate(row, from_attributes=True))


@router.delete("/{token_id}", status_code=204)
def revoke_token(
    token_id: UUID,
    user: CurrentUserDep,
    store: StoreDep,
) -> Response:
    """Revoke a token.

    The owner may revoke their own tokens; an org admin may revoke any token
    scoped to their organisation. Missing and unauthorized get the same 404,
    so token IDs don't act as an existence oracle.

    Args:
        token_id: The token to revoke.
        user: The authenticated caller.
        store: The database store.

    Returns:
        An empty 204 response; the row stays, stamped revoked.

    Raises:
        HTTPException: 404 when the token does not exist, or when the caller
            neither owns it nor administers its organisation.
    """
    detail = f"Token {token_id} not found"
    try:
        row = store.tokens.get(token_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail=detail) from None

    if row.user_id != user.id and not Role.at_least(store.members.role(row.organisation_id, user.id), "admin"):
        raise HTTPException(status_code=404, detail=detail)
    store.tokens.revoke(token_id)
    return Response(status_code=204)
