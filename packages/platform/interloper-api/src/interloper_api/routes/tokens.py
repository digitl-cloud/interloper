"""Personal access token routes: mint, list, and revoke the caller's own tokens.

A token authenticates programmatic clients (the MCP server, CLIs) as its
holder, in every organisation the holder belongs to, with their live role in
each. The routes are about the user, not an organisation. Management is
session-cookie-only by design: a leaked token must not be able to mint
further tokens.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Response
from interloper_db import NotFoundError, Page, PageQuery
from pydantic import BaseModel, Field

from interloper_api.dependencies import SessionUserDep, StoreDep

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
    user: SessionUserDep,
    store: StoreDep,
) -> CreatedTokenResponse:
    """Create a personal access token for the caller.

    The token acts as its holder in every organisation they belong to, with
    their live role in each, so no escalation is possible. The raw token is
    returned exactly once and cannot be recovered afterwards.

    Args:
        body: The token name and its lifetime in days; a null lifetime mints a
            token that never expires.
        user: The caller, authenticated by the session cookie alone.
        store: The database store.

    Returns:
        The token metadata together with the raw token, the one and only time it
        is disclosed.
    """
    expires_at = None
    if body.expires_in_days is not None:
        expires_at = datetime.now(timezone.utc) + timedelta(days=body.expires_in_days)

    row, raw = store.tokens.create_personal(user.id, body.name, expires_at)
    return CreatedTokenResponse(
        **TokenResponse.model_validate(row, from_attributes=True).model_dump(),
        token=raw,
    )


@router.get("")
def list_tokens(
    user: SessionUserDep,
    store: StoreDep,
    query: Annotated[PageQuery, Query()],
) -> Page[TokenResponse]:
    """List the caller's personal access tokens.

    Args:
        user: The caller, authenticated by the session cookie alone.
        store: The database store.
        query: The window to read.

    Returns:
        The page of the caller's tokens, newest first, revoked and expired
        ones included, as metadata only.
    """
    tokens = store.tokens.list(user.id, query)
    return tokens.map(lambda row: TokenResponse.model_validate(row, from_attributes=True))


@router.delete("/{token_id}", status_code=204)
def revoke_token(
    token_id: UUID,
    user: SessionUserDep,
    store: StoreDep,
) -> Response:
    """Revoke one of the caller's tokens.

    Only the owner revokes a token: it is theirs, not an organisation's, and
    removing a member's membership is what cuts their tokens off from an
    organisation. Missing and someone else's get the same 404, so token IDs
    don't act as an existence oracle.

    Args:
        token_id: The token to revoke.
        user: The caller, authenticated by the session cookie alone.
        store: The database store.

    Returns:
        An empty 204 response; the row stays, stamped revoked.

    Raises:
        HTTPException: 404 when the token does not exist or is not the
            caller's.
    """
    detail = f"Token {token_id} not found"
    try:
        row = store.tokens.get(token_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail=detail) from None

    if row.user_id != user.id:
        raise HTTPException(status_code=404, detail=detail)
    store.tokens.revoke(token_id)
    return Response(status_code=204)
