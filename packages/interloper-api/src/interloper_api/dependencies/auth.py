"""Request-scoped identity: who is calling, and in which organisation the request acts, or which run.

The caller proves who they are with the app's session cookie or an
``Authorization: Bearer`` token, each resolved for its own surface, so a
secret presented on the wrong one is refused; the routes that manage
credentials and memberships, and the super-admin ones, take the cookie alone.
The organisation is named by the path, never remembered between requests. A run's pod authenticates as its
run, with the run token, and nothing else.
"""

from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import Annotated, Any
from uuid import UUID

from fastapi import Cookie, Depends, Header, HTTPException
from interloper_db import Audience, NotFoundError, Organisation, Profile, Role, Run, Store

from interloper_api.dependencies.state import get_reaper_config, get_store


def get_caller(
    store: Store = Depends(get_store),
    session_token: str | None = Cookie(default=None),
    authorization: str | None = Header(default=None),
) -> Profile:
    """Resolve the caller from the bearer token, else the session cookie.

    A bearer token resolves as an API credential (personal or access token)
    and the cookie as a login session; a bearer token, when presented, is the
    credential, so a failing one is never rescued by a cookie. Any other
    ``Authorization`` scheme (a proxy's HTTP Basic in front of a staging
    deployment) is not ours and leaves the cookie to authenticate.

    Args:
        store: The Store instance.
        session_token: The session cookie value.
        authorization: The ``Authorization`` header value.

    Returns:
        The authenticated Profile.

    Raises:
        HTTPException: 401 when no credential is presented or the secret does
            not resolve for its surface.
    """
    scheme, _, secret = (authorization or "").partition(" ")
    if scheme.lower() == "bearer":
        resolved = store.tokens.resolve(secret, Audience.API) if secret else None
    elif session_token:
        resolved = store.tokens.resolve(session_token, Audience.APP)
    else:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if resolved is None:
        raise HTTPException(status_code=401, detail="Invalid or expired credentials")
    profile, _ = resolved
    return profile


CurrentUserDep = Annotated[Profile, Depends(get_caller)]


def get_session_caller(
    store: Store = Depends(get_store),
    session_token: str | None = Cookie(default=None),
    authorization: str | None = Header(default=None),
) -> Profile:
    """Resolve the caller from the session cookie alone, refusing any bearer token.

    Credential and membership management (tokens, logout, invitations, the
    last organisation) and the platform-wide super-admin surface answer only
    the browser's login session, so a leaked token can neither mint its
    successors, act on the user's sessions or memberships, nor act across
    organisations.

    Args:
        store: The Store instance.
        session_token: The session cookie value.
        authorization: The ``Authorization`` header value; a bearer token
            alone refuses the request, any other scheme is ignored.

    Returns:
        The authenticated Profile.

    Raises:
        HTTPException: 401 when a bearer token is presented, or the session
            cookie is missing or does not resolve.
    """
    if (authorization or "").partition(" ")[0].lower() == "bearer":
        raise HTTPException(status_code=401, detail="This route accepts only the session cookie")
    if not session_token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    resolved = store.tokens.resolve(session_token, Audience.APP)
    if resolved is None:
        raise HTTPException(status_code=401, detail="Invalid or expired credentials")
    profile, _ = resolved
    return profile


SessionUserDep = Annotated[Profile, Depends(get_session_caller)]


@dataclass(frozen=True)
class OrgScope:
    """The organisation a request acts in, and the role the caller holds there.

    Attributes:
        organisation: The organisation the path names.
        role: The caller's live role in it; ``admin`` for a super-admin.
    """

    organisation: Organisation
    role: str


def get_org_scope(org_id: UUID, user: CurrentUserDep, store: Store = Depends(get_store)) -> OrgScope:
    """Resolve the organisation the path names and the caller's role in it.

    A super-admin acts as ``admin`` in every live organisation; anyone else
    must be a member. A non-member reads the organisation as missing, so an id
    reveals nothing.

    Args:
        org_id: The organisation id from the path.
        user: The authenticated caller.
        store: The Store instance.

    Returns:
        The organisation and the caller's role in it.

    Raises:
        HTTPException: 404 when the organisation is missing, deleted, or the
            caller is neither a member nor a super-admin.
    """
    role = Role.ADMIN.value if user.is_super_admin else store.members.role(org_id, user.id)
    if role is not None:
        with suppress(NotFoundError):
            return OrgScope(organisation=store.organisations.get(org_id), role=role)
    raise HTTPException(status_code=404, detail=f"Organisation {org_id} not found")


OrgDep = Annotated[OrgScope, Depends(get_org_scope)]


def require_run(
    run_id: UUID,
    store: Store = Depends(get_store),
    reaper: Any = Depends(get_reaper_config),
    authorization: str | None = Header(default=None),
) -> Run:
    """Resolve the run a bearer run token stands for, and check it is the run the path names.

    Only a run token authenticates here, never a session or a personal
    token: a run's pod writes as its run and nothing else.

    Args:
        run_id: The run the path names.
        store: The Store instance.
        reaper: The run liveness settings; a token outlives its run by the
            heartbeat timeout, for a pod still delivering after its reaping.
        authorization: The ``Authorization`` header.

    Returns:
        The run, dispatched, running, or ended within the grace.

    Raises:
        HTTPException: 401 without a live run token; 404 when the token's run
            is not the path's, so a token cannot learn whether another run exists.
    """
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(status_code=401, detail="Run token required")
    run = store.runs.resolve_token(token, grace=timedelta(seconds=reaper.heartbeat_timeout))
    if run is None:
        raise HTTPException(status_code=401, detail="Unknown or expired run token")
    if run.id != run_id:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return run
