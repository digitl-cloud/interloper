"""Request-scoped identity: who is calling, and in which organisation the request acts, or which run.

The caller proves who they are with the app's session cookie or an
``Authorization: Bearer`` token, each resolved for its own surface, so a
secret presented on the wrong one is refused; the routes that manage
credentials and memberships, and the super-admin ones, take the cookie alone.
The organisation is named by the path, never remembered between requests, and
is read with the credential, so an organisation route authenticates and
authorises in one statement. A run's pod authenticates as its run, with the
run token, and nothing else.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated
from uuid import UUID

from fastapi import Cookie, Depends, Header, HTTPException
from interloper_db import Audience, Organisation, Profile, Role, Run, Store, Token

from interloper_api.dependencies.state import StoreDep


def bearer(authorization: str | None) -> str | None:
    """The bearer token an ``Authorization`` header presents.

    Any other scheme (a proxy's HTTP Basic in front of a staging deployment)
    is not ours and reads as no token at all.

    Args:
        authorization: The header value, or ``None`` when absent.

    Returns:
        The token, empty when the scheme is bearer and nothing follows it;
        ``None`` for no header or another scheme.
    """
    scheme, _, secret = (authorization or "").partition(" ")
    return secret if scheme.lower() == "bearer" else None


def session_user(store: Store, session_token: str | None) -> Profile | None:
    """The user a session cookie stands for, when it stands for one.

    Args:
        store: The Store instance.
        session_token: The session cookie value, or ``None`` when absent.

    Returns:
        The user, or ``None`` when there is no cookie or it does not resolve.
    """
    resolved = store.tokens.resolve(session_token, Audience.APP) if session_token else None
    return resolved[0] if resolved else None


@dataclass(frozen=True)
class PresentedCredential:
    """The secret a request presents, and the surface it is resolved for.

    A bearer token resolves as an API credential (personal or access token)
    and the cookie as a login session; a bearer token, when presented, is the
    credential, so a failing one is never rescued by a cookie.

    Attributes:
        secret: The raw secret.
        audience: ``api`` for a bearer token, ``app`` for the session cookie.
    """

    secret: str
    audience: Audience

    @classmethod
    def from_request(cls, session_token: str | None, authorization: str | None) -> PresentedCredential:
        """Take the bearer token, else the session cookie.

        Args:
            session_token: The session cookie value, or ``None`` when absent.
            authorization: The ``Authorization`` header value, or ``None``
                when absent; a scheme other than bearer leaves the cookie to
                authenticate.

        Returns:
            The secret and the surface it resolves for.

        Raises:
            HTTPException: 401 when no credential is presented, or the bearer
                token is empty.
        """
        token = bearer(authorization)
        if token is None:
            if not session_token:
                raise HTTPException(status_code=401, detail="Not authenticated")
            return cls(secret=session_token, audience=Audience.APP)
        if not token:
            raise HTTPException(status_code=401, detail="Invalid or expired credentials")
        return cls(secret=token, audience=Audience.API)


@dataclass(frozen=True)
class CallerCredential:
    """The credential a request presented, and the user it stands for.

    Attributes:
        user: The authenticated user.
        token: The token row the secret resolved to; its ``client_id`` names
            the OAuth client for an access token and is ``None`` for a login
            session or a personal token.
    """

    user: Profile
    token: Token


def get_caller_credential(
    store: StoreDep,
    session_token: str | None = Cookie(default=None),
    authorization: str | None = Header(default=None),
) -> CallerCredential:
    """Resolve the credential from the bearer token, else the session cookie.

    Args:
        store: The Store instance.
        session_token: The session cookie value.
        authorization: The ``Authorization`` header value.

    Returns:
        The user and the token row that authenticated them.

    Raises:
        HTTPException: 401 when no credential is presented or the secret does
            not resolve for its surface.
    """
    presented = PresentedCredential.from_request(session_token, authorization)
    resolved = store.tokens.resolve(presented.secret, presented.audience)
    if resolved is None:
        raise HTTPException(status_code=401, detail="Invalid or expired credentials")
    return CallerCredential(user=resolved[0], token=resolved[1])


CallerCredentialDep = Annotated[CallerCredential, Depends(get_caller_credential)]


def get_caller(credential: CallerCredentialDep) -> Profile:
    """Resolve the caller as the user the request's credential stands for.

    Args:
        credential: The credential the request presented.

    Returns:
        The authenticated Profile.
    """
    return credential.user


CurrentUserDep = Annotated[Profile, Depends(get_caller)]


def get_session_caller(
    store: StoreDep,
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
    if bearer(authorization) is not None:
        raise HTTPException(status_code=401, detail="This route accepts only the session cookie")
    if not session_token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    user = session_user(store, session_token)
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid or expired credentials")
    return user


SessionUserDep = Annotated[Profile, Depends(get_session_caller)]


@dataclass(frozen=True)
class OrgScope:
    """Who acts, in which organisation, and with which role.

    Attributes:
        credential: The credential the request presented, and the user it
            stands for.
        organisation: The organisation the path names.
        role: The caller's live role in it; ``admin`` for a super-admin.
    """

    credential: CallerCredential
    organisation: Organisation
    role: str

    def require(self, minimum: str) -> OrgScope:
        """Refuse a caller whose role is below *minimum*.

        Args:
            minimum: Minimum role required (``viewer``, ``editor``, ``admin``).

        Returns:
            This scope, passed through for the route.

        Raises:
            HTTPException: 403 when the role is below the minimum.
        """
        if not Role.at_least(self.role, minimum):
            raise HTTPException(status_code=403, detail=f"Requires {minimum} role or higher")
        return self


def get_org_scope(
    org_id: UUID,
    store: StoreDep,
    session_token: str | None = Cookie(default=None),
    authorization: str | None = Header(default=None),
) -> OrgScope:
    """Resolve the caller, the organisation the path names and their role in it, in one statement.

    The credential is taken as :func:`get_caller_credential` takes it. A
    non-member reads the organisation as missing, so an id reveals nothing.

    Args:
        org_id: The organisation id from the path.
        store: The Store instance.
        session_token: The session cookie value.
        authorization: The ``Authorization`` header value.

    Returns:
        The caller's credential, the organisation and the caller's role in it.

    Raises:
        HTTPException: 401 when no credential is presented or the secret does
            not resolve for its surface; 404 when the organisation is missing,
            deleted, or the caller cannot act in it.
    """
    presented = PresentedCredential.from_request(session_token, authorization)
    resolved = store.tokens.resolve_in(presented.secret, presented.audience, org_id)
    if resolved is None:
        raise HTTPException(status_code=401, detail="Invalid or expired credentials")
    user, token, organisation, role = resolved
    if organisation is None or role is None:
        raise HTTPException(status_code=404, detail=f"Organisation {org_id} not found")
    return OrgScope(credential=CallerCredential(user=user, token=token), organisation=organisation, role=role)


OrgDep = Annotated[OrgScope, Depends(get_org_scope)]


def require_run(
    run_id: UUID,
    store: StoreDep,
    authorization: str | None = Header(default=None),
) -> Run:
    """Resolve the run a bearer run token stands for, and check it is the run the path names.

    Only a run token authenticates here, never a session or a personal
    token: a run's pod writes as its run and nothing else.

    Args:
        run_id: The run the path names.
        store: The Store instance.
        authorization: The ``Authorization`` header.

    Returns:
        The run, dispatched, running, or ended within the grace its token carries.

    Raises:
        HTTPException: 401 without a live run token; 404 when the token's run
            is not the path's, so a token cannot learn whether another run exists.
    """
    token = bearer(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="Run token required")
    run = store.runs.resolve_token(token)
    if run is None:
        raise HTTPException(status_code=401, detail="Unknown or expired run token")
    if run.id != run_id:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return run
