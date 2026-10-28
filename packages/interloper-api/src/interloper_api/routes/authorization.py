"""The OAuth 2.1 authorization server's endpoints: interloper granting access to clients.

Not to be confused with ``routes/oauth.py``, where interloper is the client of
data providers. The flow:

1. ``GET /auth/oauth/authorize`` validates the client's request and sends the
   browser to the app's consent page, ``/authorize?request=<sealed>``. With no
   session, the Google login runs first and returns to the authorize call, so
   the request is always sealed for the user who will decide on it.
2. The page reads ``GET /auth/oauth/consent`` and posts the user's decision
   to ``POST /auth/oauth/consent``, both on the session cookie alone; the
   answer is where to send the browser, back to the client with a code or an
   error.
3. The client exchanges the code at ``POST /auth/oauth/token``, refreshes
   there, and may revoke at ``POST /auth/oauth/revoke``. Clients register at
   ``POST /auth/oauth/register``; the server describes itself at
   ``/.well-known/oauth-authorization-server``.
"""

from __future__ import annotations

import json
from typing import Annotated
from urllib.parse import parse_qsl, urlencode, urlsplit

from authlib.oauth2 import OAuth2Error
from fastapi import APIRouter, Cookie, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
from interloper_db import Audience, Profile
from pydantic import BaseModel

from interloper_api.authorization import (
    AUTHORIZE_PATH,
    METADATA_PATH,
    AuthorizationCodeGrant,
    OAuthRequest,
    RegistrationRequest,
)
from interloper_api.dependencies import AuthorizationServerDep, SessionUserDep, StoreDep, session_user

router = APIRouter(prefix="/auth/oauth", tags=["authorization"])

well_known_router = APIRouter(tags=["authorization"])

EXPIRED_REQUEST = (
    "The authorization request is invalid, has expired or was started by another user; "
    "start again from the application."
)


# -- Request readers -----------------------------------------------------------


async def read_form_request(request: Request, server: AuthorizationServerDep) -> OAuthRequest:
    """Read a token or revocation request: its urlencoded body, awaited once.

    Args:
        request: The incoming request.
        server: The authorization server, whose issuer the request URL is built on.

    Returns:
        The snapshot Authlib reads; parameters outside an urlencoded body are
        ignored, as OAuth requires.
    """
    form: list[tuple[str, str]] = []
    if request.headers.get("content-type", "").split(";")[0].strip() == "application/x-www-form-urlencoded":
        form = parse_qsl((await request.body()).decode(errors="replace"), keep_blank_values=True)
    return OAuthRequest(request.method, server.uri(request.url.path), request.headers, form=form)


async def read_registration_request(request: Request, server: AuthorizationServerDep) -> RegistrationRequest:
    """Read a client registration request: its JSON body, awaited once.

    Args:
        request: The incoming request.
        server: The authorization server, whose issuer the request URL is built on.

    Returns:
        The snapshot Authlib reads; a body that is not a JSON object reads as
        empty, which registration refuses.
    """
    try:
        data = json.loads(await request.body() or b"{}")
    except ValueError:
        data = {}
    return RegistrationRequest(server.uri(request.url.path), request.headers, data if isinstance(data, dict) else {})


FormRequestDep = Annotated[OAuthRequest, Depends(read_form_request)]
RegistrationRequestDep = Annotated[RegistrationRequest, Depends(read_registration_request)]


# -- Metadata & registration ---------------------------------------------------


@well_known_router.get(METADATA_PATH)
def metadata(server: AuthorizationServerDep) -> JSONResponse:
    """Describe the authorization server (RFC 8414).

    Args:
        server: The authorization server.

    Returns:
        The metadata document.
    """
    return JSONResponse(server.metadata)


@router.post("/register")
def register(registration: RegistrationRequestDep, server: AuthorizationServerDep) -> Response:
    """Register a client (RFC 7591), open to anyone.

    Args:
        registration: The registration request.
        server: The authorization server.

    Returns:
        201 with the client's id (and secret, for a confidential client), or
        400 with the OAuth error.
    """
    return server.create_endpoint_response("client_registration", registration)


# -- Authorization & consent ---------------------------------------------------


@router.get("/authorize")
def authorize(
    request: Request,
    server: AuthorizationServerDep,
    store: StoreDep,
    session_token: Annotated[str | None, Cookie()] = None,
) -> Response:
    """Validate a client's authorization request and send the browser to the consent page.

    Args:
        request: The incoming request, whose query is the authorization request.
        server: The authorization server.
        store: The store the session cookie is resolved through.
        session_token: The app's session cookie.

    Returns:
        A redirect to the app's consent page carrying the request sealed for
        the session's user; to the Google login, returning here, when no
        session resolves; to the client with the error once its redirect URI
        is validated; a 400 before that.
    """
    oauth_request = server.authorization_request(list(request.query_params.multi_items()))
    try:
        server.get_consent_grant(oauth_request)
    except OAuth2Error as error:
        return server.handle_error_response(oauth_request, error)
    user = session_user(store, session_token)
    if user is None:
        login = urlencode({"redirect": f"{AUTHORIZE_PATH}?{request.url.query}"})
        return RedirectResponse(f"/api/auth/google?{login}", status_code=302)
    consent = urlencode({"request": server.seal(oauth_request, user.id)})
    return RedirectResponse(f"/authorize?{consent}", status_code=302)


class ConsentResponse(BaseModel):
    """What the consent page shows the user before they decide.

    Attributes:
        client_name: The name the client registered; unverified.
        redirect_host: The host the browser returns to, with its port.
        resource: The surface the client asks for, ``api`` or ``mcp``.
    """

    client_name: str
    redirect_host: str
    resource: Audience

    @classmethod
    def from_grant(cls, grant: AuthorizationCodeGrant) -> ConsentResponse:
        """Describe a validated authorization request.

        Args:
            grant: The grant validating the request.

        Returns:
            The description.
        """
        parts = urlsplit(grant.redirect_uri)
        host = f"{parts.hostname}:{parts.port}" if parts.port else parts.hostname or ""
        return cls(client_name=grant.request.client.row.name, redirect_host=host, resource=grant.audience)


class ConsentDecision(BaseModel):
    """The user's decision on a sealed authorization request.

    Attributes:
        request: The sealed request the consent page was given.
        approve: Whether the user lets the client act for them.
    """

    request: str
    approve: bool


class ConsentRedirect(BaseModel):
    """Where the consent page sends the browser: back to the client.

    Attributes:
        redirect_to: The client's redirect URI with the code, or the error.
    """

    redirect_to: str


def _unseal(server: AuthorizationServerDep, sealed: str, user: Profile) -> OAuthRequest:
    """Open the sealed request the consent page carries, for the user deciding on it.

    Args:
        server: The authorization server.
        sealed: The sealed request.
        user: The user whose session presents it.

    Returns:
        The authorization request.

    Raises:
        HTTPException: 400 when it is altered, expired, not this server's,
            or sealed for another user.
    """
    oauth_request = server.unseal(sealed, user.id)
    if oauth_request is None:
        raise HTTPException(status_code=400, detail=EXPIRED_REQUEST)
    return oauth_request


@router.get("/consent")
def describe_consent(
    sealed: Annotated[str, Query(alias="request")], user: SessionUserDep, server: AuthorizationServerDep
) -> ConsentResponse:
    """Describe the authorization request the user is asked to approve.

    Args:
        sealed: The sealed request, from the consent page's address.
        user: The user deciding, authenticated by the session cookie alone.
        server: The authorization server.

    Returns:
        The client's name, the host it redirects to and the surface it asks for.

    Raises:
        HTTPException: 400 when the request is expired or no longer valid
            (the client was removed, say).
    """
    oauth_request = _unseal(server, sealed, user)
    try:
        grant = server.get_consent_grant(oauth_request, end_user=user)
    except OAuth2Error as error:
        raise HTTPException(status_code=400, detail=error.get_error_description() or error.error)
    return ConsentResponse.from_grant(grant)


@router.post("/consent", response_model=None)
def decide_consent(
    decision: ConsentDecision, user: SessionUserDep, server: AuthorizationServerDep
) -> ConsentRedirect | Response:
    """Record the user's decision: issue the code, or tell the client access was denied.

    The decision is the browser session's, so a bearer token can never
    approve a client.

    Args:
        decision: The sealed request and the decision.
        user: The user deciding, authenticated by the session cookie alone.
        server: The authorization server.

    Returns:
        Where to send the browser: the client's redirect URI with a code, or
        with ``error=access_denied``; a 400 with the OAuth error when the
        request can no longer be answered at the client.
    """
    oauth_request = _unseal(server, decision.request, user)
    try:
        grant = server.get_consent_grant(oauth_request, end_user=user)
    except OAuth2Error as error:
        response = server.handle_error_response(oauth_request, error)
    else:
        response = server.create_authorization_response(
            oauth_request, grant_user=user if decision.approve else None, grant=grant
        )
    location = response.headers.get("location")
    if location is None:
        return response
    return ConsentRedirect(redirect_to=location)


# -- Token & revocation --------------------------------------------------------


@router.post("/token")
def token(oauth_request: FormRequestDep, server: AuthorizationServerDep, store: StoreDep) -> Response:
    """Exchange a code, or rotate a refresh token, for a token pair.

    One transaction: the claimed code or refresh token stays locked until its
    successors are written, so a concurrent replay waits and then revokes them.

    Args:
        oauth_request: The token request.
        server: The authorization server.
        store: The store the transaction is opened on.

    Returns:
        200 with the tokens, or the OAuth error.
    """
    with store.transaction():
        return server.create_token_response(oauth_request)


@router.post("/revoke")
def revoke(oauth_request: FormRequestDep, server: AuthorizationServerDep, store: StoreDep) -> Response:
    """Revoke a token the client holds (RFC 7009).

    Args:
        oauth_request: The revocation request.
        server: The authorization server.
        store: The store the transaction is opened on.

    Returns:
        200, also for a token the server does not know; the OAuth error when
        the client fails to authenticate or the token is another client's.
    """
    with store.transaction():
        return server.create_endpoint_response("revocation", oauth_request)
