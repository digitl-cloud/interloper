"""Authentication routes — Google OAuth, session management."""

from __future__ import annotations

from contextlib import suppress
from typing import Annotated, Any
from urllib.parse import urlencode
from uuid import UUID
from zoneinfo import ZoneInfo

import httpx2
from fastapi import APIRouter, Cookie, HTTPException, Response
from fastapi.responses import RedirectResponse
from interloper.errors import NotFoundError
from interloper_db import Organisation, Profile, Store
from interloper_db.models import AuthSession
from pydantic import BaseModel, Field

from interloper_api.dependencies import (
    AuthConfigDep,
    CurrentUserDep,
    SessionContextDep,
    StoreDep,
    get_features,
)
from interloper_api.routes.organisations import OrganisationResponse

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"

router = APIRouter(prefix="/auth", tags=["auth"])


# -- Helpers -------------------------------------------------------------------


def _signup_allowed(email: str, auth_config: Any, store: Store) -> bool:
    """Decide whether a first-time login may create a profile.

    An empty ``allowed_domains`` keeps signup open (the default).
    Otherwise the email must be on an allowed domain, be a configured
    super-admin, or hold a pending invitation.

    Args:
        email: The Google-verified address of the account signing in.
        auth_config: The resolved auth configuration.
        store: The database store.

    Returns:
        True when a profile may be created for this address.
    """
    allowed_domains = auth_config.allowed_domains
    if not allowed_domains:
        return True

    email = email.lower()
    if email in auth_config.super_admin_emails:
        return True
    if email.rsplit("@", 1)[-1] in allowed_domains:
        return True
    return store.invitations.has_pending(email)


# -- Login & session -----------------------------------------------------------


class AuthUserResponse(BaseModel):
    """Current user with active organisation context."""

    id: UUID
    email: str
    name: str | None = None
    avatar_url: str | None = None
    timezone: str | None = None
    role: str
    is_super_admin: bool = False
    organisation: OrganisationResponse | None = None
    last_organisation_id: UUID | None = None
    features: dict[str, bool] = {}

    @classmethod
    def from_session(cls, profile: Profile, session_row: AuthSession, store: Store) -> AuthUserResponse:
        """Describe the caller in the organisation their session is working in.

        A session outliving its organisation still resolves: the caller is
        authenticated, just no longer scoped anywhere, and reads as a viewer.

        Args:
            profile: The caller's profile.
            session_row: The caller's login session.
            store: The Store instance the organisation and role are read through.

        Returns:
            The response model, carrying the enabled feature flags.
        """
        organisation: Organisation | None = None
        if session_row.organisation_id:
            with suppress(NotFoundError):
                organisation = store.organisations.get(session_row.organisation_id)
        role = (store.members.role(organisation.id, profile.id) if organisation else None) or "viewer"
        return cls(
            id=profile.id,
            email=profile.email,
            name=profile.name,
            avatar_url=profile.avatar_url,
            timezone=profile.timezone,
            role=role,
            is_super_admin=profile.is_super_admin,
            organisation=OrganisationResponse.from_organisation(organisation) if organisation else None,
            last_organisation_id=profile.last_organisation_id,
            features=get_features(),
        )


@router.get("/google")
def google_login(
    auth_config: AuthConfigDep,
    redirect: str | None = None,
) -> RedirectResponse:
    """Redirect user to Google's OAuth consent screen.

    Args:
        redirect: App-relative destination to return to once the login
            completes; carried through Google as the ``state`` parameter and
            defaulting to the app root.
        auth_config: The resolved auth configuration.

    Returns:
        A redirect to Google's consent screen.

    Raises:
        HTTPException: 500 when no Google client id is configured.
    """
    client_id = auth_config.google_client_id
    redirect_uri = auth_config.google_redirect_uri

    if not client_id:
        raise HTTPException(status_code=500, detail="Google OAuth not configured")

    params = urlencode(
        {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": "openid email profile",
            "access_type": "offline",
            "prompt": "consent",
            "state": redirect or "/",
        }
    )
    return RedirectResponse(url=f"{GOOGLE_AUTH_URL}?{params}")


@router.get("/google/callback")
def google_callback(
    code: str,
    store: StoreDep,
    auth_config: AuthConfigDep,
    state: str | None = None,
) -> RedirectResponse:
    """Exchange Google authorization code for tokens, upsert profile, create session.

    Args:
        code: The single-use authorization code handed back by Google.
        state: The destination recorded at the start of the flow; honoured only
            when it is app-relative, otherwise the app root is used.
        store: The database store.
        auth_config: The resolved auth configuration.

    Returns:
        A redirect to the post-login destination carrying the session cookie, or
        a redirect back to the login page when signup is not allowed.

    Raises:
        HTTPException: 500 when Google OAuth is not configured, 401 when the code
            exchange or the user-info lookup fails or comes back incomplete.
    """
    client_id = auth_config.google_client_id
    client_secret = auth_config.google_client_secret
    redirect_uri = auth_config.google_redirect_uri
    cookie_secure: bool = auth_config.cookie_secure
    session_expiry_days: int = auth_config.session_expiry_days

    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="Google OAuth not configured")

    token_resp = httpx2.post(
        GOOGLE_TOKEN_URL,
        data={
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        },
    )
    if token_resp.status_code != 200:
        raise HTTPException(status_code=401, detail="Failed to exchange authorization code")

    tokens = token_resp.json()
    access_token = tokens.get("access_token")
    if not access_token:
        raise HTTPException(status_code=401, detail="No access token in response")

    userinfo_resp = httpx2.get(
        GOOGLE_USERINFO_URL,
        headers={"Authorization": f"Bearer {access_token}"},
    )
    if userinfo_resp.status_code != 200:
        raise HTTPException(status_code=401, detail="Failed to fetch user info")

    userinfo = userinfo_resp.json()
    google_id = userinfo.get("id")
    email = userinfo.get("email")
    name = userinfo.get("name")
    avatar_url = userinfo.get("picture")

    if not google_id or not email:
        raise HTTPException(status_code=401, detail="Incomplete user info from Google")

    # Gate signup only: existing profiles always sign in, a first login must
    # pass the allowlist before a profile is created.
    if not store.profiles.get_by_google_id(google_id) and not _signup_allowed(email, auth_config, store):
        return RedirectResponse(url="/login?error=signup_not_allowed", status_code=302)

    profile = store.profiles.upsert(
        google_id=google_id,
        email=email,
        name=name,
        avatar_url=avatar_url,
    )

    # Bootstrap super-admins from settings. Promote-only: removing an email from
    # the list never demotes an existing super-admin.
    if not profile.is_super_admin and email.lower() in auth_config.super_admin_emails:
        profile = store.profiles.set_super_admin(profile.id, value=True)

    # Create session (no org context — frontend resolves org after login)
    token = store.sessions.create(profile.id)

    redirect_url = state if state and state.startswith("/") else "/"
    response = RedirectResponse(url=redirect_url, status_code=302)
    response.set_cookie(
        key="session_token",
        value=token,
        httponly=True,
        samesite="lax",
        secure=cookie_secure,
        max_age=session_expiry_days * 86400,
        path="/",
    )
    return response


@router.post("/logout", status_code=204)
def logout(user: CurrentUserDep, store: StoreDep) -> Response:
    """End every session of the current user and clear the cookie.

    Every session is dropped, not just this one: a logout is expected to end
    the user's other browsers too.

    Args:
        user: The authenticated caller.
        store: The database store.

    Returns:
        An empty 204 response clearing the session cookie.
    """
    store.sessions.delete_all(user.id)
    response = Response(status_code=204)
    response.delete_cookie("session_token", path="/")
    return response


@router.get("/me")
def get_me(context: SessionContextDep, store: StoreDep) -> AuthUserResponse:
    """Return the current user and their active organisation (if any).

    Args:
        context: The caller and their session.
        store: The database store.

    Returns:
        The caller's profile, their role in the active organisation (``viewer``
        when there is none), and the enabled feature flags.
    """
    profile, session_row = context
    return AuthUserResponse.from_session(profile, session_row, store)


# -- Profile -------------------------------------------------------------------


class UpdateMeRequest(BaseModel):
    """User-editable profile fields; omitted fields stay untouched.

    Email is absent by design: it is Google-managed and refreshed at every
    login, so an edit here would be silently reverted.
    """

    name: str | None = Field(default=None, min_length=1, max_length=200)
    timezone: str | None = None


@router.patch("/me")
def update_me(body: UpdateMeRequest, context: SessionContextDep, store: StoreDep) -> AuthUserResponse:
    """Update the current user's profile (display name, timezone).

    Args:
        body: The fields to change; omitted fields stay untouched.
        context: The caller and their session.
        store: The database store.

    Returns:
        The caller as :func:`get_me` describes them, after the update.

    Raises:
        HTTPException: 422 when the timezone is not a known IANA zone name.
    """
    if body.timezone is not None:
        try:
            ZoneInfo(body.timezone)
        except (KeyError, ValueError):
            raise HTTPException(status_code=422, detail=f"Unknown timezone {body.timezone!r}")
    profile, session_row = context
    profile = store.profiles.update(profile.id, name=body.name, timezone=body.timezone)
    return AuthUserResponse.from_session(profile, session_row, store)


# -- Organisation context ------------------------------------------------------


class SwitchOrgRequest(BaseModel):
    """Request body for switching the active organisation."""

    organisation_id: UUID


@router.post("/switch-org", status_code=204)
def switch_org(
    body: SwitchOrgRequest,
    user: CurrentUserDep,
    store: StoreDep,
    session_token: Annotated[str | None, Cookie()] = None,
) -> Response:
    """Switch the session's active organisation. User must be a member.

    Args:
        body: The organisation to make active.
        user: The authenticated caller.
        store: The database store.
        session_token: The session cookie; the active organisation is stored on
            the session, so without it there is nothing to record.

    Returns:
        An empty 204 response.

    Raises:
        HTTPException: 403 when the caller is not a member of the organisation.
    """
    if not store.members.role(body.organisation_id, user.id):
        raise HTTPException(status_code=403, detail="Not a member of this organisation")
    if session_token:
        store.sessions.switch_org(session_token, body.organisation_id, user.id)
    return Response(status_code=204)


class AcceptInviteRequest(BaseModel):
    """Request body for accepting an invitation."""

    token: str


@router.post("/accept-invite")
def accept_invite(
    body: AcceptInviteRequest,
    user: CurrentUserDep,
    store: StoreDep,
    session_token: Annotated[str | None, Cookie()] = None,
) -> OrganisationResponse:
    """Accept an organisation invitation using its token.

    Args:
        body: The invitation token to redeem.
        user: The authenticated caller, who becomes a member on success.
        store: The database store.
        session_token: The session cookie; when present, the joined organisation
            also becomes the session's active one.

    Returns:
        The organisation joined.

    Raises:
        HTTPException: 400 when the invitation is unknown, already redeemed, or
            expired.
    """
    organisation = store.invitations.accept(body.token, user.id)
    if not organisation:
        raise HTTPException(status_code=400, detail="Invalid or expired invitation")
    if session_token:
        store.sessions.switch_org(session_token, organisation.id, user.id)
    return OrganisationResponse.from_organisation(organisation)
