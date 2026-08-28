"""Authentication routes — Google OAuth, session management."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from urllib.parse import urlencode
from uuid import UUID
from zoneinfo import ZoneInfo

import httpx
from fastapi import APIRouter, Cookie, Depends, HTTPException, Response
from fastapi.responses import RedirectResponse
from interloper_db import Organisation, Profile, Store
from pydantic import BaseModel, Field

from interloper_api.dependencies import get_auth_config, get_current_user, get_features, get_store

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
    return store.has_pending_invitation(email)


# -- Login & session -----------------------------------------------------------


class OrganisationResponse(BaseModel):
    """Organisation summary for auth responses."""

    id: UUID
    name: str
    created_at: datetime | None = None


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


@router.get("/google")
def google_login(
    redirect: str | None = None,
    auth_config: Any = Depends(get_auth_config),
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
    state: str | None = None,
    store: Store = Depends(get_store),
    auth_config: Any = Depends(get_auth_config),
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

    token_resp = httpx.post(
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

    userinfo_resp = httpx.get(
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
    if not store.get_profile_by_google_id(google_id) and not _signup_allowed(email, auth_config, store):
        return RedirectResponse(url="/login?error=signup_not_allowed", status_code=302)

    profile = store.upsert_profile(
        google_id=google_id,
        email=email,
        name=name,
        avatar_url=avatar_url,
    )

    # Bootstrap super-admins from settings. Promote-only: removing an email from
    # the list never demotes an existing super-admin.
    if not profile.is_super_admin and email.lower() in auth_config.super_admin_emails:
        profile = store.set_super_admin(profile.id)

    # Create session (no org context — frontend resolves org after login)
    token = store.create_session(user_id=profile.id)

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


@router.post("/logout")
def logout(
    response: Response,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Delete all sessions for the current user and clear the cookie.

    Every session is dropped, not just this one: a logout is expected to end
    the user's other browsers too.

    Args:
        response: The outgoing response, used to clear the session cookie.
        user: The authenticated caller.
        store: The database store.

    Returns:
        A status acknowledgement.
    """
    store.delete_user_sessions(user.id)
    response.delete_cookie("session_token", path="/")
    return {"status": "ok"}


@router.get("/me")
def get_me(
    store: Store = Depends(get_store),
    session_token: str | None = Cookie(default=None),
) -> AuthUserResponse:
    """Return the current user and their active organisation (if any).

    Args:
        store: The database store.
        session_token: The session cookie, absent when the caller is not
            logged in.

    Returns:
        The caller's profile, their role in the active organisation (``viewer``
        when there is none), and the enabled feature flags.

    Raises:
        HTTPException: 401 when the session cookie is missing, unknown, or
            expired.
    """
    if not session_token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    result = store.resolve_session(session_token)
    if not result:
        raise HTTPException(status_code=401, detail="Invalid or expired session")

    profile, session_row = result
    org: Organisation | None = None
    role = "viewer"

    if session_row.organisation_id:
        org = store.get_organisation(session_row.organisation_id)

    if org and profile.id:
        user_role = store.get_user_role(profile.id, org.id)
        if user_role:
            role = user_role

    return AuthUserResponse(
        id=profile.id,
        email=profile.email,
        name=profile.name,
        avatar_url=profile.avatar_url,
        timezone=profile.timezone,
        role=role,
        is_super_admin=profile.is_super_admin,
        organisation=OrganisationResponse.model_validate(org, from_attributes=True) if org else None,
        last_organisation_id=profile.last_organisation_id,
        features=get_features(),
    )


# -- Profile -------------------------------------------------------------------


class UpdateMeRequest(BaseModel):
    """User-editable profile fields; omitted fields stay untouched.

    Email is absent by design: it is Google-managed and refreshed at every
    login, so an edit here would be silently reverted.
    """

    name: str | None = Field(default=None, min_length=1, max_length=200)
    timezone: str | None = None


class ProfileResponse(BaseModel):
    """The caller's own profile, as returned by profile updates."""

    id: UUID
    email: str
    name: str | None = None
    avatar_url: str | None = None
    timezone: str | None = None


@router.patch("/me")
def update_me(
    body: UpdateMeRequest,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
) -> ProfileResponse:
    """Update the current user's profile (display name, timezone).

    Args:
        body: The fields to change; omitted fields stay untouched.
        user: The authenticated caller.
        store: The database store.

    Returns:
        The profile as it stands after the update.

    Raises:
        HTTPException: 422 when the timezone is not a known IANA zone name.
    """
    if body.timezone is not None:
        try:
            ZoneInfo(body.timezone)
        except (KeyError, ValueError):
            raise HTTPException(status_code=422, detail=f"Unknown timezone {body.timezone!r}")

    profile = store.update_profile(user.id, name=body.name, timezone=body.timezone)
    return ProfileResponse.model_validate(profile, from_attributes=True)


# -- Organisation context ------------------------------------------------------


class SwitchOrgRequest(BaseModel):
    """Request body for switching the active organisation."""

    organisation_id: UUID


@router.post("/switch-org")
def switch_org(
    body: SwitchOrgRequest,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
    session_token: str | None = Cookie(default=None),
) -> dict[str, str]:
    """Switch the session's active organisation. User must be a member.

    Args:
        body: The organisation to make active.
        user: The authenticated caller.
        store: The database store.
        session_token: The session cookie; the active organisation is stored on
            the session, so without it there is nothing to record.

    Returns:
        A status acknowledgement.

    Raises:
        HTTPException: 403 when the caller is not a member of the organisation.
    """
    role = store.get_user_role(user.id, body.organisation_id)
    if not role:
        raise HTTPException(status_code=403, detail="Not a member of this organisation")

    if session_token:
        store.set_session_org(session_token, body.organisation_id, user_id=user.id)
    return {"status": "ok"}


class AcceptInviteRequest(BaseModel):
    """Request body for accepting an invitation."""

    token: str


@router.post("/accept-invite")
def accept_invite(
    body: AcceptInviteRequest,
    user: Profile = Depends(get_current_user),
    store: Store = Depends(get_store),
    session_token: str | None = Cookie(default=None),
) -> dict[str, str]:
    """Accept an organisation invitation using its token.

    Args:
        body: The invitation token to redeem.
        user: The authenticated caller, who becomes a member on success.
        store: The database store.
        session_token: The session cookie; when present, the joined organisation
            also becomes the session's active one.

    Returns:
        A status acknowledgement.

    Raises:
        HTTPException: 400 when the invitation is unknown, already redeemed, or
            expired.
    """
    org = store.accept_invitation(body.token, user.id)
    if not org:
        raise HTTPException(status_code=400, detail="Invalid or expired invitation")

    if session_token:
        store.set_session_org(session_token, org.id, user_id=user.id)

    return {"status": "ok"}
