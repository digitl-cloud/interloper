"""Authentication routes: Google login, the login session, and the caller's own profile."""

from __future__ import annotations

import secrets
from typing import Annotated, Any
from urllib.parse import urlencode, urlsplit
from uuid import UUID
from zoneinfo import ZoneInfo

import httpx2
from fastapi import APIRouter, Cookie, HTTPException, Response
from fastapi.responses import RedirectResponse
from interloper_db import OrganisationQuery, Profile, Store
from pydantic import BaseModel, Field

from interloper_api.dependencies import (
    CurrentUserDep,
    SessionUserDep,
    SettingsDep,
    StateDep,
    StoreDep,
    get_org_scope,
)
from interloper_api.routes.organisations import OrganisationResponse

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"
LOGIN_STATE_COOKIE = "login_state"
LOGIN_STATE_MAX_AGE = 600

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


def _app_relative(path: str) -> str:
    r"""Return ``path`` when it stays on this app, otherwise the app root.

    Browsers read ``//host`` and ``/\host`` as protocol-relative URLs, so a
    single leading slash is not enough.

    Args:
        path: The requested post-login destination.

    Returns:
        ``path`` when it is an app-relative path, otherwise ``"/"``.
    """
    parts = urlsplit(path)
    if not path.startswith("/") or path[1:2] in ("/", "\\") or parts.scheme or parts.netloc:
        return "/"
    return path


# -- Login & session -----------------------------------------------------------


class MembershipResponse(BaseModel):
    """An organisation the caller belongs to, with the role they act with in it."""

    id: UUID
    name: str
    slug: str
    role: str


class AuthUserResponse(BaseModel):
    """The caller, the organisations they belong to, and the enabled features."""

    id: UUID
    email: str
    name: str | None = None
    avatar_url: str | None = None
    timezone: str | None = None
    is_super_admin: bool = False
    memberships: list[MembershipResponse] = []
    last_organisation_id: UUID | None = None
    features: dict[str, bool] = {}

    @classmethod
    def from_profile(cls, profile: Profile, store: Store, features: dict[str, bool]) -> AuthUserResponse:
        """Describe the caller with their memberships.

        A super-admin's memberships carry ``admin``, the role every
        organisation route grants them; organisations they do not belong to
        are not listed.

        Args:
            profile: The caller's profile.
            store: The Store instance the memberships and roles are read through.
            features: The optional features this deployment enables.

        Returns:
            The response model, carrying the enabled feature flags.
        """
        organisations = store.organisations.list(OrganisationQuery(user_id=profile.id, limit=None)).items
        memberships: list[MembershipResponse] = []
        for organisation in organisations:
            role = store.members.role_of(organisation.id, profile)
            if role is not None:
                memberships.append(
                    MembershipResponse(id=organisation.id, name=organisation.name, slug=organisation.slug, role=role)
                )
        return cls(
            id=profile.id,
            email=profile.email,
            name=profile.name,
            avatar_url=profile.avatar_url,
            timezone=profile.timezone,
            is_super_admin=profile.is_super_admin,
            memberships=memberships,
            last_organisation_id=profile.last_organisation_id,
            features=features,
        )


@router.get("/google")
def google_login(
    settings: SettingsDep,
    redirect: str | None = None,
) -> RedirectResponse:
    """Redirect user to Google's OAuth consent screen.

    Args:
        settings: The settings, whose ``auth`` names the Google client.
        redirect: App-relative destination to return to once the login
            completes; carried through Google in the ``state`` parameter after
            a random nonce, defaulting to the app root.

    Returns:
        A redirect to Google's consent screen, setting the nonce in a
        short-lived cookie the callback checks ``state`` against.

    Raises:
        HTTPException: 500 when no Google client id is configured.
    """
    auth_config = settings.auth
    client_id = auth_config.google_client_id
    redirect_uri = auth_config.google_redirect_uri

    if not client_id:
        raise HTTPException(status_code=500, detail="Google OAuth not configured")

    nonce = secrets.token_urlsafe(32)
    params = urlencode(
        {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": "openid email profile",
            "access_type": "offline",
            "prompt": "consent",
            "state": f"{nonce}:{redirect or '/'}",
        }
    )
    response = RedirectResponse(url=f"{GOOGLE_AUTH_URL}?{params}")
    response.set_cookie(
        key=LOGIN_STATE_COOKIE,
        value=nonce,
        httponly=True,
        samesite="lax",
        secure=auth_config.cookie_secure,
        max_age=LOGIN_STATE_MAX_AGE,
        path="/",
    )
    return response


@router.get("/google/callback")
def google_callback(
    code: str,
    store: StoreDep,
    settings: SettingsDep,
    state: str | None = None,
    login_state: Annotated[str | None, Cookie()] = None,
) -> RedirectResponse:
    """Exchange Google's authorization code, upsert the profile, and open a login session.

    Args:
        code: The single-use authorization code handed back by Google.
        state: The nonce and destination recorded at the start of the flow; the
            destination is honoured only when it is app-relative, otherwise the
            app root is used.
        store: The database store.
        settings: The settings, whose ``auth`` names the Google client.
        login_state: The nonce cookie :func:`google_login` set; a callback whose
            ``state`` does not carry it was not started by this browser.

    Returns:
        A redirect to the post-login destination carrying the session cookie, or
        a redirect back to the login page when the nonce does not match or
        signup is not allowed.

    Raises:
        HTTPException: 500 when Google OAuth is not configured, 401 when the code
            exchange or the user-info lookup fails or comes back incomplete.
    """
    auth_config = settings.auth
    client_id = auth_config.google_client_id
    client_secret = auth_config.google_client_secret
    redirect_uri = auth_config.google_redirect_uri
    cookie_secure: bool = auth_config.cookie_secure
    session_expiry_days: int = auth_config.session_expiry_days

    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="Google OAuth not configured")

    nonce, _, redirect = (state or "").partition(":")
    if not login_state or not secrets.compare_digest(nonce, login_state):
        response = RedirectResponse(url="/login?error=login_expired", status_code=302)
        response.delete_cookie(LOGIN_STATE_COOKIE, path="/")
        return response

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

    token = store.tokens.create_session(profile.id)

    response = RedirectResponse(url=_app_relative(redirect), status_code=302)
    response.delete_cookie(LOGIN_STATE_COOKIE, path="/")
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
def logout(user: SessionUserDep, store: StoreDep) -> Response:
    """End every login session of the current user and clear the cookie.

    Every session is revoked, not just this one: a logout is expected to end
    the user's other browsers too. Personal access tokens are left alone.

    Args:
        user: The caller, authenticated by the session cookie alone.
        store: The database store.

    Returns:
        An empty 204 response clearing the session cookie.
    """
    store.tokens.revoke_sessions(user.id)
    response = Response(status_code=204)
    response.delete_cookie("session_token", path="/")
    return response


@router.get("/me")
def get_me(user: CurrentUserDep, store: StoreDep, state: StateDep) -> AuthUserResponse:
    """Return the current user and the organisations they belong to.

    Args:
        user: The authenticated caller.
        store: The database store.
        state: The application's state, read for the enabled features.

    Returns:
        The caller's profile, their memberships with the role in each, the
        organisation they last used, and the enabled feature flags.
    """
    return AuthUserResponse.from_profile(user, store, state.features)


# -- Profile -------------------------------------------------------------------


class UpdateMeRequest(BaseModel):
    """User-editable profile fields; omitted fields stay untouched.

    Email is absent by design: it is Google-managed and refreshed at every
    login, so an edit here would be silently reverted.
    """

    name: str | None = Field(default=None, min_length=1, max_length=200)
    timezone: str | None = None


@router.patch("/me")
def update_me(body: UpdateMeRequest, user: CurrentUserDep, store: StoreDep, state: StateDep) -> AuthUserResponse:
    """Update the current user's profile (display name, timezone).

    Args:
        body: The fields to change; omitted fields stay untouched.
        user: The authenticated caller.
        store: The database store.
        state: The application's state, read for the enabled features.

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
    profile = store.profiles.update(user.id, name=body.name, timezone=body.timezone)
    return AuthUserResponse.from_profile(profile, store, state.features)


class LastOrganisationRequest(BaseModel):
    """Request body naming the organisation the caller is working in."""

    org_id: UUID


@router.put("/me/last-organisation", status_code=204)
def set_last_organisation(body: LastOrganisationRequest, user: SessionUserDep, store: StoreDep) -> Response:
    """Remember the organisation the caller last worked in, where the app lands them next time.

    Args:
        body: The organisation, one the caller can act in.
        user: The caller, authenticated by the session cookie alone.
        store: The database store.

    Returns:
        An empty 204 response.
    """
    get_org_scope(body.org_id, user, store)
    store.profiles.update(user.id, last_organisation_id=body.org_id)
    return Response(status_code=204)


# -- Invitations ---------------------------------------------------------------


class AcceptInviteRequest(BaseModel):
    """Request body for accepting an invitation."""

    token: str


@router.post("/accept-invite")
def accept_invite(body: AcceptInviteRequest, user: SessionUserDep, store: StoreDep) -> OrganisationResponse:
    """Accept an organisation invitation using its token.

    Args:
        body: The invitation token to redeem.
        user: The caller, authenticated by the session cookie alone, who becomes a
            member on success.
        store: The database store.

    Returns:
        The organisation joined.

    Raises:
        HTTPException: 400 when the invitation is unknown, already redeemed, or
            expired.
    """
    organisation = store.invitations.accept(body.token, user.id)
    if not organisation:
        raise HTTPException(status_code=400, detail="Invalid or expired invitation")
    return OrganisationResponse.from_organisation(organisation)
