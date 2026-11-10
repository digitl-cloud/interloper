"""Authentication routes: Google login, the login session, and the caller's own profile."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import RedirectResponse
from interloper_db import OrganisationQuery, Profile, Store
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from interloper_api.dependencies import (
    CurrentUserDep,
    GoogleLoginDep,
    SessionUserDep,
    SettingsDep,
    StateDep,
    StoreDep,
    get_org_scope,
)
from interloper_api.login import GoogleAccount, LoginError
from interloper_api.routes.organisations import OrganisationResponse

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
    key: str
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
                    MembershipResponse(id=organisation.id, name=organisation.name, key=organisation.key, role=role)
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
async def google_login(request: Request, login: GoogleLoginDep, redirect: str | None = None) -> RedirectResponse:
    """Send the browser to Google's account picker.

    Args:
        request: The login request, whose session keeps the login's state.
        login: The Google login.
        redirect: App-relative destination to return to once the login
            completes, defaulting to the app root.

    Returns:
        A redirect to Google, or to the login page when Google cannot be
        reached.
    """
    try:
        return await login.redirect(request, redirect or "/")
    except LoginError as error:
        return RedirectResponse(url=f"/login?error={error.reason}", status_code=302)


@router.get("/google/callback")
async def google_callback(
    request: Request, login: GoogleLoginDep, store: StoreDep, settings: SettingsDep
) -> RedirectResponse:
    """Complete the Google login and open a login session.

    Args:
        request: Google's callback, carrying the code and ``state``.
        login: The Google login.
        store: The database store.
        settings: The settings, whose ``auth`` holds the signup and session
            policy.

    Returns:
        A redirect to the login's destination carrying the session cookie, or
        to the login page when the login did not complete or signup is not
        allowed.
    """
    try:
        account, destination = await login.complete(request)
    except LoginError as error:
        return RedirectResponse(url=f"/login?error={error.reason}", status_code=302)
    return await run_in_threadpool(_sign_in, account, destination, store, settings.auth)


def _sign_in(account: GoogleAccount, destination: str, store: Store, auth_config: Any) -> RedirectResponse:
    """Open a login session for the account Google proved.

    Args:
        account: The verified Google account.
        destination: The destination the login asked for; honoured only when
            it is app-relative, otherwise the app root is used.
        store: The database store.
        auth_config: The resolved auth configuration.

    Returns:
        A redirect to the destination carrying the session cookie, or to the
        login page when signup is not allowed.
    """
    # Gate signup only: existing profiles always sign in, a first login must
    # pass the allowlist before a profile is created.
    if not store.profiles.get_by_google_id(account.google_id) and not _signup_allowed(
        account.email, auth_config, store
    ):
        return RedirectResponse(url="/login?error=signup_not_allowed", status_code=302)

    profile = store.profiles.upsert(
        google_id=account.google_id,
        email=account.email,
        name=account.name,
        avatar_url=account.avatar_url,
    )

    # Bootstrap super-admins from settings. Promote-only: removing an email from
    # the list never demotes an existing super-admin.
    if not profile.is_super_admin and account.email.lower() in auth_config.super_admin_emails:
        profile = store.profiles.set_super_admin(profile.id, value=True)

    response = RedirectResponse(url=_app_relative(destination), status_code=302)
    response.set_cookie(
        key="session_token",
        value=store.tokens.create_session(profile.id),
        httponly=True,
        samesite="lax",
        secure=auth_config.cookie_secure,
        max_age=auth_config.session_expiry_days * 86400,
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
