"""Authentication routes: signing in through a provider, the login session, and the caller's own profile."""

from __future__ import annotations

import logging
from collections.abc import MutableMapping
from datetime import datetime
from typing import Annotated, Any
from urllib.parse import urlsplit
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Cookie, Depends, HTTPException, Request, Response
from fastapi.responses import RedirectResponse
from interloper_db import NotFoundError, OrganisationQuery, Profile, ProfileQuery, Store
from interloper_db.models import Identity
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from interloper_api.auth import Account, LinkProposal, Login, LoginError, LoginProvider
from interloper_api.dependencies import (
    CurrentUserDep,
    FeaturesDep,
    LoginDep,
    LoginProviderDep,
    OfferedLoginDep,
    SessionUserDep,
    SettingsDep,
    StateDep,
    StoreDep,
    get_org_scope,
)
from interloper_api.notifications import LinkConfirmationEmail
from interloper_api.routes.organisations import OrganisationResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])

_INVALID_LINK = "This link is invalid or has expired"


# -- Helpers -------------------------------------------------------------------


def _signup_allowed(email: str, auth_config: Any, store: Store) -> bool:
    """Decide whether a first-time login may create a profile.

    An empty ``allowed_domains`` keeps signup open (the default).
    Otherwise the email must be on an allowed domain, be a configured
    super-admin, or hold a pending invitation.

    Args:
        email: The address the identity provider verified for the account
            signing in.
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


class LoginProviderResponse(BaseModel):
    """A sign-in provider the deployment offers.

    Attributes:
        name: The provider's name in the login routes.
        label: The provider's name as the login page shows it.
    """

    name: str
    label: str

    @classmethod
    def from_provider(cls, provider: LoginProvider) -> LoginProviderResponse:
        """Describe a provider the deployment offers.

        Args:
            provider: The provider.

        Returns:
            The response model.
        """
        return cls(name=provider.name, label=provider.label)


class AuthUserResponse(BaseModel):
    """The caller, how they sign in, the organisations they belong to, and the enabled features."""

    id: UUID
    email: str
    name: str | None = None
    avatar_url: str | None = None
    timezone: str | None = None
    is_super_admin: bool = False
    login_providers: list[LoginProviderResponse] = []
    memberships: list[MembershipResponse] = []
    last_organisation_id: UUID | None = None
    features: dict[str, bool] = {}

    @classmethod
    def from_profile(
        cls, profile: Profile, store: Store, login: Login | None, features: dict[str, bool]
    ) -> AuthUserResponse:
        """Describe the caller with their sign-in providers and memberships.

        A super-admin's memberships carry ``admin``, the role every
        organisation route grants them; organisations they do not belong to
        are not listed.

        Args:
            profile: The caller's profile.
            store: The Store instance the identities, memberships and roles
                are read through.
            login: The sign-in providers the deployment offers; ``None``
                reports none.
            features: The optional features this deployment enables.

        Returns:
            The response model, carrying the enabled feature flags.
        """
        providers = login.providers_of(store.profiles.issuers(profile.id)) if login is not None else []
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
            login_providers=[LoginProviderResponse.from_provider(provider) for provider in providers],
            memberships=memberships,
            last_organisation_id=profile.last_organisation_id,
            features=features,
        )


@router.get("/providers")
def list_login_providers(state: StateDep) -> list[LoginProviderResponse]:
    """List the sign-in providers this deployment offers, for the login page.

    Answers without a credential: the login page asks before anyone signs in.

    Args:
        state: The application's state, holding the providers.

    Returns:
        The providers, in the order the login page shows them; empty when the
        deployment offers none.
    """
    if state.login is None:
        return []
    return [LoginProviderResponse.from_provider(provider) for provider in state.login.providers.values()]


@router.get("/{provider}/login")
async def login(request: Request, provider: LoginProviderDep, redirect: str | None = None) -> RedirectResponse:
    """Send the browser to the provider's account picker.

    Args:
        request: The login request, whose session keeps the login's state.
        provider: The provider the path names.
        redirect: App-relative destination to return to once the login
            completes, defaulting to the app root.

    Returns:
        A redirect to the provider, or to the login page when the provider
        cannot be reached.
    """
    try:
        return await provider.redirect(request, redirect or "/")
    except LoginError as error:
        return RedirectResponse(url=f"/login?error={error.reason}", status_code=302)


@router.get("/{provider}/callback")
async def login_callback(
    request: Request, provider: LoginProviderDep, store: StoreDep, settings: SettingsDep
) -> RedirectResponse:
    """Complete the provider's login and open a login session.

    Args:
        request: The provider's callback, carrying the code and ``state``.
        provider: The provider the path names.
        store: The database store.
        settings: The settings, whose ``auth`` holds the signup and session
            policy, ``smtp`` the mail server and ``server`` the public URL.

    Returns:
        A redirect to the login's destination carrying the session cookie, to
        the link page when its address belongs to a profile that already
        signs in, or to the login page when the login did not complete or is
        refused.
    """
    try:
        account, destination = await provider.complete(request)
    except LoginError as error:
        return RedirectResponse(url=f"/login?error={error.reason}", status_code=302)
    return await run_in_threadpool(_sign_in, request.session, account, provider, destination, store, settings)


def _sign_in(
    login_session: MutableMapping[str, Any],
    account: Account,
    provider: LoginProvider,
    destination: str,
    store: Store,
    settings: Any,
) -> RedirectResponse:
    """Open a login session for the account the provider proved.

    The address is the trust anchor, as it already is for signup,
    invitations and super-admin promotion. A first login whose address a
    profile holds joins that profile: at once when nothing signs in to it yet;
    otherwise it is proposed as a link, since a provider's word about an
    address can be stale, and joins once the person proves the profile is
    theirs, by signing in the way it does or by the link emailed to it.

    Args:
        login_session: The login's session, where a proposed link waits.
        account: The verified account.
        provider: The provider that proved it.
        destination: The destination the login asked for; honoured only when
            it is app-relative, otherwise the app root is used.
        store: The database store.
        settings: The settings, whose ``auth`` holds the signup and session
            policy.

    Returns:
        A redirect to the destination carrying the session cookie, to the link
        page when the account is proposed as a link, or to the login page when
        several profiles hold the address or signup is not allowed.
    """
    auth_config = settings.auth
    if not store.profiles.get_by_identity(account.issuer, account.subject):
        holders = store.profiles.list(ProfileQuery(email=account.email, limit=None)).items
        if len(holders) > 1:
            profile_ids = ", ".join(str(holder.id) for holder in holders)
            logger.warning("Login refused: profiles %s all hold its address", profile_ids)
            return RedirectResponse(url="/login?error=address_shared", status_code=302)
        if holders and store.profiles.issuers(holders[0].id):
            LinkProposal(
                issuer=account.issuer,
                subject=account.subject,
                provider=provider.name,
                profile_id=holders[0].id,
                email=holders[0].email,
                destination=_app_relative(destination),
            ).save(login_session)
            return RedirectResponse(url="/auth/link", status_code=302)
        if holders:
            store.profiles.link(holders[0].id, issuer=account.issuer, subject=account.subject)
        elif not _signup_allowed(account.email, auth_config, store):
            return RedirectResponse(url="/login?error=signup_not_allowed", status_code=302)

    profile = store.profiles.upsert(
        issuer=account.issuer,
        subject=account.subject,
        email=account.email,
        name=account.name,
        avatar_url=account.avatar_url,
    )

    # Signing in the way the profile does proves it is the person's: the link
    # their earlier login proposed joins it.
    proposal = LinkProposal.take(login_session)
    if (
        proposal is not None
        and proposal.profile_id == profile.id
        and store.profiles.get_by_identity(proposal.issuer, proposal.subject) is None
    ):
        store.profiles.link(profile.id, issuer=proposal.issuer, subject=proposal.subject)

    # Bootstrap super-admins from settings. Promote-only: removing an email from
    # the list never demotes an existing super-admin.
    if not profile.is_super_admin and profile.email.lower() in auth_config.super_admin_emails:
        profile = store.profiles.set_super_admin(profile.id, value=True)

    response = RedirectResponse(url=_app_relative(destination), status_code=302)
    auth_config.set_session_cookie(response, store.tokens.create_session(profile.id))
    return response


# -- Link proposal -------------------------------------------------------------


class LinkProposalResponse(BaseModel):
    """The link a login proposed, and the ways the person can prove the profile is theirs.

    Attributes:
        email: The address of the profile the new account would join.
        provider: The provider of the new account; ``None`` when the
            deployment no longer offers it.
        methods: The offered providers the profile signs in through; signing
            in through one links the new account.
        email_available: Whether the link can be emailed to the address
            instead, which needs a mail server.
        destination: Where the login that proposed it asked to land.
    """

    email: str
    provider: LoginProviderResponse | None
    methods: list[LoginProviderResponse]
    email_available: bool
    destination: str

    @classmethod
    def from_proposal(cls, proposal: LinkProposal, store: Store, login: Login, settings: Any) -> LinkProposalResponse:
        """Describe a proposal with the ways it can be proved.

        Args:
            proposal: The proposal.
            store: The database store, read for the profile's ways to sign in.
            login: The sign-in providers the deployment offers.
            settings: The settings, whose ``smtp`` is the mail server.

        Returns:
            The response model.
        """
        provider = login.providers.get(proposal.provider)
        methods = login.providers_of(store.profiles.issuers(proposal.profile_id))
        return cls(
            email=proposal.email,
            provider=LoginProviderResponse.from_provider(provider) if provider is not None else None,
            methods=[LoginProviderResponse.from_provider(method) for method in methods],
            email_available=settings.smtp.enabled,
            destination=proposal.destination,
        )


def get_link_proposal(request: Request, login: OfferedLoginDep) -> LinkProposal:
    """The link the caller's login proposed, from its login session.

    Args:
        request: The request, whose login session holds the proposal.
        login: The sign-in providers the deployment offers, whose middleware
            keeps the login session.

    Returns:
        The proposal.

    Raises:
        HTTPException: 404 when no proposal waits, or it expired with the
            login session.
    """
    proposal = LinkProposal.from_session(request.session)
    if proposal is None:
        raise HTTPException(status_code=404, detail="No link is waiting; sign in again")
    return proposal


LinkProposalDep = Annotated[LinkProposal, Depends(get_link_proposal)]


@router.get("/link")
def read_link_proposal(
    proposal: LinkProposalDep, store: StoreDep, login: OfferedLoginDep, settings: SettingsDep
) -> LinkProposalResponse:
    """Describe the link the caller's login proposed, for the page that asks them to prove it.

    Answers without a session: the login session is the credential, and it
    already proves the new account.

    Args:
        proposal: The proposal.
        store: The database store.
        login: The sign-in providers the deployment offers.
        settings: The settings, whose ``smtp`` is the mail server.

    Returns:
        The proposal and the ways to prove it.
    """
    return LinkProposalResponse.from_proposal(proposal, store, login, settings)


@router.post("/link/email", status_code=204)
def email_link_proposal(
    request: Request, proposal: LinkProposalDep, login: OfferedLoginDep, settings: SettingsDep
) -> Response:
    """Email the address a link that confirms the proposed account, for someone who cannot sign in the old way.

    The link carries the proposal, encrypted, so nothing is stored; the proposal
    leaves the login session.

    Args:
        request: The request, whose login session holds the proposal.
        proposal: The proposal.
        login: The sign-in providers the deployment offers, which sign the
            link and name the provider in the email.
        settings: The settings, whose ``smtp`` is the mail server and
            ``server`` the public URL the link points at.

    Returns:
        An empty 204 response.

    Raises:
        HTTPException: 409 when the deployment sends no email.
    """
    if not settings.smtp.enabled:
        raise HTTPException(status_code=409, detail="This deployment sends no email")
    provider = login.providers.get(proposal.provider)
    email = LinkConfirmationEmail.from_link(
        login.seal_link(proposal),
        provider_label=provider.label if provider is not None else proposal.provider,
        address=proposal.email,
        base_url=settings.server.public_url,
    )
    email.deliver(settings.smtp, proposal.email)
    LinkProposal.discard(request.session)
    return Response(status_code=204)


@router.delete("/link", status_code=204)
def cancel_link_proposal(request: Request, login: OfferedLoginDep) -> Response:
    """Drop the link the caller's login proposed, if any.

    Args:
        request: The request, whose login session may hold a proposal.
        login: The sign-in providers the deployment offers, whose middleware
            keeps the login session.

    Returns:
        An empty 204 response.
    """
    LinkProposal.discard(request.session)
    return Response(status_code=204)


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
def get_me(user: CurrentUserDep, store: StoreDep, login: LoginDep, features: FeaturesDep) -> AuthUserResponse:
    """Return the current user, how they sign in, and the organisations they belong to.

    Args:
        user: The authenticated caller.
        store: The database store.
        login: The sign-in providers the deployment offers.
        features: Optional-feature availability, reported back.

    Returns:
        The caller's profile, the providers they sign in through, their
        memberships with the role in each, the organisation they last used,
        and the enabled feature flags.
    """
    return AuthUserResponse.from_profile(user, store, login, features)


# -- Profile -------------------------------------------------------------------


class UpdateMeRequest(BaseModel):
    """User-editable profile fields; omitted fields stay untouched.

    Email is absent by design: linking, signup and super-admin rights trust
    the profile's address, so it is the one the profile was created with,
    and changing it would need proof of the new one.
    """

    name: str | None = Field(default=None, min_length=1, max_length=200)
    timezone: str | None = None


@router.patch("/me")
def update_me(
    body: UpdateMeRequest, user: CurrentUserDep, store: StoreDep, login: LoginDep, features: FeaturesDep
) -> AuthUserResponse:
    """Update the current user's profile (display name, timezone).

    Args:
        body: The fields to change; omitted fields stay untouched.
        user: The authenticated caller.
        store: The database store.
        login: The sign-in providers the deployment offers.
        features: Optional-feature availability, reported back.

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
    return AuthUserResponse.from_profile(profile, store, login, features)


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


# -- Sign-in methods -----------------------------------------------------------


class IdentityResponse(BaseModel):
    """A way the caller signs in.

    Attributes:
        id: The identity's reference, derived from it, which names it to the
            other routes without exposing the provider's subject.
        provider: The provider the deployment offers for it; ``None`` when the
            deployment no longer offers one.
        created_at: When it was added.
    """

    id: UUID
    provider: LoginProviderResponse | None
    created_at: datetime | None

    @classmethod
    def from_identity(cls, identity: Identity, login: Login | None) -> IdentityResponse:
        """Describe an identity, naming its provider among those offered.

        Args:
            identity: The identity.
            login: The sign-in providers the deployment offers.

        Returns:
            The response model.
        """
        providers = login.providers_of([identity.issuer]) if login is not None else []
        return cls(
            id=identity.reference,
            provider=LoginProviderResponse.from_provider(providers[0]) if providers else None,
            created_at=identity.created_at,
        )


class ConfirmLinkRequest(BaseModel):
    """Request body confirming an emailed link."""

    token: str


@router.post("/link/confirm")
def confirm_link(body: ConfirmLinkRequest, store: StoreDep, login: OfferedLoginDep) -> IdentityResponse:
    """Let the account an emailed link proposes sign in to the profile holding the address.

    Answers without a credential: following the link from the address's
    inbox is the proof. The app's page posts it on a click, never on load, so
    a mail scanner opening the link confirms nothing. Following it again
    changes nothing.

    Args:
        body: The link's token.
        store: The database store.
        login: The sign-in providers the deployment offers, which sealed the
            link.

    Returns:
        The way to sign in the link added.

    Raises:
        HTTPException: 400 when the link was altered or expired, or its profile
            is gone or no longer holds the address it was emailed to; 409 when
            the account signs in to another profile since.
    """
    proposal = login.open_link(body.token)
    if proposal is None:
        raise HTTPException(status_code=400, detail=_INVALID_LINK)
    try:
        profile = store.profiles.get(proposal.profile_id)
    except NotFoundError as error:
        raise HTTPException(status_code=400, detail=_INVALID_LINK) from error
    if profile.email.lower() != proposal.email.lower():
        raise HTTPException(status_code=400, detail=_INVALID_LINK)
    holder = store.profiles.get_by_identity(proposal.issuer, proposal.subject)
    if holder is None:
        store.profiles.link(profile.id, issuer=proposal.issuer, subject=proposal.subject)
    elif holder.id != profile.id:
        raise HTTPException(status_code=409, detail="This account already signs in to another profile")
    added = next(
        identity
        for identity in store.profiles.identities(profile.id)
        if (identity.issuer, identity.subject) == (proposal.issuer, proposal.subject)
    )
    return IdentityResponse.from_identity(added, login)


@router.get("/identities")
def list_identities(user: SessionUserDep, store: StoreDep, login: LoginDep) -> list[IdentityResponse]:
    """List the ways the caller signs in, oldest first.

    Args:
        user: The caller, authenticated by the session cookie alone.
        store: The database store.
        login: The sign-in providers the deployment offers.

    Returns:
        The caller's identities.
    """
    return [IdentityResponse.from_identity(identity, login) for identity in store.profiles.identities(user.id)]


@router.delete("/identities/{identity_id}", status_code=204)
def remove_identity(
    identity_id: UUID, user: SessionUserDep, store: StoreDep, session_token: Annotated[str | None, Cookie()] = None
) -> Response:
    """Remove a way the caller signs in, and end the caller's other sessions.

    Whoever signed in through the account removed keeps no session; the
    caller keeps theirs.

    Args:
        identity_id: The identity's reference.
        user: The caller, authenticated by the session cookie alone.
        store: The database store.
        session_token: The caller's own session, which stays live.

    Returns:
        An empty 204 response.
    """
    store.profiles.unlink(user.id, identity_id)
    store.tokens.revoke_sessions(user.id, keep=session_token)
    return Response(status_code=204)
