"""Super-admin routes: the platform-wide views.

These endpoints are gated by :func:`require_super_admin` and are NOT bound to
the session's active organisation: every organisation (soft-deleted ones
included), every user, quotas, the operator OAuth clients and the instance
configuration. Managing one
organisation's members and invitations goes through the organisation routes,
which admit super-admins (:mod:`interloper_api.routes.organisations`). They
deliberately grant no access to org-scoped *data* (sources, jobs, runs, …).
"""

from __future__ import annotations

import datetime as dt
import inspect
from datetime import datetime
from importlib import metadata
from typing import Annotated, Any
from uuid import UUID

from authlib.oauth2.rfc7591 import InvalidRedirectURIError
from fastapi import APIRouter, HTTPException, Query, Request, Response
from interloper.catalog import Catalog
from interloper.settings import SETTINGS, Settings
from interloper_db import Organisation, OrganisationQuery, Page, PageQuery, Profile, ProfileQuery, UsageQuery
from interloper_db.models import OAuthClient
from interloper_db.store.insights import ActivityEntry
from interloper_db.store.quotas import METRIC_SUCCESSFUL_RUNS, QUOTAS
from pydantic import BaseModel, Field, RootModel, field_validator

from interloper_api.authorization import CLIENT_NAME_MAX_LENGTH, Client
from interloper_api.dependencies import SettingsDep, StateDep, StoreDep, SuperAdminDep
from interloper_api.notifications import SuperAdminPromotionEmail
from interloper_api.routes.apps import AppResponse
from interloper_api.routes.organisations import CreateOrganisationRequest

router = APIRouter(prefix="/admin", tags=["admin"])


# -- Request & response models -------------------------------------------------


class AdminOrganisationResponse(BaseModel):
    """Organisation summary with member count for the admin surface.

    Soft-deleted organisations stay listed, their retained history and ledger
    still being attributable, but are no longer manageable.
    """

    id: UUID
    name: str
    key: str
    member_count: int
    created_at: datetime | None = None
    deleted_at: datetime | None = None

    @classmethod
    def from_organisation(cls, organisation: Organisation, member_count: int) -> AdminOrganisationResponse:
        """Describe an organisation row with its member count.

        Args:
            organisation: The organisation row.
            member_count: How many members it has.

        Returns:
            The response model.
        """
        return cls(
            id=organisation.id,
            name=organisation.name,
            key=organisation.key,
            member_count=member_count,
            created_at=organisation.created_at,
            deleted_at=organisation.deleted_at,
        )


class AdminUserOrganisation(BaseModel):
    """Organisation reference on a user row."""

    id: UUID
    name: str


class AdminUserResponse(BaseModel):
    """Platform user with its organisation memberships for the admin surface."""

    id: UUID
    email: str
    name: str | None = None
    avatar_url: str | None = None
    is_super_admin: bool = False
    organisations: list[AdminUserOrganisation]
    created_at: datetime | None = None

    @classmethod
    def from_profile(cls, profile: Profile) -> AdminUserResponse:
        """Describe a profile with the organisations it belongs to.

        Args:
            profile: The profile row, its ``organisations`` loaded.

        Returns:
            The response model.
        """
        return cls(
            id=profile.id,
            email=profile.email,
            name=profile.name,
            avatar_url=profile.avatar_url,
            is_super_admin=profile.is_super_admin,
            organisations=[AdminUserOrganisation(id=org.id, name=org.name) for org in profile.organisations],
            created_at=profile.created_at,
        )


class AdminUserUpdateRequest(BaseModel):
    """Platform-wide privileges a super-admin may change on another user."""

    is_super_admin: bool


class AdminOAuthClientCreateRequest(BaseModel):
    """An operator client to register: its name and the URIs its codes may be sent to."""

    name: str = Field(min_length=1, max_length=CLIENT_NAME_MAX_LENGTH)
    redirect_uris: list[str]

    @field_validator("name")
    @classmethod
    def _strip_name(cls, name: str) -> str:
        """Refuse a name of whitespace only, which the consent page would show blank.

        Args:
            name: The name as sent.

        Returns:
            The name without surrounding whitespace.

        Raises:
            ValueError: When nothing is left once stripped.
        """
        stripped = name.strip()
        if not stripped:
            raise ValueError("A client needs a name")
        return stripped


class AdminOAuthClientCreatedResponse(AppResponse):
    """A client just registered, carrying its secret: the only response that ever does."""

    client_secret: str

    @classmethod
    def from_registration(cls, client: OAuthClient, secret: str | None) -> AdminOAuthClientCreatedResponse:
        """Describe a client just registered, with the secret the store returned once.

        Args:
            client: The stored client.
            secret: The raw secret; an operator client is confidential, so it
                always has one.

        Returns:
            The response model.

        Raises:
            ValueError: When the client came back without a secret.
        """
        if secret is None:
            raise ValueError(f"Client {client.client_id} was registered without a secret")
        return cls(**AppResponse.from_client(client).model_dump(), client_secret=secret)


class AdminSettingsField(BaseModel):
    """One field of a settings section, or one public key inside a mapping field (``config.image``).

    ``value`` is filled only when the section lists the field as public;
    ``configured`` says whether the deployment changed it from its default,
    which is all a secret ever shows.
    """

    name: str
    description: str | None = None
    public: bool
    configured: bool
    value: Any = None


class AdminSettingsSection(BaseModel):
    """One declared settings section, as the running instance loaded it."""

    name: str
    extension: str
    description: str | None = None
    fields: list[AdminSettingsField]

    @classmethod
    def from_settings(cls, name: str, extension: str, settings: Settings) -> AdminSettingsSection:
        """Redact a loaded section down to the values it lists as public.

        Args:
            name: The section's name.
            extension: The key of the extension that declares it.
            settings: The section as this process runs with it.

        Returns:
            The section, its fields in declaration order, each public key of a
            mapping field right after that field.
        """
        fields: list[AdminSettingsField] = []
        for field_name, field in type(settings).model_fields.items():
            value = getattr(settings, field_name)
            public = field_name in settings.public
            fields.append(
                AdminSettingsField(
                    name=field_name,
                    description=field.description,
                    public=public,
                    configured=value != field.get_default(call_default_factory=True),
                    value=value if public else None,
                )
            )
            for path in sorted(entry for entry in settings.public if entry.startswith(f"{field_name}.")):
                found, nested = cls._reach(value, path.split(".")[1:])
                if found:
                    fields.append(AdminSettingsField(name=path, public=True, configured=True, value=nested))
        summary = inspect.cleandoc(type(settings).__doc__ or "").partition("\n")[0]
        return cls(name=name, extension=extension, description=summary or None, fields=fields)

    @classmethod
    def _reach(cls, value: Any, keys: list[str]) -> tuple[bool, Any]:
        """Follow ``keys`` down nested mappings.

        Args:
            value: The mapping field's value.
            keys: The keys below the field, outermost first.

        Returns:
            Whether every key was present, and the value it leads to (``None``
            when one was missing).
        """
        for key in keys:
            if not isinstance(value, dict) or key not in value:
                return False, None
            value = value[key]
        return True, value


# One set of quota limits, keyed by quota key; null means unlimited (or unset,
# for overrides). Derived from the ``QUOTAS`` registry rather than declared
# field-by-field, so registering a quota surfaces it here — and in the frontend,
# which already indexes these by key — with no code change.
AdminQuotaLimits = dict[str, int | None]


class AdminOrgQuotaStatus(BaseModel):
    """One organisation's quota limits and current usage.

    Soft-deleted organisations keep their ledger visible but are read-only.
    ``recomputed_successful_runs`` comes from the runs table rather than the
    ledger, so a mismatch with ``successful_runs`` signals counter drift.
    """

    id: UUID
    name: str
    deleted_at: datetime | None = None
    limits: AdminQuotaLimits
    effective: AdminQuotaLimits
    sources: int
    max_assets_per_source: int
    successful_runs: int
    reserved_runs: int
    recomputed_successful_runs: int


class AdminQuotaField(BaseModel):
    """One quota's display descriptor: key, registry label, instance default."""

    key: str
    label: str
    default: int | None = None


class AdminQuotasResponse(BaseModel):
    """Quota overview: global defaults plus per-organisation status."""

    period_start: dt.date
    defaults: AdminQuotaLimits
    fields: list[AdminQuotaField]
    organisations: list[AdminOrgQuotaStatus]


class AdminQuotaUpdateRequest(RootModel[AdminQuotaLimits]):
    """Per-org quota overrides. Omitted keys keep their value; null clears one.

    Keys are checked against the registry here so an unknown quota is a 422
    at the boundary rather than a ``KeyError`` out of the store.
    """

    @field_validator("root")
    @classmethod
    def _known_keys_and_non_negative(cls, value: AdminQuotaLimits) -> AdminQuotaLimits:
        """Validate the payload against the quota registry.

        Args:
            value: The submitted overrides, keyed by quota key.

        Returns:
            The payload unchanged.

        Raises:
            ValueError: If a key names no registered quota, or a limit is
                negative.
        """
        if unknown := sorted(set(value) - set(QUOTAS.keys())):
            raise ValueError(f"Unknown quota(s): {', '.join(unknown)}. Known: {', '.join(sorted(QUOTAS.keys()))}")
        if negative := sorted(key for key, limit in value.items() if limit is not None and limit < 0):
            raise ValueError(f"Quota limits must be >= 0: {', '.join(negative)}")
        return value


class AdminActivityEntry(BaseModel):
    """One event in an organisation's derived activity feed, worded for display."""

    kind: str
    when: datetime
    title: str
    detail: str | None = None

    @classmethod
    def from_entry(cls, entry: ActivityEntry) -> AdminActivityEntry:
        """Word a derived activity entry as a title and a detail line.

        Args:
            entry: The store's activity entry.

        Returns:
            The response model; the detail is ``None`` when the entry carries
            nothing beyond its title.
        """
        subject, extra = entry.subject, entry.extra
        if entry.kind == "org_created":
            title, detail = "Organisation created", None
        elif entry.kind == "org_deleted":
            title, detail = "Organisation deleted", "Retained read-only for billing history."
        elif entry.kind == "member_joined":
            title, detail = f"{subject} joined the organisation", f"Role: {extra}" if extra else None
        elif entry.kind == "invitation_sent":
            title, detail = f"Invitation sent to {subject}", f"Invited by {extra}" if extra else None
        elif entry.kind == "source_added":
            title, detail = f"Source added: {subject}", None
        elif entry.kind == "runs_completed":
            count = int(subject or 0)
            title, detail = f"{count:,} run{'' if count == 1 else 's'} completed successfully", None
        else:
            title, detail = entry.kind, None
        return cls(kind=entry.kind, when=entry.when, title=title, detail=detail)


class AdminConfigResponse(BaseModel):
    """Read-only, secrets-redacted snapshot of the instance configuration."""

    version: str | None = None
    features: dict[str, bool]
    catalog: dict[str, list[str]]
    sections: list[AdminSettingsSection]

    @classmethod
    def from_declared(cls, features: dict[str, bool], catalog: Catalog) -> AdminConfigResponse:
        """Build the redacted snapshot served by ``GET /admin/config`` from every declared settings section.

        A section shows the values of the fields it lists as public and, for
        every other field, only whether it is configured, so a new field stays
        hidden until its section makes it public. A section whose package
        cannot be imported is left out. The catalog is reported from the
        hydrated ``Catalog`` (the enabled components, their dependencies and
        the framework's own), not the ``catalog.include`` setting, which only
        holds the explicitly configured import paths.

        Args:
            features: Optional-feature flags keyed by feature name.
            catalog: The hydrated catalog to report.

        Returns:
            The snapshot, ready to serve as-is.
        """
        try:
            version: str | None = metadata.version("interloper-api")
        except metadata.PackageNotFoundError:
            version = None
        sections = []
        for name, entry in sorted(Settings.declared().items()):
            settings_class = SETTINGS.get(name)
            if settings_class is not None:
                sections.append(AdminSettingsSection.from_settings(name, entry.extension, settings_class.get()))
        return cls(
            version=version,
            features=features,
            catalog=_catalog_by_kind(catalog),
            sections=sections,
        )


# -- Config snapshot -----------------------------------------------------------


def _catalog_by_kind(catalog: Catalog) -> dict[str, list[str]]:
    """Group the hydrated catalog's component keys by kind, both sorted.

    Args:
        catalog: The hydrated catalog whose ``components`` are grouped.

    Returns:
        Component keys by kind, kinds and keys both sorted alphabetically.
    """
    by_kind: dict[str, list[str]] = {}
    for key, definition in catalog.components.items():
        by_kind.setdefault(definition.kind, []).append(key)
    return {kind: sorted(keys) for kind, keys in sorted(by_kind.items())}


# -- Helpers -------------------------------------------------------------------


def _quota_limits(limits: Any) -> AdminQuotaLimits:
    """Map limits from a ``{key: limit}`` dict (store) or attributes (settings).

    Args:
        limits: A ``{key: limit}`` mapping, or an object carrying one attribute
            per quota key.

    Returns:
        Every registered quota's limit, ``None`` where unset.
    """
    if isinstance(limits, dict):
        return {key: limits.get(key) for key in QUOTAS}
    return {key: getattr(limits, key, None) for key in QUOTAS}


def _effective_limits(overrides: AdminQuotaLimits, defaults: AdminQuotaLimits) -> AdminQuotaLimits:
    """Per-org overrides win key-by-key over the global defaults.

    Args:
        overrides: One organisation's overrides; a key that is missing or set
            to ``None`` falls through to the default.
        defaults: The instance-wide limits.

    Returns:
        The effective limit of every registered quota.
    """
    return {key: override if (override := overrides.get(key)) is not None else defaults.get(key) for key in QUOTAS}


def _quota_fields(defaults: AdminQuotaLimits) -> list[AdminQuotaField]:
    """Field descriptors for admin quota surfaces, in registry order.

    Keys, labels and defaults all come from the registry, so registering a
    quota is the whole change: no wire model, no frontend edit. ``Registry``
    sorts its keys, so the admin surfaces list quotas alphabetically rather
    than in the order they happen to be registered.

    Args:
        defaults: The instance-wide limits, reported as each field's default.

    Returns:
        One descriptor per registered quota.
    """
    return [AdminQuotaField(key=key, label=QUOTAS[key].label, default=defaults.get(key)) for key in QUOTAS]


# -- Instance config -----------------------------------------------------------


@router.get("/config")
def get_instance_config(user: SuperAdminDep, state: StateDep) -> AdminConfigResponse:
    """Read-only snapshot of the instance configuration (secrets redacted).

    Args:
        user: The calling super-admin, resolved from the session.
        state: The application's state, holding the snapshot built at startup.

    Returns:
        The instance-config snapshot.
    """
    return state.admin_config


# -- Quotas --------------------------------------------------------------------


@router.get("/quotas")
def get_quotas(
    user: SuperAdminDep,
    store: StoreDep,
    settings: SettingsDep,
) -> AdminQuotasResponse:
    """Quota limits and current-period usage for every organisation.

    Args:
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.
        settings: The settings, whose ``quota`` holds the instance-wide defaults.

    Returns:
        The current period start, the global defaults, the field descriptors,
        and one status entry per organisation, soft-deleted ones included.
    """
    defaults = _quota_limits(settings.quota)
    period_start = store.usage.current_period()
    overrides = store.quotas.all_overrides()
    usage = {
        row.org_id: row
        for row in store.usage.list(UsageQuery(period_start=period_start, limit=None)).items
        if row.metric == METRIC_SUCCESSFUL_RUNS
    }
    sources = store.usage.sources_by_org()
    max_assets = store.usage.max_assets_per_source_by_org()
    recomputed = store.usage.successful_runs_by_org(period_start)

    organisations = []
    for org in store.organisations.list(OrganisationQuery(include_deleted=True, limit=None)).items:
        limits = _quota_limits(overrides.get(org.id, {}))
        org_usage = usage.get(org.id)
        organisations.append(
            AdminOrgQuotaStatus(
                id=org.id,
                name=org.name,
                deleted_at=org.deleted_at,
                limits=limits,
                effective=_effective_limits(limits, defaults),
                sources=sources.get(org.id, 0),
                max_assets_per_source=max_assets.get(org.id, 0),
                successful_runs=org_usage.used if org_usage else 0,
                reserved_runs=org_usage.reserved if org_usage else 0,
                recomputed_successful_runs=recomputed.get(org.id, 0),
            )
        )
    return AdminQuotasResponse(
        period_start=period_start,
        defaults=defaults,
        fields=_quota_fields(defaults),
        organisations=organisations,
    )


@router.patch("/organisations/{org_id}/quota")
def update_org_quota(
    org_id: UUID,
    body: AdminQuotaUpdateRequest,
    user: SuperAdminDep,
    store: StoreDep,
) -> AdminQuotaLimits:
    """Set an organisation's quota overrides; omitted keys keep their value, null clears one.

    Args:
        org_id: The organisation whose overrides are written.
        body: The overrides to apply. An omitted key keeps its current value; a
            null clears the override, so the global default applies again.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        The organisation's overrides after the write, ``None`` where unset.
    """
    store.organisations.get(org_id)
    return _quota_limits(store.quotas.set_overrides(org_id, body.root))


# -- Organisations -------------------------------------------------------------


@router.get("/organisations")
def list_organisations(
    user: SuperAdminDep,
    store: StoreDep,
    query: Annotated[PageQuery, Query()],
) -> Page[AdminOrganisationResponse]:
    """List every organisation with its member count, soft-deleted ones included.

    Args:
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.
        query: The window to read.

    Returns:
        The page of organisations.
    """
    organisations = store.organisations.list(OrganisationQuery(include_deleted=True, **query.model_dump()))
    counts = store.members.count_by_org([org.id for org in organisations.items])
    return organisations.map(lambda org: AdminOrganisationResponse.from_organisation(org, counts.get(org.id, 0)))


@router.post("/organisations", status_code=201)
def create_organisation(
    body: CreateOrganisationRequest,
    user: SuperAdminDep,
    store: StoreDep,
) -> AdminOrganisationResponse:
    """Create an organisation. The super-admin is not added as a member.

    Args:
        body: The new organisation's name.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        The created organisation, whose member count is therefore zero.
    """
    return AdminOrganisationResponse.from_organisation(store.organisations.create(name=body.name), member_count=0)


@router.get("/organisations/{org_id}/activity")
def get_organisation_activity(
    org_id: UUID,
    user: SuperAdminDep,
    store: StoreDep,
    query: Annotated[PageQuery, Query()],
) -> Page[AdminActivityEntry]:
    """Derived activity feed for one organisation, newest first.

    Composed from existing records (memberships, pending invitations,
    sources, run aggregates, the org row itself); there is no audit trail,
    so actor attribution is limited to what those rows carry.

    Args:
        org_id: The organisation whose activity is derived.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.
        query: The window to read.

    Returns:
        The page of activity entries, newest first.
    """
    return store.insights.feed(org_id, query).map(AdminActivityEntry.from_entry)


# -- Users ---------------------------------------------------------------------


@router.get("/users")
def list_users(
    user: SuperAdminDep,
    store: StoreDep,
    query: Annotated[ProfileQuery, Query()],
) -> Page[AdminUserResponse]:
    """List every user profile with the organisations it belongs to.

    Args:
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.
        query: Whether to keep super-admins only, and the window to read.

    Returns:
        The page of profiles, each with its memberships.
    """
    return store.profiles.list(query).map(AdminUserResponse.from_profile)


@router.patch("/users/{user_id}")
def update_user(
    user_id: UUID,
    body: AdminUserUpdateRequest,
    user: SuperAdminDep,
    store: StoreDep,
    settings: SettingsDep,
    request: Request,
) -> AdminUserResponse:
    """Grant or revoke a user's super-admin access.

    A promotion emails every super-admin, the new one included, so a grant
    never goes unnoticed. A revocation is refused for an email listed in
    ``auth.super_admin_emails``: the next login or API start would promote
    it straight back.

    Args:
        user_id: The profile to change.
        body: Whether the profile is a super-admin.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.
        settings: The settings, read for the configured super-admin emails and
            the mailer.
        request: The incoming request, whose base URL the email links back to.

    Returns:
        The updated user.

    Raises:
        HTTPException: 400 when the caller targets their own account, 409 when
            revoking a super-admin the configuration lists.
    """
    if user_id == user.id:
        raise HTTPException(status_code=400, detail="You cannot change your own super-admin access")
    target = store.profiles.get(user_id)
    if not body.is_super_admin and target.email.lower() in settings.auth.super_admin_emails:
        raise HTTPException(
            status_code=409,
            detail=f"{target.email} is listed in auth.super_admin_emails; remove it there to revoke access",
        )

    promoted = body.is_super_admin and not target.is_super_admin
    updated = store.profiles.set_super_admin(user_id, value=body.is_super_admin)
    if promoted:
        email = SuperAdminPromotionEmail.from_promotion(updated, promoted_by=user, base_url=str(request.base_url))
        for recipient in store.profiles.list(ProfileQuery(super_admin=True, limit=None)).items:
            email.deliver(settings.smtp, recipient.email)
    return AdminUserResponse.from_profile(updated)


@router.delete("/users/{user_id}", status_code=204)
def delete_user(
    user_id: UUID,
    user: SuperAdminDep,
    store: StoreDep,
) -> Response:
    """Delete a user entirely: profile, sessions, tokens, memberships, sent invitations.

    Args:
        user_id: The profile to delete.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        An empty 204 response.

    Raises:
        HTTPException: 400 when the caller targets their own account.
    """
    if user_id == user.id:
        raise HTTPException(status_code=400, detail="You cannot delete your own account")
    store.profiles.delete(user_id)
    return Response(status_code=204)


# -- OAuth clients -------------------------------------------------------------


@router.get("/oauth-clients")
def list_oauth_clients(user: SuperAdminDep, store: StoreDep) -> list[AppResponse]:
    """List the operator-registered OAuth clients, the ones ``interloper oauth list`` shows.

    Clients that registered themselves are left out: anyone may register one,
    and each belongs to the users who approve it, who revoke it from their own
    settings.

    Args:
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        The operator clients by name, without secret material.
    """
    return [AppResponse.from_client(client) for client in store.oauth.list_clients(registered_by="operator")]


@router.post("/oauth-clients", status_code=201)
def create_oauth_client(
    body: AdminOAuthClientCreateRequest,
    user: SuperAdminDep,
    store: StoreDep,
) -> AdminOAuthClientCreatedResponse:
    """Register a confidential operator client, as ``interloper oauth create`` does.

    Args:
        body: The client's name and redirect URIs.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        The client with its secret, which only its hash is stored of: this
        response is the one chance to read it.

    Raises:
        HTTPException: 422 when no redirect URI is given or one could not
            safely receive codes.
    """
    try:
        Client.check_redirect_uris(body.redirect_uris)
    except InvalidRedirectURIError as error:
        raise HTTPException(status_code=422, detail=error.description) from error
    client, secret = store.oauth.register_client(
        body.name, body.redirect_uris, registered_by="operator", confidential=True
    )
    return AdminOAuthClientCreatedResponse.from_registration(client, secret)


@router.delete("/oauth-clients/{client_id}", status_code=204)
def delete_oauth_client(client_id: str, user: SuperAdminDep, store: StoreDep) -> Response:
    """Delete an operator client and every grant it holds, as ``interloper oauth delete`` does.

    Its codes and refresh tokens are removed and its access tokens revoked for
    every user, and the conversations it started are deleted.

    Args:
        client_id: The client to delete.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        An empty 204 response.

    Raises:
        HTTPException: 404 when no operator client has that id; a client that
            registered itself is never deleted here.
    """
    if store.oauth.delete_client(client_id, registered_by="operator") is None:
        raise HTTPException(status_code=404, detail=f"No operator client {client_id}")
    return Response(status_code=204)
