"""Super-admin routes: the platform-wide views.

These endpoints are gated by :func:`require_super_admin` and are NOT bound to
the session's active organisation: every organisation (soft-deleted ones
included), every user, quotas and the instance configuration. Managing one
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

from fastapi import APIRouter, HTTPException, Query, Request, Response
from interloper_db import Organisation, OrganisationQuery, Page, PageQuery, Profile, ProfileQuery, UsageQuery
from interloper_db.store.insights import ActivityEntry
from interloper_db.store.quotas import METRIC_SUCCESSFUL_RUNS, QUOTAS
from pydantic import BaseModel, RootModel, field_validator

from interloper_api.dependencies import (
    AdminConfigDep,
    AuthConfigDep,
    QuotaDefaultsDep,
    StoreDep,
    SuperAdminDep,
    get_smtp_config,
)
from interloper_api.notifications import SuperAdminPromotionEmail
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


class AdminLauncherConfig(BaseModel):
    """Launcher type plus the key-allowlisted, non-secret part of its config.

    ``defaults`` carries the launcher class's own constructor defaults for
    allowlisted keys the config doesn't set — the effective values.
    """

    type: str
    config: dict[str, Any]
    defaults: dict[str, Any]


class AdminRunnerConfig(BaseModel):
    """Runner type plus the key-allowlisted, non-secret part of its config.

    ``defaults`` carries the runner class's own field defaults for
    allowlisted keys the config doesn't set — the effective values.
    """

    type: str
    config: dict[str, Any]
    defaults: dict[str, Any]


class AdminDeploymentConfig(BaseModel):
    """What this instance is: version, execution stack, optional features."""

    version: str | None = None
    launcher: AdminLauncherConfig
    runner: AdminRunnerConfig
    features: dict[str, bool]
    agent_model: str | None = None


class AdminAuthConfig(BaseModel):
    """Authentication and signup policy."""

    allowed_domains: list[str]
    super_admin_emails: list[str]
    google_oauth_configured: bool
    google_redirect_uri: str
    session_expiry_days: int
    cookie_secure: bool


class AdminCronConfig(BaseModel):
    """Cron controller tuning."""

    enabled: bool
    reconcile_interval: int
    batch_size: int
    max_execution_delay: int | None = None


class AdminWorkerConfig(BaseModel):
    """Queue worker tuning."""

    enabled: bool
    poll_interval: int


class AdminReaperConfig(BaseModel):
    """Run liveness tuning: the heartbeat and the reaper that fails silent or overdue runs."""

    enabled: bool
    poll_interval: int
    startup_timeout: int
    heartbeat_interval: int
    heartbeat_timeout: int
    run_timeout: int | None


class AdminSmtpConfig(BaseModel):
    """SMTP status (credentials reduced to the enabled flag)."""

    enabled: bool
    host: str
    from_addr: str


class AdminTelemetryConfig(BaseModel):
    """OpenTelemetry status (endpoint/headers reduced to booleans)."""

    enabled: bool
    protocol: str
    endpoint_configured: bool
    traces: bool
    metrics: bool
    sample_ratio: float


class AdminServicesConfig(BaseModel):
    """Background service roles and their tuning."""

    cron: AdminCronConfig
    worker: AdminWorkerConfig
    reaper: AdminReaperConfig
    smtp: AdminSmtpConfig
    telemetry: AdminTelemetryConfig
    app_external_url: str
    mcp_external_url: str


class AdminDataConfig(BaseModel):
    """Data-layer status and the computed catalog (kind → component keys)."""

    encryption_configured: bool
    catalog: dict[str, list[str]]


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

    deployment: AdminDeploymentConfig
    auth: AdminAuthConfig
    services: AdminServicesConfig
    data: AdminDataConfig
    quotas: AdminQuotaLimits

    @classmethod
    def from_settings(cls, settings: Any, features: dict[str, bool], catalog: Any = None) -> AdminConfigResponse:
        """Build the redacted instance-config snapshot served by ``GET /admin/config``.

        Every exposed field is hand-picked here (allowlist, not blocklist), so new
        settings fields default to *not exposed* and secrets only ever surface as
        "configured" booleans. The catalog is reported from the hydrated ``Catalog``
        (the enabled components, their dependencies and the framework's own), not
        ``settings.catalog``, which only holds the explicitly configured import paths.

        Args:
            settings: The instance settings the snapshot reads from.
            features: Optional-feature flags keyed by feature name.
            catalog: The hydrated catalog to report, or ``None`` to report an empty
                one (the snapshot is then built without catalog access).

        Returns:
            The snapshot, ready to serve as-is.
        """
        try:
            version: str | None = metadata.version("interloper-api")
        except metadata.PackageNotFoundError:
            version = None

        launcher_config = _filter_config(settings.launcher.config, _LAUNCHER_CONFIG_KEYS)
        # The launcher forwards a nested runner config into the containers it
        # launches — that's where the effective run concurrency lives on k8s/docker.
        nested_runner_config = (settings.launcher.config or {}).get("runner_config")
        if isinstance(nested_runner_config, dict):
            launcher_config["runner_config"] = _filter_config(nested_runner_config, _RUNNER_CONFIG_KEYS)

        runner_config = _filter_config(settings.runner.config, _RUNNER_CONFIG_KEYS)

        return cls(
            deployment=AdminDeploymentConfig(
                version=version,
                launcher=AdminLauncherConfig(
                    type=settings.launcher.type,
                    config=launcher_config,
                    defaults={
                        key: value
                        for key, value in _launcher_defaults(settings.launcher.type).items()
                        if key not in launcher_config
                    },
                ),
                runner=AdminRunnerConfig(
                    type=settings.runner.type,
                    config=runner_config,
                    defaults={
                        key: value
                        for key, value in _runner_defaults(settings.runner.type).items()
                        if key not in runner_config
                    },
                ),
                features=features,
                agent_model=settings.agent.model if settings.agent.enabled else None,
            ),
            auth=AdminAuthConfig(
                allowed_domains=settings.auth.allowed_domains,
                super_admin_emails=settings.auth.super_admin_emails,
                google_oauth_configured=bool(settings.auth.google_client_id and settings.auth.google_client_secret),
                google_redirect_uri=settings.auth.google_redirect_uri,
                session_expiry_days=settings.auth.session_expiry_days,
                cookie_secure=settings.auth.cookie_secure,
            ),
            services=AdminServicesConfig(
                cron=AdminCronConfig(
                    enabled=settings.cron.enabled,
                    reconcile_interval=settings.cron.reconcile_interval,
                    batch_size=settings.cron.batch_size,
                    max_execution_delay=settings.cron.max_execution_delay,
                ),
                worker=AdminWorkerConfig(
                    enabled=settings.worker.enabled,
                    poll_interval=settings.worker.poll_interval,
                ),
                reaper=AdminReaperConfig(
                    enabled=settings.reaper.enabled,
                    poll_interval=settings.reaper.poll_interval,
                    startup_timeout=settings.reaper.startup_timeout,
                    heartbeat_interval=settings.reaper.heartbeat_interval,
                    heartbeat_timeout=settings.reaper.heartbeat_timeout,
                    run_timeout=settings.reaper.run_timeout,
                ),
                smtp=AdminSmtpConfig(
                    enabled=settings.smtp.enabled,
                    host=settings.smtp.host,
                    from_addr=settings.smtp.from_addr,
                ),
                telemetry=AdminTelemetryConfig(
                    enabled=settings.otel.enabled,
                    protocol=settings.otel.protocol,
                    endpoint_configured=bool(settings.otel.endpoint),
                    traces=settings.otel.traces,
                    metrics=settings.otel.metrics,
                    sample_ratio=settings.otel.sample_ratio,
                ),
                app_external_url=settings.server.external_url,
                mcp_external_url=settings.mcp.external_url,
            ),
            data=AdminDataConfig(
                encryption_configured=bool(settings.secrets.encryption_key),
                catalog=_catalog_by_kind(catalog) if catalog is not None else {},
            ),
            quotas=_quota_limits(settings.quota),
        )


# -- Config snapshot -----------------------------------------------------------


# Non-secret launcher/runner config keys exposed on /admin/config. Anything not
# listed (env injections, image_pull_secrets, credentials, …) stays server-side.
_LAUNCHER_CONFIG_KEYS = frozenset(
    {
        "image",
        "namespace",
        "service_account_name",
        "image_pull_policy",
        "ttl_seconds_after_finished",
        "node_selector",
        "resources",
        "volumes",
        "runner_type",
    }
)
_RUNNER_CONFIG_KEYS = frozenset(
    {
        "max_workers",
        "image",
        "namespace",
        "service_account_name",
        "image_pull_policy",
        "ttl_seconds_after_finished",
        "node_selector",
        "resources",
    }
)


def _filter_config(config: dict[str, Any] | None, allowed: frozenset[str]) -> dict[str, Any]:
    """Keep only allowlisted keys of a launcher/runner config dict.

    Args:
        config: The raw config dict, or ``None`` when nothing is configured.
        allowed: The keys cleared for exposure on the admin surface.

    Returns:
        The config restricted to ``allowed``, empty when ``config`` is ``None``.
    """
    return {key: value for key, value in (config or {}).items() if key in allowed}


def _launcher_defaults(launcher_type: str) -> dict[str, Any]:
    """Allowlisted constructor defaults of the registered launcher class.

    Launcher defaults live in ``__init__`` signatures, invisible to settings —
    without this, an unset value reads as "missing" when a class default
    applies. Returns ``{}`` when the launcher package isn't installed here
    (e.g. an API-only image) — the view then simply shows no defaults.

    Args:
        launcher_type: The registered launcher key, as configured on the
            instance (``k8s``, ``docker``, ``in_process``, …).

    Returns:
        Allowlisted parameter names mapped to their default value, skipping
        parameters that default to ``None``.
    """
    try:
        from interloper_scheduler.launcher import LAUNCHERS
    except ImportError:
        return {}
    launcher_cls = LAUNCHERS.get(launcher_type)
    if launcher_cls is None:
        return {}
    return {
        name: param.default
        for name, param in inspect.signature(launcher_cls.__init__).parameters.items()
        if name in _LAUNCHER_CONFIG_KEYS and param.default is not inspect.Parameter.empty and param.default is not None
    }


def _runner_defaults(runner_type: str) -> dict[str, Any]:
    """Allowlisted field defaults of the registered runner class (pydantic model).

    Args:
        runner_type: The registered runner key, as configured on the instance.

    Returns:
        Allowlisted field names mapped to their default value, empty when the
        type is unregistered.
    """
    from interloper.runner.base import RUNNERS

    runner_cls = RUNNERS.get(runner_type)
    if runner_cls is None:
        return {}
    return {
        name: field.default
        for name, field in runner_cls.model_fields.items()
        if name in _RUNNER_CONFIG_KEYS and not field.is_required() and field.default is not None
    }


def _catalog_by_kind(catalog: Any) -> dict[str, list[str]]:
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
def get_instance_config(
    user: SuperAdminDep,
    config: AdminConfigDep,
) -> AdminConfigResponse:
    """Read-only snapshot of the instance configuration (secrets redacted).

    Args:
        user: The calling super-admin, resolved from the session.
        config: The snapshot built at startup, ``None`` when the app could not
            build one.

    Returns:
        The instance-config snapshot.

    Raises:
        HTTPException: 503 when no snapshot was built at startup.
    """
    if config is None:
        raise HTTPException(status_code=503, detail="Instance configuration not available")
    return config


# -- Quotas --------------------------------------------------------------------


@router.get("/quotas")
def get_quotas(
    user: SuperAdminDep,
    store: StoreDep,
    quota_defaults: QuotaDefaultsDep,
) -> AdminQuotasResponse:
    """Quota limits and current-period usage for every organisation.

    Args:
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.
        quota_defaults: The instance-wide quota defaults from settings.

    Returns:
        The current period start, the global defaults, the field descriptors,
        and one status entry per organisation, soft-deleted ones included.
    """
    defaults = _quota_limits(quota_defaults)
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
    auth_config: AuthConfigDep,
    request: Request,
) -> AdminUserResponse:
    """Grant or revoke a user's super-admin access.

    A promotion emails every super-admin, the new one included, so a grant
    never goes unnoticed. A revocation is refused for an email listed in
    ``auth.super_admin_emails``: the next login or ``db init`` would promote
    it straight back.

    Args:
        user_id: The profile to change.
        body: Whether the profile is a super-admin.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.
        auth_config: The resolved auth configuration, read for the configured
            super-admin emails.
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
    if not body.is_super_admin and target.email.lower() in auth_config.super_admin_emails:
        raise HTTPException(
            status_code=409,
            detail=f"{target.email} is listed in auth.super_admin_emails; remove it there to revoke access",
        )

    promoted = body.is_super_admin and not target.is_super_admin
    updated = store.profiles.set_super_admin(user_id, value=body.is_super_admin)
    if promoted:
        email = SuperAdminPromotionEmail.from_promotion(updated, promoted_by=user, base_url=str(request.base_url))
        for recipient in store.profiles.list(ProfileQuery(super_admin=True, limit=None)).items:
            email.deliver(get_smtp_config(), recipient.email)
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
