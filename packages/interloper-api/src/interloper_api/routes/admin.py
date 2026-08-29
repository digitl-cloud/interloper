"""Super-admin routes — cross-organisation management.

These endpoints are gated by :func:`require_super_admin` and are NOT bound to
the session's active organisation. They let a platform super-admin manage every
organisation's metadata, membership, and invitations. They deliberately grant
no access to org-scoped *data* (sources, jobs, runs, …).
"""

from __future__ import annotations

import datetime as dt
import inspect
import logging
from datetime import datetime
from importlib import metadata
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from interloper_db import Profile, Store
from interloper_db.store.quotas import METRIC_SUCCESSFUL_RUNS, QUOTAS
from pydantic import BaseModel, RootModel, field_validator

from interloper_api.dependencies import get_admin_config, get_quota_defaults, get_store, require_super_admin
from interloper_api.notifications import InvitationEmail

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])

_ROLES = {"viewer", "editor", "admin"}


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


class CreateOrganisationRequest(BaseModel):
    """Request body for creating an organisation."""

    name: str


class UpdateOrganisationRequest(BaseModel):
    """Request body for renaming an organisation."""

    name: str


class DeleteOrganisationRequest(BaseModel):
    """Confirmation body for deleting an organisation — must repeat its exact name."""

    name: str


class MemberResponse(BaseModel):
    """Organisation member."""

    id: UUID
    email: str
    name: str | None = None
    avatar_url: str | None = None
    role: str


class UpdateRoleRequest(BaseModel):
    """Request body for changing a member's role."""

    role: str


class JoinOrganisationRequest(BaseModel):
    """Request body for a super-admin joining an organisation."""

    role: str = "admin"


class InviteRequest(BaseModel):
    """Request body for inviting a user."""

    email: str
    role: str = "viewer"


class InvitationResponse(BaseModel):
    """Pending invitation."""

    id: UUID
    email: str
    role: str
    created_at: datetime | None = None
    expires_at: datetime


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
    """Reaper tuning."""

    enabled: bool
    timeout: int
    poll_interval: int


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
    """One event in an organisation's derived activity feed."""

    kind: str
    when: datetime
    title: str
    detail: str | None = None


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
        (auto-discovered universe + configured extras), not ``settings.catalog``,
        which only holds the explicitly configured import paths.

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
                    timeout=settings.reaper.timeout,
                    poll_interval=settings.reaper.poll_interval,
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
    return {
        key: override if (override := overrides.get(key)) is not None else defaults.get(key)
        for key in QUOTAS
    }


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


def _validate_role(role: str) -> str:
    """Validate a role string or raise 400.

    Args:
        role: The role name submitted by the caller.

    Returns:
        The role unchanged.

    Raises:
        HTTPException: 400 when the role is none of viewer, editor, admin.
    """
    if role not in _ROLES:
        raise HTTPException(status_code=400, detail=f"Invalid role: {role}")
    return role


def _send_invitation_email(
    request: Request,
    invitation: Any,
    org_name: str,
    inviter_name: str,
) -> None:
    """Send the invitation email if SMTP is configured, never failing the request.

    Args:
        request: The incoming request, whose base URL the invite link is built on.
        invitation: The stored invitation, read for its recipient and token.
        org_name: The organisation name shown to the recipient.
        inviter_name: The display name of the super-admin who invited.
    """
    from interloper_api.dependencies import get_smtp_config

    smtp_config = get_smtp_config()
    if not smtp_config or not smtp_config.enabled:
        logger.warning("SMTP not configured; invitation email to %s not sent", invitation.email)
        return

    token = invitation.token
    email = invitation.email
    base_url = str(request.base_url).rstrip("/")
    invite_url = f"{base_url}/invite/{token}"

    try:
        InvitationEmail(
            org_name=org_name,
            inviter_name=inviter_name,
            invite_url=invite_url,
        ).send(smtp_config, email)
    except Exception:
        logger.exception("Failed to send invitation email to %s", email)


# -- Instance config -----------------------------------------------------------


@router.get("/config")
def get_instance_config(
    user: Profile = Depends(require_super_admin),
    config: Any = Depends(get_admin_config),
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
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
    quota_defaults: Any = Depends(get_quota_defaults),
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
    period_start = store.quotas.current_period_start()
    overrides = store.quotas.list_overrides()
    usage = {
        row.org_id: row
        for row in store.quotas.list_usage(period_start=period_start)
        if row.metric == METRIC_SUCCESSFUL_RUNS
    }
    sources = store.quotas.count_sources_by_org()
    max_assets = store.quotas.max_assets_per_source_by_org()
    recomputed = store.quotas.count_successful_runs_by_org(period_start)

    organisations = []
    for org, _member_count in store.organisations.list_all():
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
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
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
    store.organisations.get(org_id)  # 404 before touching anything else
    overrides = store.quotas.set_overrides(org_id, body.root)
    return _quota_limits(overrides)


# -- Activity ------------------------------------------------------------------


def _activity_title(entry: dict[str, Any]) -> tuple[str, str | None]:
    """Presentation strings for a derived activity entry.

    Args:
        entry: One raw activity record, read for its ``kind``, ``subject`` and
            ``extra`` keys.

    Returns:
        The title and the detail line, the latter ``None`` when the entry
        carries nothing beyond its title.
    """
    kind, subject, extra = entry["kind"], entry["subject"], entry["extra"]
    if kind == "org_created":
        return "Organisation created", None
    if kind == "org_deleted":
        return "Organisation deleted", "Retained read-only for billing history."
    if kind == "member_joined":
        return f"{subject} joined the organisation", f"Role: {extra}" if extra else None
    if kind == "invitation_sent":
        return f"Invitation sent to {subject}", f"Invited by {extra}" if extra else None
    if kind == "source_added":
        return f"Source added — {subject}", None
    if kind == "runs_completed":
        count = int(subject)
        return f"{count:,} run{'' if count == 1 else 's'} completed successfully", None
    return kind, None


@router.get("/organisations/{org_id}/activity")
def get_organisation_activity(
    org_id: UUID,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> list[AdminActivityEntry]:
    """Derived activity feed for one organisation, newest first.

    Composed from existing records (memberships, pending invitations,
    sources, run aggregates, the org row itself) — there is no audit
    trail, so actor attribution is limited to what those rows carry.

    Args:
        org_id: The organisation whose activity is derived.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        The activity entries, newest first.
    """
    entries = []
    for entry in store.organisations.list_activity(org_id):
        title, detail = _activity_title(entry)
        entries.append(AdminActivityEntry(kind=entry["kind"], when=entry["when"], title=title, detail=detail))
    return entries


# -- Users ---------------------------------------------------------------------


@router.get("/users")
def list_all_users(
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> list[AdminUserResponse]:
    """List every user profile with the organisations it belongs to.

    Args:
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        Every profile on the platform, each with its memberships.
    """
    return [
        AdminUserResponse(
            id=profile.id,
            email=profile.email,
            name=profile.name,
            avatar_url=profile.avatar_url,
            is_super_admin=profile.is_super_admin,
            organisations=[AdminUserOrganisation(id=org.id, name=org.name) for org in orgs],
            created_at=profile.created_at,
        )
        for profile, orgs in store.auth.list_all_profiles()
    ]


@router.delete("/users/{user_id}")
def delete_user(
    user_id: UUID,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Delete a user entirely: profile, sessions, tokens, memberships, sent invitations.

    Args:
        user_id: The profile to delete.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        ``{"status": "ok"}`` once the profile is gone.

    Raises:
        HTTPException: 400 when the caller targets their own account.
    """
    if user_id == user.id:
        raise HTTPException(status_code=400, detail="You cannot delete your own account")
    store.auth.delete_profile(user_id)
    return {"status": "ok"}


# -- Organisations -------------------------------------------------------------


@router.get("/organisations")
def list_all_organisations(
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> list[AdminOrganisationResponse]:
    """List every organisation with its member count, soft-deleted ones included.

    Args:
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        Every organisation, each with its member count.
    """
    return [
        AdminOrganisationResponse(
            id=org.id,
            name=org.name,
            member_count=count,
            created_at=org.created_at,
            deleted_at=org.deleted_at,
        )
        for org, count in store.organisations.list_all()
    ]


@router.post("/organisations", status_code=201)
def create_organisation(
    body: CreateOrganisationRequest,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> AdminOrganisationResponse:
    """Create an organisation. The super-admin is not added as a member.

    Args:
        body: The new organisation's name.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        The created organisation, whose member count is therefore zero.
    """
    org = store.organisations.create(name=body.name)
    return AdminOrganisationResponse(
        id=org.id,
        name=org.name,
        member_count=0,
        created_at=org.created_at,
    )


@router.patch("/organisations/{org_id}")
def update_organisation(
    org_id: UUID,
    body: UpdateOrganisationRequest,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> AdminOrganisationResponse:
    """Rename an organisation.

    Args:
        org_id: The organisation to rename.
        body: The new name.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        The renamed organisation with its current member count.
    """
    org = store.organisations.update(org_id, body.name)
    members = store.organisations.list_members(org_id)
    return AdminOrganisationResponse(
        id=org.id,
        name=org.name,
        member_count=len(members),
        created_at=org.created_at,
    )


@router.delete("/organisations/{org_id}")
def delete_organisation(
    org_id: UUID,
    body: DeleteOrganisationRequest,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Soft-delete an organisation: purges its data, keeps execution history and the usage ledger.

    The body must repeat the exact name. Deleted organisations stay in the
    list (read-only) and read as missing everywhere else.

    Args:
        org_id: The organisation to soft-delete.
        body: The confirmation, repeating the organisation's exact name.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        ``{"status": "ok"}`` once the organisation is soft-deleted.

    Raises:
        HTTPException: 400 when the confirmation name does not match.
    """
    org = store.organisations.get(org_id)
    if body.name != org.name:
        raise HTTPException(status_code=400, detail="Organisation name does not match")
    store.organisations.delete(org_id)
    return {"status": "ok"}


# -- Members -------------------------------------------------------------------


@router.get("/organisations/{org_id}/members")
def list_members(
    org_id: UUID,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> list[MemberResponse]:
    """List all members of any organisation.

    Args:
        org_id: The organisation whose members are listed.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        Every member of the organisation with its role.
    """
    store.organisations.get(org_id)  # 404 before touching anything else
    members = store.organisations.list_members(org_id)
    return [
        MemberResponse(
            id=profile.id,
            email=profile.email,
            name=profile.name,
            avatar_url=profile.avatar_url,
            role=role,
        )
        for profile, role in members
    ]


@router.post("/organisations/{org_id}/members", status_code=201)
def join_organisation(
    org_id: UUID,
    body: JoinOrganisationRequest,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> MemberResponse:
    """Add the calling super-admin to any organisation — no invitation needed.

    Args:
        org_id: The organisation to join.
        body: The role to take, ``admin`` unless overridden.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        The caller's new membership.

    Raises:
        HTTPException: 409 when the caller already belongs to the organisation.
    """
    store.organisations.get(org_id)  # 404 before touching anything else
    _validate_role(body.role)
    if not store.organisations.add_member(org_id, user.id, body.role):
        raise HTTPException(status_code=409, detail="Already a member of this organisation")
    return MemberResponse(
        id=user.id,
        email=user.email,
        name=user.name,
        avatar_url=user.avatar_url,
        role=body.role,
    )


@router.patch("/organisations/{org_id}/members/{user_id}")
def update_member_role(
    org_id: UUID,
    user_id: UUID,
    body: UpdateRoleRequest,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Change a member's role in any organisation.

    Args:
        org_id: The organisation the membership belongs to.
        user_id: The member whose role changes.
        body: The new role.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        ``{"status": "ok"}`` once the role is written.
    """
    _validate_role(body.role)
    store.organisations.update_member_role(org_id, user_id, body.role)
    return {"status": "ok"}


@router.delete("/organisations/{org_id}/members/{user_id}")
def remove_member(
    org_id: UUID,
    user_id: UUID,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Remove a member from any organisation.

    Args:
        org_id: The organisation the membership belongs to.
        user_id: The member to remove.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        ``{"status": "ok"}`` once the membership is gone.
    """
    store.organisations.remove_member(org_id, user_id)
    return {"status": "ok"}


# -- Invitations ---------------------------------------------------------------


@router.get("/organisations/{org_id}/invitations")
def list_invitations(
    org_id: UUID,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> list[InvitationResponse]:
    """List pending invitations for any organisation.

    Args:
        org_id: The organisation whose invitations are listed.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        The organisation's pending invitations.
    """
    store.organisations.get(org_id)  # 404 before touching anything else
    return [
        InvitationResponse(
            id=inv.id,
            email=inv.email,
            role=inv.role,
            created_at=inv.created_at,
            expires_at=inv.expires_at,
        )
        for inv in store.organisations.list_invitations(org_id)
    ]


@router.post("/organisations/{org_id}/invitations", status_code=201)
def invite_member(
    org_id: UUID,
    body: InviteRequest,
    request: Request,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> InvitationResponse:
    """Invite a user to any organisation by email.

    The invitation is stored first and mailed best-effort: an unconfigured or
    failing SMTP is logged, never surfaced, so the link stays redeemable.

    Args:
        org_id: The organisation to invite into.
        body: The recipient email and the role granted on acceptance.
        request: The incoming request, whose base URL the invite link is built on.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        The created invitation.
    """
    org = store.organisations.get(org_id)
    _validate_role(body.role)
    invitation = store.organisations.create_invitation(
        org_id=org_id,
        email=body.email.strip(),
        role=body.role,
        invited_by=user.id,
    )

    inviter_name = user.name or user.email
    _send_invitation_email(request, invitation, org.name, inviter_name)

    return InvitationResponse(
        id=invitation.id,
        email=invitation.email,
        role=invitation.role,
        created_at=invitation.created_at,
        expires_at=invitation.expires_at,
    )


@router.delete("/organisations/{org_id}/invitations/{invitation_id}")
def cancel_invitation(
    org_id: UUID,
    invitation_id: UUID,
    user: Profile = Depends(require_super_admin),
    store: Store = Depends(get_store),
) -> dict[str, str]:
    """Cancel a pending invitation in any organisation.

    Args:
        org_id: The organisation the invitation belongs to; it scopes the path
            only, the invitation id alone identifying the row.
        invitation_id: The invitation to cancel.
        user: The calling super-admin, resolved from the session.
        store: The database store backing the request.

    Returns:
        ``{"status": "ok"}`` once the invitation is gone.
    """
    store.organisations.delete_invitation(invitation_id)
    return {"status": "ok"}
