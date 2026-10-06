"""Typed result models for the toolkit surface.

Every toolkit function returns ``<SuccessModel> | ToolError`` — a union
discriminated by the literal ``status`` field, preserving the
``{"status": "success" | "error"}`` envelope both AI surfaces already
speak. The models are the tool contract: MCP derives per-tool output
schemas from them, and the fields of row-projecting models act as an
allowlist of what leaves the platform.

Row-shaped payloads deliberately embed the interloper-db models (``Run``,
``Backfill``, ``Event``, ``Component`` — already pydantic via SQLModel)
rather than duplicating their shape: the full-row contract predates this
module, and embedding makes the coupling visible in the signature. The exceptions
project on purpose: :class:`ComponentSummary`, because sensitive kinds must
never expose config or credential material, so the model's fields fail
closed instead of relying on conditional key insertion; and
:class:`EventRecord`, because a page of tracebacks would not fit a tool
response.

Catalog definition payloads stay ``dict[str, Any]``: their content is
definition-specific JSON schema material with no fixed shape to type.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from interloper_db.models import Backfill, Component, Event, Execution, Run
from interloper_db.store.insights import AssetKeyCoverage, ErrorCause, JobCoverage, JobHealth, JobOutcome
from pydantic import BaseModel

_MISSING_RANGES_LIMIT = 20


class ToolError(BaseModel):
    """A failed tool call, as a structured result rather than an exception.

    ``valid_values`` lists the accepted values when an argument named an
    unknown one; ``category`` classes a provider failure (``config``,
    ``auth``, ``network``, ``error``).
    """

    status: Literal["error"] = "error"
    error: str
    valid_values: list[str] | None = None
    category: str | None = None


# -- Catalog --------------------------------------------------------------------


class DefinitionCounts(BaseModel):
    """Per-kind definition counts (the kind-less ``list_definitions`` call)."""

    status: Literal["success"] = "success"
    definition_counts: dict[str, int]
    message: str


class DefinitionEntry(BaseModel):
    """One catalog definition, summarized for listing.

    The trailing optional fields are kind-specific: ``asset_count`` /
    ``in_collection`` / ``collection_count`` are populated for sources,
    ``provider`` / ``required_fields`` / ``oauth`` / ``oauth_available``
    for connections.
    """

    key: str
    name: str
    description: str | None = None
    icon: str | None = None
    tags: list[str] = []
    maturity: str = "stable"
    asset_count: int | None = None
    in_collection: bool | None = None
    collection_count: int | None = None
    provider: str | None = None
    required_fields: list[str] | None = None
    oauth: bool | None = None
    oauth_available: bool | None = None


class DefinitionList(BaseModel):
    """Catalog definitions of one kind."""

    status: Literal["success"] = "success"
    kind: str
    count: int
    definitions: list[DefinitionEntry]


class DefinitionDetail(BaseModel):
    """One definition's full catalog payload (definition-specific shape)."""

    status: Literal["success"] = "success"
    definition: dict[str, Any]


class AssetSchemaResult(BaseModel):
    """The JSON schema and metadata of one asset within a source."""

    status: Literal["success"] = "success"
    source_key: str
    asset_key: str
    qualified_key: str
    asset_schema: dict[str, Any] | None = None
    partitioning: Any = None
    tags: list[str] = []
    requires: dict[str, Any] = {}
    optional_requires: dict[str, Any] = {}


class FieldMatch(BaseModel):
    """One schema field matching a search query."""

    source_key: str
    asset_key: str
    qualified_key: str
    field_name: str
    field_type: str
    description: str


class FieldSearchResult(BaseModel):
    """One page of the fields across all asset schemas matching a query."""

    status: Literal["success"] = "success"
    query: str
    match_count: int
    total: int
    matches: list[FieldMatch]


class SharedField(BaseModel):
    """A field present in both compared schemas, with type agreement."""

    field: str
    type_a: str
    type_b: str
    type_match: bool


class SchemaComparison(BaseModel):
    """Side-by-side comparison of two asset schemas."""

    status: Literal["success"] = "success"
    asset_a: str
    asset_b: str
    shared_count: int
    only_a_count: int
    only_b_count: int
    shared_fields: list[SharedField]
    only_in_a: list[str]
    only_in_b: list[str]


# -- Collection -----------------------------------------------------------------


class ComponentCounts(BaseModel):
    """Per-kind component counts (the kind-less ``list_components`` call)."""

    status: Literal["success"] = "success"
    component_counts: dict[str, int]
    message: str


class ComponentSummary(BaseModel):
    """A component instance, projected for listing.

    Deliberately not the db row: these fields are an allowlist. ``config``
    is only populated for non-sensitive kinds; credential-bearing kinds
    surface identity and metadata alone.
    """

    id: str
    key: str
    name: str | None = None
    type_name: str
    created_at: datetime | None = None
    config: dict[str, Any] | None = None
    asset_count: int | None = None


class ComponentList(BaseModel):
    """One page of the org's component instances of one kind."""

    status: Literal["success"] = "success"
    kind: str
    count: int
    total: int
    components: list[ComponentSummary]


class ComponentRef(BaseModel):
    """A component's identity, as a write reports what it touched."""

    id: UUID
    kind: str | None = None
    key: str | None = None
    name: str | None = None


class ComponentUpdated(BaseModel):
    """One component edited by ``update_component``."""

    status: Literal["success"] = "success"
    message: str
    component: ComponentRef
    asset_count: int | None = None
    changed_fields: list[str] | None = None
    unresolved_requirements: list[str] | None = None


class FailedInstance(BaseModel):
    """One instance a batch creation refused, and why."""

    name: str
    value: str | None = None
    error: str


class ConnectionsCreated(BaseModel):
    """The outcome of ``create_connections``, instance by instance."""

    status: Literal["success"] = "success"
    message: str
    created: list[ComponentRef]
    failed: list[FailedInstance]


class UserRequest(BaseModel):
    """A result that asks the user for something the model cannot supply.

    An interactive surface takes it to the user and answers the call with
    their input; elsewhere it is an ordinary result the model relays.
    """


class ConnectionSetup(UserRequest):
    """The hand-off ``request_connection_setup`` makes.

    ``setup_url`` opens the app's connection form for this definition, when
    the deployment knows its public URL.
    """

    status: Literal["success"] = "success"
    message: str
    connection_key: str
    name: str | None = None
    oauth: bool
    oauth_available: bool
    setup_url: str | None = None


class ConnectionCheck(BaseModel):
    """The outcome of a connection's health check.

    ``live`` is false when the type implements no check and only hydration
    was verified; ``category`` classes a failure (``config``, ``auth``,
    ``network``, ``error``).
    """

    status: Literal["success"] = "success"
    connection: ComponentRef
    ok: bool
    live: bool
    category: str | None = None
    message: str | None = None


class BindResult(BaseModel):
    """One relation edge created or repointed by ``bind_relation``."""

    status: Literal["success"] = "success"
    src_id: str
    name: str
    dst_id: str
    dst_kind: str


class UnbindResult(BaseModel):
    """One relation edge removed by ``unbind_relation``."""

    status: Literal["success"] = "success"
    src_id: str
    name: str
    dst_id: str


# -- Sources and jobs -----------------------------------------------------------


class FieldOption(BaseModel):
    """One live option of a provider-backed config field."""

    label: str | None = None
    value: Any = None


class FieldOptions(BaseModel):
    """The live options of a source's provider-backed config field."""

    status: Literal["success"] = "success"
    source_key: str
    field: str
    total: int
    returned: int
    options: list[FieldOption]


class SourceCreated(BaseModel):
    """One source created by ``create_source``."""

    status: Literal["success"] = "success"
    message: str
    source: ComponentRef
    asset_count: int
    connection_bound: bool
    destination_count: int
    unresolved_requirements: list[str]


class CreatedInstance(BaseModel):
    """One source a batch creation made, and the account value it got."""

    id: UUID
    name: str | None = None
    value: str


class SourcesCreated(BaseModel):
    """The outcome of ``create_sources``, instance by instance."""

    status: Literal["success"] = "success"
    message: str
    field: str
    created: list[CreatedInstance]
    failed: list[FailedInstance]
    unresolved_requirements: list[str]


class JobCreated(BaseModel):
    """One cron job created by ``create_job``."""

    status: Literal["success"] = "success"
    message: str
    job: ComponentRef
    cron: str
    enabled: bool
    target_count: int


# -- Lineage --------------------------------------------------------------------


class RelationEdge(BaseModel):
    """A direct asset-to-asset relation edge from the perspective of one asset."""

    asset_id: str
    param_name: str
    asset_key: str
    source_id: str


class UpstreamResult(BaseModel):
    """Direct upstream dependencies of an asset."""

    status: Literal["success"] = "success"
    asset_id: str
    upstream: list[RelationEdge]


class DownstreamResult(BaseModel):
    """Direct downstream dependents of an asset."""

    status: Literal["success"] = "success"
    asset_id: str
    downstream: list[RelationEdge]


class LineageItem(BaseModel):
    """One asset in a lineage traversal, with its BFS depth."""

    asset_id: str
    depth: int
    asset_key: str | None = None
    source_id: str | None = None
    source_key: str | None = None


class LineageResult(BaseModel):
    """The full recursive lineage of an asset in one direction."""

    status: Literal["success"] = "success"
    asset_id: str
    direction: str
    lineage_count: int
    lineage: list[LineageItem]


class ImpactAnalysis(BaseModel):
    """All downstream assets affected by a failure, grouped by source."""

    status: Literal["success"] = "success"
    asset_id: str
    total_affected: int
    by_source: dict[str, list[LineageItem]]


class AssetRef(BaseModel):
    """A minimal asset reference on a cross-source edge."""

    asset_key: str | None = None
    source_id: str | None = None


class CrossSourceEdge(BaseModel):
    """A dependency edge whose endpoints belong to different sources."""

    downstream_asset_id: str
    downstream: AssetRef
    upstream_asset_id: str
    upstream: AssetRef
    param_name: str


class CrossSourceDependencies(BaseModel):
    """All dependency edges crossing source boundaries."""

    status: Literal["success"] = "success"
    cross_source_count: int
    dependencies: list[CrossSourceEdge]


# -- Scheduling -----------------------------------------------------------------


class ComponentToggled(BaseModel):
    """A job or asset switched on or off."""

    status: Literal["success"] = "success"
    message: str
    component: ComponentRef
    enabled: bool


class RunQueued(BaseModel):
    """One run queued by ``trigger_run``."""

    status: Literal["success"] = "success"
    message: str
    run: Run


class BackfillQueued(BaseModel):
    """One backfill queued by ``trigger_backfill``."""

    status: Literal["success"] = "success"
    message: str
    backfill: Backfill


class BackfillCanceled(BaseModel):
    """One backfill canceled by ``cancel_backfill``.

    ``runs_canceled`` counts the backfill's canceled runs, executing ones
    included: those stop on their next heartbeat.
    """

    status: Literal["success"] = "success"
    message: str
    backfill: Backfill
    runs_canceled: int


class RunCanceled(BaseModel):
    """The run ``cancel_run`` canceled."""

    status: Literal["success"] = "success"
    message: str
    run: Run


class RunRetried(BaseModel):
    """The new attempt ``retry_run`` queued."""

    status: Literal["success"] = "success"
    message: str
    run: Run


class JobList(BaseModel):
    """One page of the org's scheduled jobs (full component rows)."""

    status: Literal["success"] = "success"
    count: int
    total: int
    jobs: list[Component]


class RunList(BaseModel):
    """One page of the runs matching the filters."""

    status: Literal["success"] = "success"
    count: int
    total: int
    runs: list[Run]


class RunDetail(BaseModel):
    """One run with its per-operation execution summary."""

    status: Literal["success"] = "success"
    run: Run
    executions: list[Execution]


class EventRecord(BaseModel):
    """An event row without its traceback, which only ``get_event`` carries.

    A traceback runs to tens of kilobytes, so a page of them would not fit a
    tool response; every other column is the row's.
    """

    id: UUID
    run_id: UUID | None = None
    event_type: str
    timestamp: datetime
    component_id: UUID | None = None
    component_kind: str | None = None
    component_key: str | None = None
    level: str | None = None
    message: str | None = None
    error: str | None = None
    data: dict[str, Any] | None = None


class EventList(BaseModel):
    """One page of a run's events, oldest first."""

    status: Literal["success"] = "success"
    run_id: str
    count: int
    total: int
    events: list[EventRecord]


class EventDetail(BaseModel):
    """One event in full, its error and traceback clipped to a readable size."""

    status: Literal["success"] = "success"
    event: Event


class RunErrorEvent(BaseModel):
    """One error event of a failed run, its text clipped; ``get_event`` has it whole."""

    event_id: UUID
    component_key: str | None = None
    error: str
    timestamp: datetime


class RunFailure(BaseModel):
    """A failed run with its error events.

    ``error_count`` is the run's whole tally; ``errors`` holds the first
    page of them.
    """

    run: Run
    error_count: int
    errors: list[RunErrorEvent]


class FailureList(BaseModel):
    """One page of the failed runs, newest first, with their errors."""

    status: Literal["success"] = "success"
    count: int
    total: int
    failures: list[RunFailure]


class BackfillList(BaseModel):
    """One page of the backfills, newest first, optionally only the active ones."""

    status: Literal["success"] = "success"
    count: int
    total: int
    backfills: list[Backfill]


class Scan(BaseModel):
    """How much an aggregate read, and whether its cap cut the read short."""

    rows: int
    truncated: bool


class ErrorGroupRow(BaseModel):
    """Failures sharing one grouping key over the window.

    ``failed_attempts`` counts every attempt that failed, retried ones
    included; ``terminal_failures`` only those that were the operation's or
    run's final word.
    """

    job_id: UUID | None = None
    job_name: str | None = None
    asset_key: str | None = None
    cause: ErrorCause | None = None
    failed_attempts: int
    terminal_failures: int
    runs_affected: int
    first_seen: datetime
    last_seen: datetime
    sample_run_id: UUID
    sample: str


class ErrorBreakdown(BaseModel):
    """One page of error groups over a window, loudest first."""

    status: Literal["success"] = "success"
    since: datetime | None = None
    until: datetime | None = None
    group_by: list[str]
    count: int
    total: int
    scan: Scan
    groups: list[ErrorGroupRow]


class AttemptTiming(BaseModel):
    """One run attempt of a backfill, with its timing."""

    run_id: UUID
    partition_key: str | None = None
    status: str
    attempt: int
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_s: float | None = None
    queue_wait_s: float | None = None


class BackfillTimeline(BaseModel):
    """A backfill's declared settings next to what its runs actually did.

    ``max_concurrent_runs`` is the peak number of attempts running at one
    instant, to compare with the backfill's declared ``concurrency``.
    """

    status: Literal["success"] = "success"
    backfill: Backfill
    max_concurrent_runs: int
    first_start_lag_s: float | None = None
    duration_p50_s: float | None = None
    duration_p90_s: float | None = None
    runs_by_status: dict[str, int]
    count: int
    total: int
    attempts: list[AttemptTiming]


# -- Analytics ------------------------------------------------------------------


class RunStats(BaseModel):
    """One page of per-job run statistics over a window, most failures first."""

    status: Literal["success"] = "success"
    since: datetime | None = None
    until: datetime | None = None
    count: int
    total: int
    jobs: list[JobOutcome]


class JobHealthRow(BaseModel):
    """One job's state and its next firing.

    A job is ``failing`` when it is enabled and its latest attempt failed, and
    ``overdue`` when its scheduled slot passed more than 15 minutes ago
    without the scheduler firing it. ``next_start_key``/``next_end_key``
    bound the partitions the next firing covers.
    """

    job_id: UUID
    job_name: str | None = None
    enabled: bool
    failing: bool
    overdue: bool
    latest_run_id: UUID | None = None
    latest_status: str | None = None
    last_success_at: datetime | None = None
    next_run_at: datetime | None = None
    next_start_key: str | None = None
    next_end_key: str | None = None

    @classmethod
    def from_health(cls, health: JobHealth) -> JobHealthRow:
        """Project a job's health.

        Args:
            health: The job's health.

        Returns:
            The row.
        """
        window = health.window
        return cls(
            job_id=health.job.id,
            job_name=health.job.name,
            enabled=health.job.enabled,
            failing=health.failing,
            overdue=health.overdue,
            latest_run_id=health.latest_run.id if health.latest_run else None,
            latest_status=health.latest_run.status if health.latest_run else None,
            last_success_at=health.last_success_at,
            next_run_at=health.next_run_at,
            next_start_key=window.granularity.format(window.start) if window else None,
            next_end_key=window.granularity.format(window.end) if window else None,
        )


class JobHealthReport(BaseModel):
    """One page of the jobs' health, failing then overdue jobs first."""

    status: Literal["success"] = "success"
    failing: int
    overdue: int
    count: int
    total: int
    jobs: list[JobHealthRow]


class PartitionRange(BaseModel):
    """An inclusive range of partition keys, usable as backfill bounds."""

    start_key: str
    end_key: str

    @classmethod
    def from_keys(cls, missing: list[str], keys: list[str]) -> list[PartitionRange]:
        """Collapse keys into runs of consecutive partitions.

        Args:
            missing: The keys to collapse, in *keys*' order.
            keys: Every key of the range, in order.

        Returns:
            One inclusive range per run of consecutive keys.
        """
        position = {key: index for index, key in enumerate(keys)}
        ranges: list[PartitionRange] = []
        for key in missing:
            if ranges and position[key] == position[ranges[-1].end_key] + 1:
                ranges[-1].end_key = key
            else:
                ranges.append(cls(start_key=key, end_key=key))
        return ranges


class AssetCoverageRow(BaseModel):
    """One asset's partition coverage over a range.

    A partition is ``covered`` once any run's execution of the asset
    succeeded, ``failed`` when executions of it only failed, and
    ``never_run`` when no run executed the asset for it. ``missing`` lists
    the uncovered partitions as ranges, at most 20 of them.
    """

    asset_id: UUID
    asset_key: str | None = None
    covered: int
    failed: int
    never_run: int
    missing: list[PartitionRange]
    missing_ranges_total: int

    @classmethod
    def from_coverage(cls, asset: AssetKeyCoverage, keys: list[str]) -> AssetCoverageRow:
        """Count an asset's keys by state and range its uncovered ones.

        Args:
            asset: The asset's covered and failed keys.
            keys: Every key of the range, in order.

        Returns:
            The row.
        """
        ranges = PartitionRange.from_keys([key for key in keys if key not in asset.covered], keys)
        return cls(
            asset_id=asset.asset_id,
            asset_key=asset.asset_key,
            covered=len(asset.covered),
            failed=len(asset.failed),
            never_run=len(keys) - len(asset.covered) - len(asset.failed),
            missing=ranges[:_MISSING_RANGES_LIMIT],
            missing_ranges_total=len(ranges),
        )


class AssetCoverage(BaseModel):
    """Per-asset partition coverage of a job over a range, least covered first.

    The rollup counts the range's partitions by how many of the job's assets
    are covered for them: all, some, or none.
    """

    status: Literal["success"] = "success"
    component_id: UUID
    start_key: str
    end_key: str
    partitions: int
    all_covered: int
    partly_covered: int
    none_covered: int
    count: int
    total: int
    assets: list[AssetCoverageRow]

    @classmethod
    def from_coverage(cls, coverage: JobCoverage, *, limit: int, offset: int) -> AssetCoverage:
        """Page a job's coverage, least covered asset first, and roll its partitions up.

        Args:
            coverage: The job's coverage over the range.
            limit: Maximum number of assets on the page.
            offset: Number of assets to skip.

        Returns:
            The page, with the rollup over every asset.
        """
        keys = coverage.keys
        assets = sorted(
            (AssetCoverageRow.from_coverage(asset, keys) for asset in coverage.assets),
            key=lambda row: (row.covered, row.asset_key or ""),
        )
        per_key = [sum(key in asset.covered for asset in coverage.assets) for key in keys]
        everyone = len(coverage.assets)
        page = assets[offset : offset + limit]
        return cls(
            component_id=coverage.job_id,
            start_key=keys[0],
            end_key=keys[-1],
            partitions=len(keys),
            all_covered=sum(count == everyone for count in per_key) if everyone else 0,
            partly_covered=sum(0 < count < everyone for count in per_key),
            none_covered=sum(count == 0 for count in per_key),
            count=len(page),
            total=len(assets),
            assets=page,
        )
