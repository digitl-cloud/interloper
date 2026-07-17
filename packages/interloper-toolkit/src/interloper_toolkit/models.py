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
module, and embedding makes the coupling visible in the signature. The one
exception is :class:`ComponentSummary`, where projecting is the point —
sensitive kinds must never expose config or credential material, so the
model's fields fail closed instead of relying on conditional key insertion.

Catalog definition payloads stay ``dict[str, Any]``: their content is
definition-specific JSON schema material with no fixed shape to type.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from interloper_db.models import AssetExecution, Backfill, Component, Event, Run
from pydantic import BaseModel


class ToolError(BaseModel):
    """A failed tool call, as a structured result rather than an exception."""

    status: Literal["error"] = "error"
    error: str
    valid_kinds: list[str] | None = None


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
    """Fields across all asset schemas matching a query."""

    status: Literal["success"] = "success"
    query: str
    match_count: int
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
    """The org's component instances of one kind."""

    status: Literal["success"] = "success"
    kind: str
    count: int
    components: list[ComponentSummary]


# -- Lineage --------------------------------------------------------------------


class DependencyEdge(BaseModel):
    """A direct dependency edge from the perspective of one asset."""

    asset_id: str
    param_name: str
    asset_key: str
    source_id: str


class UpstreamResult(BaseModel):
    """Direct upstream dependencies of an asset."""

    status: Literal["success"] = "success"
    asset_id: str
    upstream: list[DependencyEdge]


class DownstreamResult(BaseModel):
    """Direct downstream dependents of an asset."""

    status: Literal["success"] = "success"
    asset_id: str
    downstream: list[DependencyEdge]


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


class JobList(BaseModel):
    """The org's scheduled jobs (full component rows)."""

    status: Literal["success"] = "success"
    count: int
    jobs: list[Component]


class JobHealthStats(BaseModel):
    """Success/failure statistics over a job's recent runs."""

    total_recent_runs: int
    success_count: int
    failed_count: int
    success_rate: float | None = None
    avg_duration_seconds: float | None = None


class JobHealth(BaseModel):
    """A job's metadata plus health computed from its last runs."""

    status: Literal["success"] = "success"
    job: Component
    health: JobHealthStats


class RunList(BaseModel):
    """Recent runs matching the filters."""

    status: Literal["success"] = "success"
    count: int
    runs: list[Run]


class RunDetail(BaseModel):
    """One run with its event timeline and per-asset execution summary."""

    status: Literal["success"] = "success"
    run: Run
    events: list[Event]
    asset_executions: list[AssetExecution]


class RunErrorEvent(BaseModel):
    """One error event of a failed run."""

    asset_key: str | None = None
    error: str
    timestamp: datetime


class RunFailure(BaseModel):
    """A failed run with its error events."""

    run: Run
    errors: list[RunErrorEvent]


class FailureList(BaseModel):
    """Recent failed runs with their errors."""

    status: Literal["success"] = "success"
    count: int
    failures: list[RunFailure]


class BackfillList(BaseModel):
    """Backfills, optionally only the active ones."""

    status: Literal["success"] = "success"
    count: int
    backfills: list[Backfill]


# -- Analytics ------------------------------------------------------------------


class RunHistorySummary(BaseModel):
    """Aggregate run statistics over a look-back period."""

    status: Literal["success"] = "success"
    period_days: int
    component_id: str | None = None
    total_runs: int
    by_status: dict[str, int]
    success_rate: float | None = None
    avg_duration_seconds: float | None = None


class PartitionCoverage(BaseModel):
    """Which partition dates in a range have successful runs."""

    status: Literal["success"] = "success"
    component_id: str
    start_date: str
    end_date: str
    total_days: int
    covered_days: int
    missing_days: int
    coverage_percent: float
    missing_dates: list[str]


class JobFreshness(BaseModel):
    """One job's data freshness."""

    job: Component
    last_success_at: datetime | None = None
    hours_since_success: float | None = None
    stale: bool


class FreshnessReport(BaseModel):
    """Freshness across all enabled jobs."""

    status: Literal["success"] = "success"
    total_jobs: int
    stale_count: int
    jobs: list[JobFreshness]
