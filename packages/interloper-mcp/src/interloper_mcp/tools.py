"""MCP tool registration — thin wrappers over the shared toolkit.

Every wrapper is one delegation line; the implementations live in
``interloper_toolkit`` (shared with the ADK agent), and their LLM-facing
docstrings are adopted as the tool descriptions. FastMCP derives each tool's
input schema from the wrapper's signature and its output schema from the
typed ``<SuccessModel> | ToolError`` return annotation.

Each tool carries the MCP annotations a client keys its behaviour on
(``readOnlyHint``, ``destructiveHint``, ``idempotentHint``,
``openWorldHint``); the spec's defaults are the pessimistic ones, so reads
say so explicitly. The writes are gated in the toolkit on the token's role;
the annotations only tell the client when to ask the user first.
``create_connections`` is not registered: its arguments would carry
credentials through the client.
"""

from __future__ import annotations

from interloper_toolkit import analytics, catalog, collection, jobs, lineage, scheduling, sources
from interloper_toolkit import models as m
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from interloper_mcp.context import get_ctx

READ = ToolAnnotations(readOnlyHint=True, openWorldHint=False)
READ_PROVIDER = ToolAnnotations(readOnlyHint=True, openWorldHint=True)
EDIT = ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False)
CREATE = ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=False)
LAUNCH = ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=True)
CANCEL = ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=True, openWorldHint=False)


def register_tools(mcp: FastMCP) -> None:
    """Register the interloper tools on the server.

    Args:
        mcp: The FastMCP server instance.
    """
    # -- Catalog ----------------------------------------------------------

    @mcp.tool(description=catalog.list_definitions.__doc__, annotations=READ)
    def list_definitions(kind: str | None = None) -> m.DefinitionCounts | m.DefinitionList | m.ToolError:
        return catalog.list_definitions(get_ctx(), kind)

    @mcp.tool(description=catalog.get_definition.__doc__, annotations=READ)
    def get_definition(key: str) -> m.DefinitionDetail | m.ToolError:
        return catalog.get_definition(get_ctx(), key)

    @mcp.tool(description=catalog.get_asset_schema.__doc__, annotations=READ)
    def get_asset_schema(source_key: str, asset_key: str) -> m.AssetSchemaResult | m.ToolError:
        return catalog.get_asset_schema(get_ctx(), source_key, asset_key)

    @mcp.tool(description=catalog.search_fields.__doc__, annotations=READ)
    def search_fields(query: str, limit: int = 50, offset: int = 0) -> m.FieldSearchResult | m.ToolError:
        return catalog.search_fields(get_ctx(), query, limit, offset)

    @mcp.tool(description=catalog.compare_schemas.__doc__, annotations=READ)
    def compare_schemas(
        source_key_a: str,
        asset_key_a: str,
        source_key_b: str,
        asset_key_b: str,
    ) -> m.SchemaComparison | m.ToolError:
        return catalog.compare_schemas(get_ctx(), source_key_a, asset_key_a, source_key_b, asset_key_b)

    # -- Collection -------------------------------------------------------

    @mcp.tool(description=collection.list_components.__doc__, annotations=READ)
    def list_components(
        kind: str | None = None, q: str | None = None, limit: int = 50, offset: int = 0
    ) -> m.ComponentCounts | m.ComponentList | m.ToolError:
        return collection.list_components(get_ctx(), kind, q, limit, offset)

    @mcp.tool(description=collection.update_component.__doc__, annotations=EDIT)
    def update_component(
        component_id: str,
        name: str | None = None,
        config_updates: dict | None = None,
        asset_keys: list[str] | None = None,
    ) -> m.ComponentUpdated | m.ToolError:
        return collection.update_component(get_ctx(), component_id, name, config_updates, asset_keys)

    @mcp.tool(description=collection.bind_relation.__doc__, annotations=EDIT)
    def bind_relation(component_id: str, name: str, dst_id: str) -> m.BindResult | m.ToolError:
        return collection.bind_relation(get_ctx(), component_id, name, dst_id)

    @mcp.tool(description=collection.unbind_relation.__doc__, annotations=EDIT)
    def unbind_relation(component_id: str, name: str, dst_id: str) -> m.UnbindResult | m.ToolError:
        return collection.unbind_relation(get_ctx(), component_id, name, dst_id)

    @mcp.tool(description=collection.request_connection_setup.__doc__, annotations=READ)
    def request_connection_setup(
        connection_key: str, name: str | None = None, force_new: bool = False
    ) -> m.ConnectionSetup | m.ToolError:
        return collection.request_connection_setup(get_ctx(), connection_key, name, force_new)

    @mcp.tool(description=collection.check_connection.__doc__, annotations=READ_PROVIDER)
    async def check_connection(connection_id: str) -> m.ConnectionCheck | m.ToolError:
        return await collection.check_connection(get_ctx(), connection_id)

    # -- Sources and jobs -------------------------------------------------

    @mcp.tool(description=sources.resolve_source_field_options.__doc__, annotations=READ_PROVIDER)
    async def resolve_source_field_options(
        source_key: str, connection_id: str, field: str | None = None
    ) -> m.FieldOptions | m.ToolError:
        return await sources.resolve_source_field_options(get_ctx(), source_key, connection_id, field)

    @mcp.tool(description=sources.create_source.__doc__, annotations=CREATE)
    def create_source(
        source_key: str,
        name: str,
        config: dict,
        connection_id: str | None = None,
        asset_keys: list[str] | None = None,
        destination_ids: list[str] | None = None,
    ) -> m.SourceCreated | m.ToolError:
        return sources.create_source(get_ctx(), source_key, name, config, connection_id, asset_keys, destination_ids)

    @mcp.tool(description=sources.create_sources.__doc__, annotations=CREATE)
    def create_sources(
        source_key: str,
        instances: list[dict],
        connection_id: str | None = None,
        asset_keys: list[str] | None = None,
        shared_config: dict | None = None,
        field: str | None = None,
        destination_ids: list[str] | None = None,
    ) -> m.SourcesCreated | m.ToolError:
        return sources.create_sources(
            get_ctx(), source_key, instances, connection_id, asset_keys, shared_config, field, destination_ids
        )

    @mcp.tool(description=jobs.create_job.__doc__, annotations=CREATE)
    def create_job(
        name: str, cron: str, target_source_ids: list[str], lookback: int | None = 1, offset: int = 1
    ) -> m.JobCreated | m.ToolError:
        return jobs.create_job(get_ctx(), name, cron, target_source_ids, lookback, offset)

    # -- Lineage ----------------------------------------------------------

    @mcp.tool(description=lineage.get_upstream.__doc__, annotations=READ)
    def get_upstream(asset_id: str) -> m.UpstreamResult | m.ToolError:
        return lineage.get_upstream(get_ctx(), asset_id)

    @mcp.tool(description=lineage.get_downstream.__doc__, annotations=READ)
    def get_downstream(asset_id: str) -> m.DownstreamResult | m.ToolError:
        return lineage.get_downstream(get_ctx(), asset_id)

    @mcp.tool(description=lineage.get_full_lineage.__doc__, annotations=READ)
    def get_full_lineage(asset_id: str, direction: str = "upstream") -> m.LineageResult | m.ToolError:
        return lineage.get_full_lineage(get_ctx(), asset_id, direction)

    @mcp.tool(description=lineage.impact_analysis.__doc__, annotations=READ)
    def impact_analysis(asset_id: str) -> m.ImpactAnalysis | m.ToolError:
        return lineage.impact_analysis(get_ctx(), asset_id)

    @mcp.tool(description=lineage.cross_source_dependencies.__doc__, annotations=READ)
    def cross_source_dependencies() -> m.CrossSourceDependencies | m.ToolError:
        return lineage.cross_source_dependencies(get_ctx())

    # -- Scheduling -------------------------------------------------------

    @mcp.tool(description=scheduling.list_jobs.__doc__, annotations=READ)
    def list_jobs(limit: int = 50, offset: int = 0) -> m.JobList | m.ToolError:
        return scheduling.list_jobs(get_ctx(), limit, offset)

    @mcp.tool(description=scheduling.get_job_health.__doc__, annotations=READ)
    def get_job_health(component_id: str) -> m.JobHealth | m.ToolError:
        return scheduling.get_job_health(get_ctx(), component_id)

    @mcp.tool(description=scheduling.toggle_job.__doc__, annotations=EDIT)
    def toggle_job(component_id: str, enabled: bool) -> m.ComponentToggled | m.ToolError:
        return scheduling.toggle_job(get_ctx(), component_id, enabled)

    @mcp.tool(description=scheduling.toggle_asset.__doc__, annotations=EDIT)
    def toggle_asset(asset_id: str, enabled: bool) -> m.ComponentToggled | m.ToolError:
        return scheduling.toggle_asset(get_ctx(), asset_id, enabled)

    @mcp.tool(description=scheduling.list_recent_runs.__doc__, annotations=READ)
    def list_recent_runs(
        component_id: str | None = None,
        status: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> m.RunList | m.ToolError:
        return scheduling.list_recent_runs(get_ctx(), component_id, status, limit, offset)

    @mcp.tool(description=scheduling.get_run_detail.__doc__, annotations=READ)
    def get_run_detail(run_id: str) -> m.RunDetail | m.ToolError:
        return scheduling.get_run_detail(get_ctx(), run_id)

    @mcp.tool(description=scheduling.list_run_events.__doc__, annotations=READ)
    def list_run_events(
        run_id: str,
        component_id: str | None = None,
        event_types: list[str] | None = None,
        errors_only: bool = False,
        limit: int = 50,
        offset: int = 0,
    ) -> m.EventList | m.ToolError:
        return scheduling.list_run_events(get_ctx(), run_id, component_id, event_types, errors_only, limit, offset)

    @mcp.tool(description=scheduling.get_event.__doc__, annotations=READ)
    def get_event(event_id: str) -> m.EventDetail | m.ToolError:
        return scheduling.get_event(get_ctx(), event_id)

    @mcp.tool(description=scheduling.list_failures.__doc__, annotations=READ)
    def list_failures(limit: int = 20, offset: int = 0) -> m.FailureList | m.ToolError:
        return scheduling.list_failures(get_ctx(), limit, offset)

    @mcp.tool(description=scheduling.error_breakdown.__doc__, annotations=READ)
    def error_breakdown(
        since: str | None = None,
        until: str | None = None,
        component_id: str | None = None,
        backfill_id: str | None = None,
        run_id: str | None = None,
        group_by: list[str] | None = None,
        limit: int = 25,
        offset: int = 0,
    ) -> m.ErrorBreakdown | m.ToolError:
        return scheduling.error_breakdown(
            get_ctx(), since, until, component_id, backfill_id, run_id, group_by, limit, offset
        )

    @mcp.tool(description=scheduling.trigger_run.__doc__, annotations=LAUNCH)
    def trigger_run(component_id: str, partition_key: str | None = None) -> m.RunQueued | m.ToolError:
        return scheduling.trigger_run(get_ctx(), component_id, partition_key)

    @mcp.tool(description=scheduling.retry_run.__doc__, annotations=LAUNCH)
    def retry_run(run_id: str, scope: str = "all") -> m.RunRetried | m.ToolError:
        return scheduling.retry_run(get_ctx(), run_id, scope)

    @mcp.tool(description=scheduling.list_backfills.__doc__, annotations=READ)
    def list_backfills(active_only: bool = True, limit: int = 20, offset: int = 0) -> m.BackfillList | m.ToolError:
        return scheduling.list_backfills(get_ctx(), active_only, limit, offset)

    @mcp.tool(description=scheduling.backfill_timeline.__doc__, annotations=READ)
    def backfill_timeline(backfill_id: str, limit: int = 50, offset: int = 0) -> m.BackfillTimeline | m.ToolError:
        return scheduling.backfill_timeline(get_ctx(), backfill_id, limit, offset)

    @mcp.tool(description=scheduling.trigger_backfill.__doc__, annotations=LAUNCH)
    def trigger_backfill(
        component_id: str, start_key: str, end_key: str, concurrency: int = 1, fail_fast: bool = False
    ) -> m.BackfillQueued | m.ToolError:
        return scheduling.trigger_backfill(get_ctx(), component_id, start_key, end_key, concurrency, fail_fast)

    @mcp.tool(description=scheduling.cancel_backfill.__doc__, annotations=CANCEL)
    def cancel_backfill(backfill_id: str) -> m.BackfillCanceled | m.ToolError:
        return scheduling.cancel_backfill(get_ctx(), backfill_id)

    # -- Analytics ---------------------------------------------------------

    @mcp.tool(description=analytics.run_history_summary.__doc__, annotations=READ)
    def run_history_summary(component_id: str | None = None, days: int = 7) -> m.RunHistorySummary | m.ToolError:
        return analytics.run_history_summary(get_ctx(), component_id, days)

    @mcp.tool(description=analytics.partition_coverage.__doc__, annotations=READ)
    def partition_coverage(component_id: str, start_date: str, end_date: str) -> m.PartitionCoverage | m.ToolError:
        return analytics.partition_coverage(get_ctx(), component_id, start_date, end_date)

    @mcp.tool(description=analytics.freshness_check.__doc__, annotations=READ)
    def freshness_check() -> m.FreshnessReport | m.ToolError:
        return analytics.freshness_check(get_ctx())

    @mcp.tool(description=analytics.run_stats.__doc__, annotations=READ)
    def run_stats(
        since: str | None = None,
        until: str | None = None,
        component_id: str | None = None,
        limit: int = 25,
        offset: int = 0,
    ) -> m.RunStats | m.ToolError:
        return analytics.run_stats(get_ctx(), since, until, component_id, limit, offset)

    @mcp.tool(description=analytics.asset_coverage.__doc__, annotations=READ)
    def asset_coverage(
        component_id: str, start_key: str, end_key: str, limit: int = 50, offset: int = 0
    ) -> m.AssetCoverage | m.ToolError:
        return analytics.asset_coverage(get_ctx(), component_id, start_key, end_key, limit, offset)
