"""MCP tool registration — thin wrappers over the shared read-only toolkit.

Every wrapper is one delegation line; the implementations live in
``interloper_db.toolkit`` (shared with the ADK agent), and their LLM-facing
docstrings are adopted as the tool descriptions. FastMCP derives each tool's
input schema from the wrapper's signature.

Deliberately read-only: no mutation (trigger/toggle/create) tools are
registered here.
"""

from __future__ import annotations

from typing import Any

from interloper_db.toolkit import analytics, catalog, collection, lineage, scheduling
from mcp.server.fastmcp import FastMCP

from interloper_mcp.context import get_ctx


def register_tools(mcp: FastMCP) -> None:
    """Register the read-only interloper tools on the server.

    Args:
        mcp: The FastMCP server instance.
    """
    # -- Catalog ----------------------------------------------------------

    @mcp.tool(description=catalog.list_definitions.__doc__)
    def list_definitions(kind: str | None = None) -> dict[str, Any]:
        return catalog.list_definitions(get_ctx(), kind)

    @mcp.tool(description=catalog.get_definition.__doc__)
    def get_definition(key: str) -> dict[str, Any]:
        return catalog.get_definition(get_ctx(), key)

    @mcp.tool(description=catalog.get_asset_schema.__doc__)
    def get_asset_schema(source_key: str, asset_key: str) -> dict[str, Any]:
        return catalog.get_asset_schema(get_ctx(), source_key, asset_key)

    @mcp.tool(description=catalog.search_fields.__doc__)
    def search_fields(query: str) -> dict[str, Any]:
        return catalog.search_fields(get_ctx(), query)

    @mcp.tool(description=catalog.compare_schemas.__doc__)
    def compare_schemas(
        source_key_a: str,
        asset_key_a: str,
        source_key_b: str,
        asset_key_b: str,
    ) -> dict[str, Any]:
        return catalog.compare_schemas(get_ctx(), source_key_a, asset_key_a, source_key_b, asset_key_b)

    # -- Collection -------------------------------------------------------

    @mcp.tool(description=collection.list_components.__doc__)
    def list_components(kind: str | None = None) -> dict[str, Any]:
        return collection.list_components(get_ctx(), kind)

    # -- Lineage ----------------------------------------------------------

    @mcp.tool(description=lineage.get_upstream.__doc__)
    def get_upstream(asset_id: str) -> dict[str, Any]:
        return lineage.get_upstream(get_ctx(), asset_id)

    @mcp.tool(description=lineage.get_downstream.__doc__)
    def get_downstream(asset_id: str) -> dict[str, Any]:
        return lineage.get_downstream(get_ctx(), asset_id)

    @mcp.tool(description=lineage.get_full_lineage.__doc__)
    def get_full_lineage(asset_id: str, direction: str = "upstream") -> dict[str, Any]:
        return lineage.get_full_lineage(get_ctx(), asset_id, direction)

    @mcp.tool(description=lineage.impact_analysis.__doc__)
    def impact_analysis(asset_id: str) -> dict[str, Any]:
        return lineage.impact_analysis(get_ctx(), asset_id)

    @mcp.tool(description=lineage.cross_source_dependencies.__doc__)
    def cross_source_dependencies() -> dict[str, Any]:
        return lineage.cross_source_dependencies(get_ctx())

    # -- Scheduling (read-only) --------------------------------------------

    @mcp.tool(description=scheduling.list_jobs.__doc__)
    def list_jobs() -> dict[str, Any]:
        return scheduling.list_jobs(get_ctx())

    @mcp.tool(description=scheduling.get_job_health.__doc__)
    def get_job_health(component_id: str) -> dict[str, Any]:
        return scheduling.get_job_health(get_ctx(), component_id)

    @mcp.tool(description=scheduling.list_recent_runs.__doc__)
    def list_recent_runs(
        component_id: str | None = None,
        status: str | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        return scheduling.list_recent_runs(get_ctx(), component_id, status, limit)

    @mcp.tool(description=scheduling.get_run_detail.__doc__)
    def get_run_detail(run_id: str) -> dict[str, Any]:
        return scheduling.get_run_detail(get_ctx(), run_id)

    @mcp.tool(description=scheduling.list_failures.__doc__)
    def list_failures(limit: int = 20) -> dict[str, Any]:
        return scheduling.list_failures(get_ctx(), limit)

    @mcp.tool(description=scheduling.list_backfills.__doc__)
    def list_backfills(active_only: bool = True) -> dict[str, Any]:
        return scheduling.list_backfills(get_ctx(), active_only)

    # -- Analytics ---------------------------------------------------------

    @mcp.tool(description=analytics.run_history_summary.__doc__)
    def run_history_summary(component_id: str | None = None, days: int = 7) -> dict[str, Any]:
        return analytics.run_history_summary(get_ctx(), component_id, days)

    @mcp.tool(description=analytics.partition_coverage.__doc__)
    def partition_coverage(component_id: str, start_date: str, end_date: str) -> dict[str, Any]:
        return analytics.partition_coverage(get_ctx(), component_id, start_date, end_date)

    @mcp.tool(description=analytics.freshness_check.__doc__)
    def freshness_check() -> dict[str, Any]:
        return analytics.freshness_check(get_ctx())
