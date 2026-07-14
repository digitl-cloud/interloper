"""Interloper Agent — multi-agent system for asset discovery, lineage, and scheduling."""

from __future__ import annotations

from typing import TYPE_CHECKING

from google.adk.agents import Agent
from interloper.settings import AppSettings

from interloper_agent.prompts import (
    ANALYTICS_INSTRUCTION,
    CATALOG_INSTRUCTION,
    CONNECTION_INSTRUCTION,
    LINEAGE_INSTRUCTION,
    ROOT_INSTRUCTION,
    SCHEDULING_INSTRUCTION,
)
from interloper_agent.tools import analytics, assets, connections, destinations, lineage, scheduling, sources

if TYPE_CHECKING:
    from google.adk.models import BaseLlm


def resolve_model(name: str | None = None) -> str | BaseLlm:
    """Resolve a model name into an ADK model reference.

    Bare names (``gemini-2.5-flash``) are native Gemini models; names with a
    provider prefix (``anthropic/claude-sonnet-4-5``) are routed through
    LiteLLM, which reads the provider's standard credential env vars.
    """
    name = name or AppSettings.get().agent.model
    if "/" in name:
        from google.adk.models.lite_llm import LiteLlm

        return LiteLlm(model=name)
    return name


_model = resolve_model()

catalog_agent = Agent(
    name="CatalogAgent",
    model=_model,
    description=(
        "Discovers the catalog of available source definitions and the organisation's collection of sources "
        "and destinations, inspects asset schemas, searches fields across the catalog, and compares schemas."
    ),
    instruction=CATALOG_INSTRUCTION,
    tools=[
        sources.list_sources,
        sources.list_catalog_sources,
        sources.get_source_detail,
        assets.get_asset_schema,
        assets.search_fields,
        assets.compare_schemas,
        destinations.list_destinations,
    ],
)

lineage_agent = Agent(
    name="LineageAgent",
    model=_model,
    description="Analyzes asset dependencies — upstream/downstream traversal, impact analysis, and cross-source edges.",
    instruction=LINEAGE_INSTRUCTION,
    tools=[
        lineage.get_upstream,
        lineage.get_downstream,
        lineage.get_full_lineage,
        lineage.impact_analysis,
        lineage.cross_source_dependencies,
    ],
)

scheduling_agent = Agent(
    name="SchedulingAgent",
    model=_model,
    description=(
        "Monitors run health, recent failures, job schedules, and backfill progress; "
        "triggers runs, starts backfills, and toggles jobs or assets on/off."
    ),
    instruction=SCHEDULING_INSTRUCTION,
    tools=[
        scheduling.list_jobs,
        scheduling.get_job_health,
        scheduling.toggle_job,
        scheduling.list_recent_runs,
        scheduling.get_run_detail,
        scheduling.list_failures,
        scheduling.trigger_run,
        scheduling.list_backfills,
        scheduling.trigger_backfill,
        scheduling.toggle_asset,
    ],
)

analytics_agent = Agent(
    name="AnalyticsAgent",
    model=_model,
    description="Provides run statistics, partition coverage analysis, and data freshness checks.",
    instruction=ANALYTICS_INSTRUCTION,
    tools=[
        analytics.run_history_summary,
        analytics.partition_coverage,
        analytics.freshness_check,
    ],
)

connection_agent = Agent(
    name="ConnectionAgent",
    model=_model,
    description=(
        "Lists the organisation's collection of connections, checks their health, and sets up new ones by "
        "presenting the app's secure setup form (OAuth sign-in or manual credentials) — never collects "
        "credentials in chat."
    ),
    instruction=CONNECTION_INSTRUCTION,
    tools=[
        connections.list_catalog_connections,
        connections.list_connections,
        connections.request_connection_setup,
        connections.check_connection,
    ],
)

root_agent = Agent(
    name="InterloperAgent",
    model=_model,
    instruction=ROOT_INSTRUCTION,
    description="Main Interloper assistant that routes queries to specialized sub-agents.",
    sub_agents=[catalog_agent, lineage_agent, scheduling_agent, analytics_agent, connection_agent],
)
