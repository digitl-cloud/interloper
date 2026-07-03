"""Interloper Agent — multi-agent system for asset discovery, lineage, and operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from google.adk.agents import Agent
from interloper.settings import AppSettings

from interloper_agent.prompts import (
    ACTION_INSTRUCTION,
    ANALYTICS_INSTRUCTION,
    CATALOG_INSTRUCTION,
    LINEAGE_INSTRUCTION,
    OPERATIONS_INSTRUCTION,
    ROOT_INSTRUCTION,
)
from interloper_agent.tools import actions, analytics, catalog, lineage, operations

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
    description="Discovers sources, inspects asset schemas, searches fields across the catalog, and compares schemas.",
    instruction=CATALOG_INSTRUCTION,
    tools=[
        catalog.list_sources,
        catalog.get_source_detail,
        catalog.get_asset_schema,
        catalog.search_fields,
        catalog.compare_schemas,
        catalog.list_destinations,
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

operations_agent = Agent(
    name="OperationsAgent",
    model=_model,
    description="Monitors run health, recent failures, job schedules, and backfill progress.",
    instruction=OPERATIONS_INSTRUCTION,
    tools=[
        operations.list_recent_runs,
        operations.get_run_detail,
        operations.list_failures,
        operations.get_job_health,
        operations.list_jobs,
        operations.list_backfills,
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

action_agent = Agent(
    name="ActionAgent",
    model=_model,
    description="Triggers runs, starts backfills, and toggles jobs or assets on/off.",
    instruction=ACTION_INSTRUCTION,
    tools=[
        actions.trigger_run,
        actions.trigger_backfill,
        actions.toggle_job,
        actions.toggle_asset,
    ],
)

root_agent = Agent(
    name="InterloperAgent",
    model=_model,
    instruction=ROOT_INSTRUCTION,
    description="Main Interloper assistant that routes queries to specialized sub-agents.",
    sub_agents=[catalog_agent, lineage_agent, operations_agent, analytics_agent, action_agent],
)
