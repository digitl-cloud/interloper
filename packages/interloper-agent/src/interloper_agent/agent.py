"""Interloper Agent — multi-agent system for asset discovery, lineage, and scheduling."""

from __future__ import annotations

import datetime
from collections.abc import Callable
from typing import TYPE_CHECKING

from google.adk.agents import Agent
from google.adk.tools.agent_tool import AgentTool
from interloper.settings import AppSettings

from interloper_agent.prompts import (
    ANALYTICS_INSTRUCTION,
    CATALOG_CONSULT_INSTRUCTION,
    CATALOG_INSTRUCTION,
    COLLECTION_INSTRUCTION,
    LINEAGE_INSTRUCTION,
    ROOT_INSTRUCTION,
    SCHEDULING_INSTRUCTION,
)
from interloper_agent.tools import analytics, catalog, collection, interaction, lineage, scheduling

if TYPE_CHECKING:
    from google.adk.agents.readonly_context import ReadonlyContext
    from google.adk.models import BaseLlm


def with_current_time(instruction: str) -> Callable[[ReadonlyContext], str]:
    """Turn a static instruction into a provider that appends the current UTC time.

    The model has no reliable notion of "now", so without this the relative
    timestamps the presentation rules ask for drift to its training data.
    """

    def provider(_: ReadonlyContext) -> str:
        now = datetime.datetime.now(datetime.timezone.utc)
        return f"{instruction}\nCurrent date and time: {now:%Y-%m-%d %H:%M} UTC. Compute relative timestamps from it."

    return provider


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


def _catalog_tools() -> list:
    """The catalog toolset, shared by the routing agent and the consultant instance."""
    return [
        catalog.list_definitions,
        catalog.get_definition,
        catalog.get_asset_schema,
        catalog.search_fields,
        catalog.compare_schemas,
    ]


catalog_agent = Agent(
    name="CatalogAgent",
    model=_model,
    description=(
        "The catalog of component definitions the platform ships: which sources and connections are "
        "available to add, asset schemas, field search, and schema comparison."
    ),
    instruction=with_current_time(CATALOG_INSTRUCTION),
    tools=_catalog_tools(),
)

# A second instance (an ADK agent can only have one parent): the catalog
# specialist as a consultable tool — the caller keeps the conversation and
# receives the specialist's answer as a tool result, unlike a transfer.
catalog_consultant = Agent(
    name="consult_catalog",
    model=_model,
    description=(
        "Consult the catalog specialist: ask a question about the catalog of component definitions "
        "(available sources and connections, asset schemas, fields, comparisons) and get a concise, "
        "grounded answer back as a tool result."
    ),
    instruction=with_current_time(CATALOG_CONSULT_INSTRUCTION),
    tools=_catalog_tools(),
)

collection_agent = Agent(
    name="CollectionAgent",
    model=_model,
    description=(
        "The organisation's collection of component instances: lists their sources, connections, and "
        "destinations, checks connection health, sets up new connections via the app's secure form "
        "(never collecting credentials in chat), and creates sources conversationally — resolving "
        "provider-backed options like the account to use through an existing connection."
    ),
    instruction=with_current_time(COLLECTION_INSTRUCTION),
    tools=[
        collection.list_components,
        collection.request_connection_setup,
        collection.create_connections,
        collection.check_connection,
        collection.resolve_source_field_options,
        collection.create_source,
        collection.create_sources,
        collection.create_job,
        interaction.request_user_selection,
        interaction.request_confirmation,
        AgentTool(agent=catalog_consultant),
    ],
)

lineage_agent = Agent(
    name="LineageAgent",
    model=_model,
    description="Analyzes asset dependencies — upstream/downstream traversal, impact analysis, and cross-source edges.",
    instruction=with_current_time(LINEAGE_INSTRUCTION),
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
        "triggers runs, starts backfills, toggles jobs or assets on/off, and creates "
        "cron jobs over the collection's sources."
    ),
    instruction=with_current_time(SCHEDULING_INSTRUCTION),
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
        collection.list_components,
        collection.create_job,
        interaction.request_confirmation,
    ],
)

analytics_agent = Agent(
    name="AnalyticsAgent",
    model=_model,
    description="Provides run statistics, partition coverage analysis, and data freshness checks.",
    instruction=with_current_time(ANALYTICS_INSTRUCTION),
    tools=[
        analytics.run_history_summary,
        analytics.partition_coverage,
        analytics.freshness_check,
    ],
)

root_agent = Agent(
    name="InterloperAgent",
    model=_model,
    instruction=with_current_time(ROOT_INSTRUCTION),
    description="Main Interloper assistant that routes queries to specialized sub-agents.",
    sub_agents=[catalog_agent, collection_agent, lineage_agent, scheduling_agent, analytics_agent],
)
