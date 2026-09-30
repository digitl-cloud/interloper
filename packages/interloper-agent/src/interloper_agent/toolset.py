"""The agent's toolset: the toolkit's functions, bound to the run's context.

A toolkit function takes the :class:`~interloper_toolkit.ToolkitContext`
first; pydantic-ai hands a tool its :class:`~pydantic_ai.RunContext`, whose
``deps`` is that same context. :func:`bind` is the whole adaptation: the same
name, docstring and parameters, minus the context the run supplies.

:data:`TOOLS` is the one place that says what the agent can do and which
calls wait for the user's approval in the app.
"""

from __future__ import annotations

import inspect
import types
import typing
from typing import Any

from interloper_toolkit import ToolkitContext, analytics, catalog, collection, jobs, lineage, scheduling, sources
from pydantic_ai import RunContext
from pydantic_ai.toolsets import FunctionToolset

from interloper_agent import interaction

TOOLS: tuple[tuple[types.FunctionType, bool], ...] = (
    # Catalog
    (catalog.list_definitions, False),
    (catalog.get_definition, False),
    (catalog.get_asset_schema, False),
    (catalog.search_fields, False),
    (catalog.compare_schemas, False),
    # Collection
    (collection.list_components, False),
    (collection.update_component, False),
    (collection.bind_relation, False),
    (collection.unbind_relation, False),
    (collection.check_connection, False),
    (collection.create_connections, True),
    (interaction.request_connection_setup, False),
    (interaction.request_user_selection, False),
    # Sources and jobs
    (sources.resolve_source_field_options, False),
    (sources.create_source, True),
    (sources.create_sources, True),
    (jobs.create_job, True),
    # Lineage
    (lineage.get_upstream, False),
    (lineage.get_downstream, False),
    (lineage.get_full_lineage, False),
    (lineage.impact_analysis, False),
    (lineage.cross_source_dependencies, False),
    # Scheduling
    (scheduling.list_jobs, False),
    (scheduling.get_job_health, False),
    (scheduling.toggle_job, False),
    (scheduling.toggle_asset, False),
    (scheduling.list_recent_runs, False),
    (scheduling.get_run_detail, False),
    (scheduling.list_run_events, False),
    (scheduling.get_event, False),
    (scheduling.list_failures, False),
    (scheduling.error_breakdown, False),
    (scheduling.trigger_run, False),
    (scheduling.retry_run, False),
    (scheduling.list_backfills, False),
    (scheduling.backfill_timeline, False),
    (scheduling.trigger_backfill, False),
    (scheduling.cancel_backfill, True),
    # Analytics
    (analytics.run_history_summary, False),
    (analytics.partition_coverage, False),
    (analytics.freshness_check, False),
    (analytics.run_stats, False),
    (analytics.asset_coverage, False),
)
"""Every tool the agent registers, as ``(toolkit function, requires approval)``."""


def bind(fn: types.FunctionType) -> types.FunctionType:
    """Adapt a toolkit function to a pydantic-ai tool taking the run context.

    The tool keeps the function's name, docstring and every parameter but
    the first, so the schema the model sees is exactly the toolkit's, and
    forwards ``run_context.deps`` (the :class:`ToolkitContext`) in its place.

    Args:
        fn: A toolkit function, sync or async, whose first parameter is the
            toolkit context.

    Returns:
        The tool function, with an explicit signature and annotations.
    """
    signature = inspect.signature(fn)
    hints = typing.get_type_hints(fn)
    _, *rest = signature.parameters
    run_context = inspect.Parameter(
        "run_context", inspect.Parameter.POSITIONAL_ONLY, annotation=RunContext[ToolkitContext]
    )
    parameters = [run_context, *(signature.parameters[name].replace(annotation=hints[name]) for name in rest)]

    if inspect.iscoroutinefunction(fn):

        async def tool(run_context: RunContext[ToolkitContext], *args: Any, **kwargs: Any) -> Any:
            return await fn(run_context.deps, *args, **kwargs)
    else:

        def tool(run_context: RunContext[ToolkitContext], *args: Any, **kwargs: Any) -> Any:
            return fn(run_context.deps, *args, **kwargs)

    tool.__name__ = tool.__qualname__ = fn.__name__
    tool.__module__ = fn.__module__
    tool.__doc__ = fn.__doc__
    tool.__signature__ = signature.replace(parameters=parameters, return_annotation=hints.get("return", Any))  # ty: ignore[invalid-assignment]
    tool.__annotations__ = {p.name: p.annotation for p in parameters} | {"return": hints.get("return", Any)}
    return tool


def toolset() -> FunctionToolset[ToolkitContext]:
    """Build the agent's toolset from :data:`TOOLS`.

    Returns:
        The toolset, one tool per toolkit function.
    """
    built: FunctionToolset[ToolkitContext] = FunctionToolset()
    for fn, requires_approval in TOOLS:
        built.add_function(bind(fn), takes_ctx=True, requires_approval=requires_approval)
    return built
