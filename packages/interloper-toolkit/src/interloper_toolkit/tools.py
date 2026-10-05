"""The tool table: every tool the AI surfaces expose, with its effect on the platform.

A surface (the agent, the MCP server) registers this table rather than a
list of its own. The effect is the one fact a surface derives its behaviour
from: MCP maps it to the annotations a client keys on, the agent to which
calls wait for the user's approval. ``carries_secrets`` marks a tool whose
arguments would carry credentials; a surface that transports arguments
through a third party leaves it out.
"""

from __future__ import annotations

import inspect
import types
import typing
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from interloper_toolkit import analytics, catalog, collection, jobs, lineage, scheduling, sources
from interloper_toolkit.context import ToolkitContext


class Effect(str, Enum):
    """What a tool does to the platform."""

    READ = "read"
    READ_PROVIDER = "read_provider"
    EDIT = "edit"
    CREATE = "create"
    LAUNCH = "launch"
    CANCEL = "cancel"


@dataclass(frozen=True)
class Tool:
    """A toolkit function as the surfaces expose it.

    Args:
        fn: The toolkit function, sync or async, taking the context first.
        effect: What it does to the platform.
        carries_secrets: Whether its arguments carry credentials.
    """

    fn: types.FunctionType
    effect: Effect
    carries_secrets: bool = False

    @property
    def name(self) -> str:
        """The tool's name, which is the function's.

        Returns:
            The function name.
        """
        return self.fn.__name__

    @property
    def needs_approval(self) -> bool:
        """Whether an interactive surface waits for the user before the call runs.

        Returns:
            True for creates and cancels, the calls a user wants to confirm.
        """
        return self.effect in (Effect.CREATE, Effect.CANCEL)

    def bind(self, context: Callable[..., ToolkitContext], *leading: inspect.Parameter) -> types.FunctionType:
        """Adapt the function to a surface that supplies the context itself.

        The bound function keeps the name, docstring and every parameter but
        the context, so the schema a model sees is exactly the toolkit's.
        ``leading`` are the surface's own parameters in the context's place,
        if any; ``context`` receives their values and returns the context to
        call with.

        Args:
            context: Derives the toolkit context from the leading arguments.
            *leading: Parameters the bound function takes before the toolkit's own.

        Returns:
            The bound function, with an explicit signature and annotations.
        """
        signature = inspect.signature(self.fn)
        hints = typing.get_type_hints(self.fn)
        _, *rest = signature.parameters
        parameters = [*leading, *(signature.parameters[name].replace(annotation=hints[name]) for name in rest)]
        returns = hints.get("return", Any)
        split = len(leading)

        if inspect.iscoroutinefunction(self.fn):

            async def tool(*args: Any, **kwargs: Any) -> Any:
                return await self.fn(context(*args[:split]), *args[split:], **kwargs)
        else:

            def tool(*args: Any, **kwargs: Any) -> Any:
                return self.fn(context(*args[:split]), *args[split:], **kwargs)

        tool.__name__ = tool.__qualname__ = self.name
        tool.__module__ = self.fn.__module__
        tool.__doc__ = self.fn.__doc__
        tool.__signature__ = signature.replace(parameters=parameters, return_annotation=returns)  # ty: ignore[invalid-assignment]
        tool.__annotations__ = {p.name: p.annotation for p in parameters} | {"return": returns}
        return tool


TOOLS: tuple[Tool, ...] = (
    # Catalog
    Tool(catalog.list_definitions, Effect.READ),
    Tool(catalog.get_definition, Effect.READ),
    Tool(catalog.get_asset_schema, Effect.READ),
    Tool(catalog.search_fields, Effect.READ),
    Tool(catalog.compare_schemas, Effect.READ),
    # Collection
    Tool(collection.list_components, Effect.READ),
    Tool(collection.update_component, Effect.EDIT),
    Tool(collection.bind_relation, Effect.EDIT),
    Tool(collection.unbind_relation, Effect.EDIT),
    Tool(collection.request_connection_setup, Effect.READ),
    Tool(collection.check_connection, Effect.READ_PROVIDER),
    Tool(collection.create_connections, Effect.CREATE, carries_secrets=True),
    # Sources and jobs
    Tool(sources.resolve_source_field_options, Effect.READ_PROVIDER),
    Tool(sources.create_source, Effect.CREATE),
    Tool(sources.create_sources, Effect.CREATE),
    Tool(jobs.create_job, Effect.CREATE),
    # Lineage
    Tool(lineage.get_upstream, Effect.READ),
    Tool(lineage.get_downstream, Effect.READ),
    Tool(lineage.get_full_lineage, Effect.READ),
    Tool(lineage.impact_analysis, Effect.READ),
    Tool(lineage.cross_source_dependencies, Effect.READ),
    # Scheduling
    Tool(scheduling.list_jobs, Effect.READ),
    Tool(scheduling.toggle_job, Effect.EDIT),
    Tool(scheduling.toggle_asset, Effect.EDIT),
    Tool(scheduling.list_recent_runs, Effect.READ),
    Tool(scheduling.get_run_detail, Effect.READ),
    Tool(scheduling.list_run_events, Effect.READ),
    Tool(scheduling.get_event, Effect.READ),
    Tool(scheduling.list_failures, Effect.READ),
    Tool(scheduling.error_breakdown, Effect.READ),
    Tool(scheduling.trigger_run, Effect.LAUNCH),
    Tool(scheduling.retry_run, Effect.LAUNCH),
    Tool(scheduling.cancel_run, Effect.CANCEL),
    Tool(scheduling.list_backfills, Effect.READ),
    Tool(scheduling.backfill_timeline, Effect.READ),
    Tool(scheduling.trigger_backfill, Effect.LAUNCH),
    Tool(scheduling.cancel_backfill, Effect.CANCEL),
    # Analytics
    Tool(analytics.job_health, Effect.READ),
    Tool(analytics.run_stats, Effect.READ),
    Tool(analytics.asset_coverage, Effect.READ),
)
"""Every tool the surfaces expose."""
