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
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any

from interloper_toolkit import analytics, catalog, collection, jobs, lineage, scheduling, sources
from interloper_toolkit.context import ToolkitContext
from interloper_toolkit.models import ToolError


class Effect(str, Enum):
    """What a tool does to the platform."""

    READ = "read"
    READ_PROVIDER = "read_provider"
    EDIT = "edit"
    CREATE = "create"
    LAUNCH = "launch"
    CANCEL = "cancel"
    DELETE = "delete"


@dataclass(frozen=True)
class Tool:
    """A toolkit function as the surfaces expose it.

    Args:
        fn: The toolkit function, sync or async, taking the context first.
        effect: What it does to the platform.
        title: Its human-readable name, an imperative a person can scan
            (``Trigger a run``), for surfaces that show tools to people.
        carries_secrets: Whether its arguments carry credentials.
    """

    fn: types.FunctionType
    effect: Effect
    title: str
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
            True for creates, cancels and deletes, the calls a user wants to confirm.
        """
        return self.effect in (Effect.CREATE, Effect.CANCEL, Effect.DELETE)

    def bind(
        self,
        context: Callable[..., ToolkitContext | ToolError],
        *leading: inspect.Parameter,
        trailing: Sequence[inspect.Parameter] = (),
    ) -> types.FunctionType:
        """Adapt the function to a surface that supplies the context itself.

        The bound function keeps the name, docstring and every parameter but
        the context, so the schema a model sees is exactly the toolkit's.
        ``leading`` are the surface's own parameters in the context's place,
        if any, and ``trailing`` its own keyword-only parameters after the
        toolkit's. ``context`` receives the leading values positionally, then
        the trailing values as keywords, and returns the context to call with.
        A :class:`~interloper_toolkit.models.ToolError` it returns is the
        bound function's result: the toolkit function is not called.

        Args:
            context: Derives the toolkit context from the surface's arguments, or refuses the call.
            *leading: Parameters the bound function takes before the toolkit's own.
            trailing: Keyword-only parameters the bound function takes after the toolkit's own.

        Returns:
            The bound function, with an explicit signature and annotations.
        """
        signature = inspect.signature(self.fn)
        hints = typing.get_type_hints(self.fn)
        _, *rest = signature.parameters
        parameters = [
            *leading,
            *(signature.parameters[name].replace(annotation=hints[name]) for name in rest),
            *trailing,
        ]
        returns = hints.get("return", Any)
        split = len(leading)

        if inspect.iscoroutinefunction(self.fn):

            async def tool(*args: Any, **kwargs: Any) -> Any:
                derived = self._derive(context, split, trailing, args, kwargs)
                if isinstance(derived, ToolError):
                    return derived
                return await self.fn(derived, *args[split:], **kwargs)
        else:

            def tool(*args: Any, **kwargs: Any) -> Any:
                derived = self._derive(context, split, trailing, args, kwargs)
                if isinstance(derived, ToolError):
                    return derived
                return self.fn(derived, *args[split:], **kwargs)

        tool.__name__ = tool.__qualname__ = self.name
        tool.__module__ = self.fn.__module__
        tool.__doc__ = self.fn.__doc__
        tool.__signature__ = signature.replace(parameters=parameters, return_annotation=returns)  # ty: ignore[invalid-assignment]
        tool.__annotations__ = {p.name: p.annotation for p in parameters} | {"return": returns}
        return tool

    @staticmethod
    def _derive(
        context: Callable[..., ToolkitContext | ToolError],
        split: int,
        trailing: Sequence[inspect.Parameter],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> ToolkitContext | ToolError:
        """Call the surface's context with its own arguments, taking the trailing ones out of the call.

        Args:
            context: The surface's context callable.
            split: How many leading arguments the call carries.
            trailing: The surface's keyword-only parameters, whose values are removed from ``kwargs``.
            args: The bound function's positional arguments.
            kwargs: The bound function's keyword arguments, left with the toolkit function's own.

        Returns:
            The toolkit context, or the refusal the surface returned.
        """
        values = {
            parameter.name: kwargs.pop(parameter.name)
            if parameter.default is inspect.Parameter.empty
            else kwargs.pop(parameter.name, parameter.default)
            for parameter in trailing
        }
        return context(*args[:split], **values)


TOOLS: tuple[Tool, ...] = (
    # Catalog
    Tool(catalog.list_definitions, Effect.READ, "Browse the catalog"),
    Tool(catalog.get_definition, Effect.READ, "Read a definition"),
    Tool(catalog.get_asset_schema, Effect.READ, "Read an asset schema"),
    Tool(catalog.search_fields, Effect.READ, "Search fields"),
    Tool(catalog.compare_schemas, Effect.READ, "Compare schemas"),
    # Collection
    Tool(collection.list_components, Effect.READ, "List components"),
    Tool(collection.update_component, Effect.EDIT, "Update a component"),
    Tool(collection.get_delete_impact, Effect.READ, "Preview a deletion"),
    Tool(collection.delete_component, Effect.DELETE, "Delete a component"),
    Tool(collection.bind_relation, Effect.EDIT, "Bind a relation"),
    Tool(collection.unbind_relation, Effect.EDIT, "Unbind a relation"),
    Tool(collection.request_connection_setup, Effect.READ, "Request a connection setup"),
    Tool(collection.check_connection, Effect.READ_PROVIDER, "Check a connection"),
    Tool(collection.create_connections, Effect.CREATE, "Create connections", carries_secrets=True),
    # Sources and jobs
    Tool(sources.resolve_source_field_options, Effect.READ_PROVIDER, "Fetch a source field's options"),
    Tool(sources.create_source, Effect.CREATE, "Create a source"),
    Tool(sources.create_sources, Effect.CREATE, "Create sources"),
    Tool(jobs.create_job, Effect.CREATE, "Create a job"),
    # Lineage
    Tool(lineage.get_upstream, Effect.READ, "Trace upstream assets"),
    Tool(lineage.get_downstream, Effect.READ, "Trace downstream assets"),
    Tool(lineage.get_full_lineage, Effect.READ, "Trace the full lineage"),
    Tool(lineage.impact_analysis, Effect.READ, "Analyse impact"),
    Tool(lineage.cross_source_dependencies, Effect.READ, "Find cross-source dependencies"),
    # Scheduling
    Tool(scheduling.list_jobs, Effect.READ, "List jobs"),
    Tool(scheduling.toggle_job, Effect.EDIT, "Toggle a job"),
    Tool(scheduling.toggle_asset, Effect.EDIT, "Toggle an asset"),
    Tool(scheduling.list_recent_runs, Effect.READ, "List recent runs"),
    Tool(scheduling.get_run_detail, Effect.READ, "Read a run"),
    Tool(scheduling.list_run_events, Effect.READ, "List a run's events"),
    Tool(scheduling.get_event, Effect.READ, "Read an event"),
    Tool(scheduling.list_failures, Effect.READ, "List recent failures"),
    Tool(scheduling.error_breakdown, Effect.READ, "Break down errors"),
    Tool(scheduling.trigger_run, Effect.LAUNCH, "Trigger a run"),
    Tool(scheduling.retry_run, Effect.LAUNCH, "Retry a run"),
    Tool(scheduling.cancel_run, Effect.CANCEL, "Cancel a run"),
    Tool(scheduling.list_backfills, Effect.READ, "List backfills"),
    Tool(scheduling.backfill_timeline, Effect.READ, "Read a backfill's timeline"),
    Tool(scheduling.trigger_backfill, Effect.LAUNCH, "Trigger a backfill"),
    Tool(scheduling.cancel_backfill, Effect.CANCEL, "Cancel a backfill"),
    # Analytics
    Tool(analytics.pipeline_overview, Effect.READ, "Read the pipeline overview"),
    Tool(analytics.job_health, Effect.READ, "Check job health"),
    Tool(analytics.run_stats, Effect.READ, "Compute run statistics"),
    Tool(analytics.asset_coverage, Effect.READ, "Check asset coverage"),
)
"""Every tool the surfaces expose."""
