"""Hook: a component that reacts to what other components do."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field

from interloper.asset.base import Asset
from interloper.component.base import Component, RelationDefinition
from interloper.job.base import Job
from interloper.source.base import Source

#: Event types a hook may subscribe to (v1: run-terminal outcomes).
HOOK_EVENT_TYPES: tuple[str, ...] = ("run_completed", "run_failed")


class HookContext(BaseModel):
    """What a firing hook knows about the event that triggered it.

    Carries the event's identity and metadata, plus the capabilities the
    operator injects: ``trigger`` creates a run for a component id, so
    trigger-style hooks stay free of any persistence dependency.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    event_type: str
    component_id: str
    run_id: str | None = None
    partition_date: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    trigger: Callable[[str], None] | None = Field(default=None, exclude=True)


class HookState(BaseModel):
    """Machine-owned hook state (see ``Component.state_model``)."""

    last_fired_at: str | None = None
    last_run_id: str | None = None


class Hook(Component):
    """A reaction to events on watched components.

    A hook declares *what to observe* (``watches``), *which outcomes matter*
    (``events``), and — like an asset's partitioning or a job's cron — the
    declaration is inert intent: the framework carries it, and an operator
    (the scheduler) evaluates events and calls :meth:`fire`. Concrete hook
    classes own the side effect::

        class SlackHook(Hook):
            channel: str = InputField(description="Channel to notify")

            def fire(self, context: HookContext) -> None:
                post_message(self.channel, context.metadata)

    The base hook is only an observer. Hooks that *act on* other components
    (``TriggerHook``) extend the vocabulary with the ``target`` verb and the
    ``targets`` field — each class's definition advertises exactly what it
    acts on.
    """

    icon: ClassVar[str] = "carbon:lightning"
    relation_types: ClassVar[dict[str, RelationDefinition]] = {
        "watch": RelationDefinition(kinds=["source", "asset", "job"], field="watches"),
        "resource": RelationDefinition(kinds=["connection", "config", "resource"], field="resources", slotted=True),
    }
    internal_fields: ClassVar[frozenset[str]] = frozenset({"watches"})
    state_model: ClassVar[type[BaseModel] | None] = HookState

    watches: list[Source | Asset | Job] = Field(default_factory=list)
    events: list[str] = Field(
        default_factory=lambda: ["run_failed"],
        description="Run outcomes this hook reacts to (run_completed, run_failed)",
    )
    enabled: bool = Field(default=True)

    def fire(self, context: HookContext) -> None:
        """Execute this hook's reaction to an event.

        Subclasses must override this method.

        Raises:
            NotImplementedError: If the subclass does not implement ``fire()``.
        """
        raise NotImplementedError(f"{type(self).__name__} does not implement fire()")
