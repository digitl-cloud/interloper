"""Trigger hook: run components in reaction to watched outcomes."""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from interloper.asset.base import Asset
from interloper.component.base import RelationDefinition
from interloper.errors import ConfigError
from interloper.hook.base import Hook, HookContext
from interloper.job.base import Job
from interloper.source.base import Source


class TriggerHook(Hook):
    """Triggers a run of each target when a watched event matches.

    The cascading-pipelines primitive: watch an upstream source or job,
    target the downstream workload. Execution goes through the operator's
    injected ``context.trigger`` capability, so the hook itself carries no
    persistence dependency.

    The ``target`` verb lives here, not on the base hook: only trigger-style
    hooks act on other components, so only their definitions advertise it.
    """

    relation_types: ClassVar[dict[str, RelationDefinition]] = {
        "target": RelationDefinition(kinds=["source", "asset", "job"], field="targets"),
    }
    internal_fields: ClassVar[frozenset[str]] = frozenset({"watches", "targets"})

    targets: list[Source | Asset | Job] = Field(default_factory=list)

    def fire(self, context: HookContext) -> None:
        """Trigger a run for every target.

        Raises:
            ConfigError: If the operator provided no trigger capability.
        """
        if context.trigger is None:
            raise ConfigError(f"TriggerHook '{self.id}' fired without a trigger capability in its context")
        for target in self.targets:
            context.trigger(target.id)
