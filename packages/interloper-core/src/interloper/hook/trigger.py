"""Trigger hook: run components in reaction to watched outcomes."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from interloper.component import Relation
from interloper.errors import ConfigError
from interloper.hook.base import Hook, HookContext

if TYPE_CHECKING:
    from interloper.asset.base import Asset
    from interloper.job.base import Job
    from interloper.source.base import Source


class TriggerHook(Hook):
    """Triggers a run of each target when a watched event matches.

    The cascading-pipelines primitive: watch an upstream source or job,
    target the downstream workload. Execution goes through the operator's
    injected ``context.trigger`` capability, so the hook itself carries no
    persistence dependency.

    The ``targets`` relation lives here, not on the base hook: only
    trigger-style hooks act on other components, so only their definitions
    advertise it.
    """

    if TYPE_CHECKING:
        targets: list[Source | Asset | Job]

    relations: ClassVar[dict[str, Relation]] = {
        "targets": Relation(["source", "asset", "job"], many=True, optional=True),
    }

    def fire(self, context: HookContext) -> None:
        """Trigger a run for every target.

        Args:
            context: The event context, whose ``trigger`` capability creates
                the run for each target's component id.

        Raises:
            ConfigError: If the operator provided no trigger capability.
        """
        if context.trigger is None:
            raise ConfigError(f"TriggerHook '{self.id}' fired without a trigger capability in its context")
        for target in self.targets:
            context.trigger(target.id)
