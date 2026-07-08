"""Trigger hook: run components in reaction to watched outcomes."""

from __future__ import annotations

from interloper.errors import ConfigError
from interloper.hook.base import Hook, HookContext


class TriggerHook(Hook):
    """Triggers a run of each target when a watched event matches.

    The cascading-pipelines primitive: watch an upstream source or job,
    target the downstream workload. Execution goes through the operator's
    injected ``context.trigger`` capability, so the hook itself carries no
    persistence dependency.
    """

    def fire(self, context: HookContext) -> None:
        """Trigger a run for every target.

        Raises:
            ConfigError: If the operator provided no trigger capability.
        """
        if context.trigger is None:
            raise ConfigError(f"TriggerHook '{self.id}' fired without a trigger capability in its context")
        for target in self.targets:
            context.trigger(target.id)
