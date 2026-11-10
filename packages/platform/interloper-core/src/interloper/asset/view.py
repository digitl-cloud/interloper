"""View: an asset whose definition is materialized, as a view in its destination."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, cast

from interloper.asset.base import Asset
from interloper.asset.output import Output
from interloper.errors import ConfigError
from interloper.events import Step, StepKind
from interloper.partitioning import PartitionWindow
from interloper.utils.concurrency import invoke

if TYPE_CHECKING:
    from collections.abc import Mapping

    from interloper.component import Component
    from interloper.destination import Destination


class View(Asset):
    """An asset whose ``data()`` returns a definition, which its destination materializes as a view.

    A view reads no upstream data: each ``il.Upstream`` it receives carries
    the destination's reference to that upstream (a table reference on a SQL
    warehouse) in ``data``, and whatever ``data()`` returns goes to
    `Destination.create_view`. It has exactly one destination, and it is
    unpartitioned: a run, for whatever partition, builds the same definition
    and recreates the view, cheaply and idempotently, so the view matches the
    wiring as of its last run.

    ``dialect`` pins a raw-SQL view to the warehouses whose dialect it is
    written in; ``None`` leaves the SQL unchecked, the author's assertion that
    it is portable.

    A view varies from an asset through its hooks alone:
    ``reads_upstream_data`` makes each upstream a reference, `_output` keeps
    the definition as it is, and `_land` creates the view.
    """

    reads_upstream_data: ClassVar[bool] = False
    dialect: ClassVar[str | None] = None

    # -- Destination -----------------------------------------------------------

    @property
    def default_destination(self) -> Destination:
        """The one destination this view is created in.

        Raises:
            ConfigError: If the view does not have exactly one destination.
        """
        destinations: list[Destination] = self.destinations
        if len(destinations) != 1:
            raise ConfigError(f"View '{self.qualified_key}' needs exactly one destination, has {len(destinations)}")
        return destinations[0]

    def validate_relations(self, nodes: Mapping[str, Component] | None = None) -> None:
        """Check the relations as any asset, then what only a view needs.

        The view's own checks need the graph: one destination; a dialect, when
        declared, that the destination shares; every bound upstream reachable
        from that destination. They run at graph build, where the DAG supplies
        ``nodes``, and not when a component is rebuilt from its spec, so a view
        source hydrates before a job supplies its destination. A manifest
        loaded through ``DAG.from_spec_file`` still gets them because it
        builds a DAG.

        Args:
            nodes: Every node materializing in the same run, keyed by id, or ``None``.

        Raises:
            ConfigError: If the view has no single destination, its dialect
                does not match the destination's, or an upstream cannot be
                referenced from it.
        """
        super().validate_relations(nodes)
        if nodes is None:
            return
        problems: list[str] = []
        destinations: list[Destination] = self.destinations
        if len(destinations) != 1:
            problems.append(f"a view needs exactly one destination, has {len(destinations)}")
        else:
            destination = destinations[0]
            declared = getattr(destination, "dialect", None)
            if self.dialect is not None and declared != self.dialect:
                problems.append(
                    f"written in dialect '{self.dialect}' but destination '{destination.key}' speaks {declared!r}"
                )
            for name in self.upstream_relations():
                for target in cast("list[Asset]", self._bound.get(name, [])):
                    try:
                        destination.reference(target)
                    except ConfigError as error:
                        problems.append(f"'{name}' upstream '{target.qualified_key}': {error}")
        if problems:
            raise ConfigError(f"View '{self.qualified_key}': " + "; ".join(problems))

    # -- Internals -------------------------------------------------------------

    def _output(self, result: Any) -> Output:
        """Leave a definition alone: it is not data.

        Args:
            result: The value ``data()`` returned.

        Returns:
            The definition as the output's data, without a schema.
        """
        return Output(result)

    async def _land(self, output: Output, window: PartitionWindow | None, metadata: dict[str, Any]) -> None:
        """Create or replace the view in its one destination, as an observed write.

        Args:
            output: The run's output, its data the view's definition.
            window: The run's window; a view is unpartitioned, so ``None``.
            metadata: Run-level metadata carried onto the emitted events.
        """
        destination = self.default_destination
        step = Step(
            StepKind.DEST_WRITE,
            self.event_metadata(metadata, window, destination_key=destination.key),
            subject=f"view '{self.key}'",
        )
        with step:
            await invoke(destination.create_view, self, output.data)
