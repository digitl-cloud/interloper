"""View: an asset whose definition is materialized, as a view in its destination."""

from __future__ import annotations

import traceback
from typing import TYPE_CHECKING, Any, ClassVar, cast

from interloper.asset.base import Asset
from interloper.asset.upstream import Upstream
from interloper.errors import ConfigError, format_exception
from interloper.events import EventBus, EventType
from interloper.telemetry import attributes as telemetry_attributes
from interloper.telemetry.tracer import tracer
from interloper.utils.concurrency import invoke

if TYPE_CHECKING:
    from collections.abc import Mapping

    from interloper.component import Component
    from interloper.dag.base import DAG
    from interloper.destination import Destination
    from interloper.partitioning.base import Partition, PartitionWindow


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
    """

    reads_upstream_data: ClassVar[bool] = False
    dialect: ClassVar[str | None] = None

    # -- Execution -------------------------------------------------------------

    async def materialize_async(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
        dag: DAG | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Build the definition and create the view.

        Args:
            partition_or_window: Accepted and ignored: every partition builds the same definition.
            dag: DAG the upstream assets are looked up in.
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).

        Returns:
            The definition ``data()`` returned, or ``None`` if the view is disabled.
        """
        if not self.enabled:
            return None
        metadata = metadata or {}
        definition = self._definition(await self.run_async(partition_or_window, dag, metadata))
        destination = self._view_destination()
        event_metadata = self._event_metadata(metadata, partition_or_window)
        event_metadata["destination_key"] = destination.key
        EventBus.emit(
            EventType.DEST_WRITE_STARTED, metadata={**event_metadata, "message": f"Creating view '{self.key}'"}
        )
        try:
            span_attributes = telemetry_attributes.from_metadata(event_metadata)
            with tracer().start_as_current_span("interloper.destination.write", attributes=span_attributes):
                await invoke(destination.create_view, self, definition)
            EventBus.emit(
                EventType.DEST_WRITE_COMPLETED, metadata={**event_metadata, "message": f"Created view '{self.key}'"}
            )
        except Exception as error:
            EventBus.emit(
                EventType.DEST_WRITE_FAILED,
                metadata={
                    **event_metadata,
                    "error": format_exception(error),
                    "traceback": traceback.format_exc(),
                    "message": f"Failed to create view '{self.key}': {format_exception(error)}",
                },
            )
            raise
        return definition

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
        destinations = self._destinations()
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

    def _definition(self, result: Any) -> Any:
        """What ``data()`` returned, as the destination receives it.

        Args:
            result: The value ``data()`` returned.

        Returns:
            The result, untouched; a subclass may check or wrap it.
        """
        return result

    def _normalize_and_conform(self, result: Any) -> Any:
        """Leave a definition alone: it is not data.

        Args:
            result: The value ``data()`` returned.

        Returns:
            The result, untouched.
        """
        return result

    def _view_destination(self) -> Destination:
        """The one destination this view is created in.

        Returns:
            The destination.

        Raises:
            ConfigError: If the view does not have exactly one destination.
        """
        destinations = self._destinations()
        if len(destinations) != 1:
            raise ConfigError(f"View '{self.qualified_key}' needs exactly one destination, has {len(destinations)}")
        return destinations[0]

    async def _read_upstreams(
        self,
        name: str,
        targets: list[Asset],
        dag: DAG | None,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any],
    ) -> list[Upstream]:
        """Reference every upstream instead of reading it.

        Args:
            name: The ``data()`` parameter the upstreams are built for.
            targets: The bound upstream assets.
            dag: Unused; a reference needs no run.
            partition_or_window: Unused; a reference has no scope.
            metadata: Unused.

        Returns:
            One `Upstream` per target, its ``data`` the destination's reference.
        """
        destination = self._view_destination()
        return [Upstream(asset=target, data=destination.reference(target)) for target in targets]
