"""Run state tracking and dynamic asset scheduling."""

from __future__ import annotations

import datetime as dt
import uuid
from typing import TYPE_CHECKING, Any

from interloper.events import EventBus, EventType
from interloper.runner.results import AssetExecutionInfo, ExecutionStatus

if TYPE_CHECKING:
    from interloper.asset.base import Asset
    from interloper.dag.base import DAG
    from interloper.partitioning.base import Partition, PartitionWindow


class RunState:
    """Tracks asset execution state and determines which assets are ready to run.

    Used by runners to dynamically schedule assets based on dependency
    completion rather than static DAG levels.

    All state mutations occur on the asyncio event loop's single thread,
    so no locking is required.
    """

    def __init__(
        self,
        dag: DAG,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the run state.

        Args:
            dag: The DAG to track.
            metadata: Arbitrary metadata (e.g. run_id, backfill_id).
                A ``run_id`` is generated automatically if not provided.
        """
        self.dag = dag
        self.metadata: dict[str, Any] = metadata or {}
        if "run_id" not in self.metadata:
            self.metadata["run_id"] = str(uuid.uuid4())

        self.asset_executions: dict[str, AssetExecutionInfo] = {}
        self.partition_or_window: Partition | PartitionWindow | None = None
        self.start_time: dt.datetime | None = None
        self.end_time: dt.datetime | None = None

        self._initialize_assets()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def run_id(self) -> str:
        """The run ID."""
        return self.metadata["run_id"]

    @property
    def backfill_id(self) -> str | None:
        """The backfill ID, if set."""
        return self.metadata.get("backfill_id")

    @property
    def elapsed_time(self) -> float | None:
        """Elapsed wall-clock time of the run in seconds, or None if not finished."""
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    @property
    def queued_assets(self) -> list[Asset]:
        """List of assets waiting to be scheduled."""
        return self._assets_with_status(ExecutionStatus.QUEUED)

    @property
    def ready_assets(self) -> list[Asset]:
        """List of assets whose dependencies are met and can be executed."""
        return self._assets_with_status(ExecutionStatus.READY)

    @property
    def running_assets(self) -> list[Asset]:
        """List of assets currently being executed."""
        return self._assets_with_status(ExecutionStatus.RUNNING)

    @property
    def completed_assets(self) -> list[Asset]:
        """List of assets that completed successfully."""
        return self._assets_with_status(ExecutionStatus.COMPLETED)

    @property
    def failed_assets(self) -> list[Asset]:
        """List of assets that failed."""
        return self._assets_with_status(ExecutionStatus.FAILED)

    # ------------------------------------------------------------------
    # Run lifecycle
    # ------------------------------------------------------------------

    def start_run(self, partition_or_window: Partition | PartitionWindow | None) -> None:
        """Record the run start time and emit RUN_STARTED + ASSET_QUEUED events."""
        self.partition_or_window = partition_or_window
        self.start_time = dt.datetime.now(dt.timezone.utc)
        self.end_time = None

        EventBus.emit(
            EventType.RUN_STARTED,
            metadata={
                **self.metadata,
                "partition_or_window": str(self.partition_or_window) if self.partition_or_window else None,
                "message": f"Run started ({len(self.dag.assets)} assets)",
            },
        )

        for asset in self.dag.assets:
            info = self.asset_executions[asset.id]
            if info.status in (ExecutionStatus.QUEUED, ExecutionStatus.READY):
                EventBus.emit(
                    EventType.ASSET_QUEUED,
                    metadata={
                        **self._asset_event_metadata(asset),
                        "message": f"Asset '{type(asset).key}' queued",
                    },
                )

    def end_run(
        self,
        status: ExecutionStatus,
        error: str | None = None,
    ) -> dict[str, AssetExecutionInfo]:
        """Record the run end time, emit a terminal event, and return asset executions.

        Returns:
            A copy of the asset execution info dictionary.
        """
        self.end_time = dt.datetime.now(dt.timezone.utc)

        event_type = EventType.RUN_COMPLETED if status == ExecutionStatus.COMPLETED else EventType.RUN_FAILED
        if status == ExecutionStatus.COMPLETED:
            message = f"Run completed ({len(self.completed_assets)}/{len(self.dag.assets)} succeeded)"
        else:
            message = f"Run failed: {error}" if error else "Run failed"

        EventBus.emit(
            event_type,
            metadata={
                **self.metadata,
                "partition_or_window": str(self.partition_or_window) if self.partition_or_window else None,
                "error": error,
                "message": message,
            },
        )

        return self.asset_executions.copy()

    def is_run_complete(self) -> bool:
        """Check whether every asset has reached a terminal state.

        Returns:
            True if all assets are completed, failed, canceled, or skipped.
        """
        return all(info.is_terminal for info in self.asset_executions.values())

    # ------------------------------------------------------------------
    # Asset state transitions
    # ------------------------------------------------------------------

    def mark_asset_running(self, asset: Asset, *, emit: bool = True) -> None:
        """Transition an asset to RUNNING.

        Args:
            asset: The asset that started.
            emit: Emit ``ASSET_STARTED`` on the EventBus.  Set to
                ``False`` for cross-process runners where the child
                process emits the event itself.
        """
        self.asset_executions[asset.id].mark_running()

        if emit:
            EventBus.emit(
                EventType.ASSET_STARTED,
                metadata={
                    **self._asset_event_metadata(asset),
                    "message": f"Asset '{type(asset).key}' started",
                },
            )

    def mark_asset_completed(self, asset: Asset, *, emit: bool = True) -> None:
        """Transition an asset to COMPLETED and promote ready dependents.

        Args:
            asset: The asset that completed.
            emit: Emit ``ASSET_COMPLETED`` on the EventBus.
        """
        self.asset_executions[asset.id].mark_completed()
        self._promote_dependents(asset.id)

        if emit:
            EventBus.emit(
                EventType.ASSET_COMPLETED,
                metadata={
                    **self._asset_event_metadata(asset),
                    "message": f"Asset '{type(asset).key}' completed",
                },
            )

    def mark_asset_canceled(self, asset: Asset, *, emit: bool = True) -> None:
        """Transition an asset to CANCELED.

        Args:
            asset: The asset that was canceled.
            emit: Emit ``ASSET_CANCELED`` on the EventBus.
        """
        self.asset_executions[asset.id].mark_canceled()

        if emit:
            EventBus.emit(
                EventType.ASSET_CANCELED,
                metadata={
                    **self._asset_event_metadata(asset),
                    "message": f"Asset '{type(asset).key}' canceled",
                },
            )

    def mark_asset_failed(
        self,
        asset: Asset,
        error: str,
        tb: str | None = None,
        *,
        emit: bool = True,
    ) -> None:
        """Transition an asset to FAILED and cancel downstream dependents.

        Args:
            asset: The asset that failed.
            error: Error message describing the failure.
            tb: Optional formatted traceback string.
            emit: Emit ``ASSET_FAILED`` and ``ASSET_CANCELED`` events on
                the EventBus.  Set to ``False`` for cross-process runners
                where the child process emits the events itself.
        """
        self.asset_executions[asset.id].mark_failed(error, tb=tb)
        canceled = self._propagate_failure(asset.id)

        if emit:
            metadata: dict[str, Any] = {
                **self._asset_event_metadata(asset),
                "error": error,
                "message": f"Asset '{type(asset).key}' failed: {error}",
            }
            if tb:
                metadata["traceback"] = tb
            EventBus.emit(EventType.ASSET_FAILED, metadata=metadata)

            for key in canceled:
                canceled_asset = self.dag.asset_map[key]
                EventBus.emit(
                    EventType.ASSET_CANCELED,
                    metadata={
                        **self._asset_event_metadata(canceled_asset),
                        "message": f"Asset '{type(self.dag.asset_map[key]).key}' canceled (upstream failure)",
                    },
                )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _initialize_assets(self) -> None:
        """Initialize all assets as QUEUED, then promote root assets to READY."""
        for asset in self.dag.assets:
            status = ExecutionStatus.SKIPPED if not asset.materializable else ExecutionStatus.QUEUED
            self.asset_executions[asset.id] = AssetExecutionInfo(
                asset_id=asset.id,
                asset_key=type(asset).key,
                status=status,
            )

        # Promote assets whose predecessors are all skipped (or empty)
        for asset in self.dag.assets:
            info = self.asset_executions[asset.id]
            if info.status != ExecutionStatus.QUEUED:
                continue
            preds = self.dag.predecessors.get(asset.id, [])
            if all(self.asset_executions[p].status == ExecutionStatus.SKIPPED for p in preds):
                info.status = ExecutionStatus.READY

    def _assets_with_status(self, status: ExecutionStatus) -> list[Asset]:
        """Return all assets matching the given execution status."""
        return [asset for asset in self.dag.assets if self.asset_executions[asset.id].status == status]

    def _asset_event_metadata(self, asset: Asset) -> dict[str, Any]:
        """Build event metadata for an asset state transition.

        Returns:
            Dictionary of metadata keys for the event.
        """
        meta: dict[str, Any] = {
            **self.metadata,
            "asset_id": asset.id,
            "asset_key": type(asset).key,
            "partition_or_window": str(self.partition_or_window) if self.partition_or_window else None,
        }
        if asset.source is not None:
            meta["source_id"] = asset.source.id
        return meta

    def _promote_dependents(self, completed_key: str) -> None:
        """Promote queued successors to READY if all their predecessors are done."""
        completed_keys = {
            k
            for k, info in self.asset_executions.items()
            if info.status in (ExecutionStatus.COMPLETED, ExecutionStatus.SKIPPED)
        }
        for successor_key in self.dag.successors.get(completed_key, []):
            info = self.asset_executions[successor_key]
            if info.status != ExecutionStatus.QUEUED:
                continue
            preds = self.dag.predecessors.get(successor_key, [])
            if all(p in completed_keys for p in preds):
                info.status = ExecutionStatus.READY

    def _propagate_failure(self, failed_key: str) -> list[str]:
        """Recursively mark all downstream dependents as CANCELED.

        Returns:
            List of asset keys that were canceled.
        """
        canceled: list[str] = []
        for successor_key in self.dag.successors.get(failed_key, []):
            info = self.asset_executions[successor_key]
            if info.is_terminal:
                continue
            info.mark_canceled()
            canceled.append(successor_key)
            canceled.extend(self._propagate_failure(successor_key))
        return canceled
