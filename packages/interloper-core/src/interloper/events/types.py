"""Event type and log level enumerations."""

from __future__ import annotations

from enum import Enum


class EventType(Enum):
    """Enumeration of all framework lifecycle event types.

    Organized by scope:

    - **Operation lifecycle**: started/completed/failed/canceled (managed by Runner).
    - **Asset data**: the ``data()`` call itself.
    - **Destination I/O**: individual read/write operations.
    - **Run / Backfill**: higher-level orchestration.
    - **User logging**: messages emitted via ``context.logger``.
    """

    # Hooks (recorded by the hook evaluator)
    HOOK_FIRED = "hook_fired"
    HOOK_FAILED = "hook_failed"

    # Operation lifecycle (managed by Runner)
    OPERATION_QUEUED = "operation_queued"
    OPERATION_STARTED = "operation_started"
    OPERATION_COMPLETED = "operation_completed"
    OPERATION_FAILED = "operation_failed"
    OPERATION_CANCELED = "operation_canceled"

    # Asset data (the data() call)
    ASSET_DATA_STARTED = "asset_data_started"
    ASSET_DATA_COMPLETED = "asset_data_completed"
    ASSET_DATA_FAILED = "asset_data_failed"

    # Destination I/O
    DEST_READ_STARTED = "dest_read_started"
    DEST_READ_COMPLETED = "dest_read_completed"
    DEST_READ_FAILED = "dest_read_failed"
    DEST_WRITE_STARTED = "dest_write_started"
    DEST_WRITE_COMPLETED = "dest_write_completed"
    DEST_WRITE_FAILED = "dest_write_failed"

    # Run orchestration
    RUN_STARTED = "run_started"
    RUN_COMPLETED = "run_completed"
    RUN_FAILED = "run_failed"

    # Backfill orchestration
    BACKFILL_STARTED = "backfill_started"
    BACKFILL_COMPLETED = "backfill_completed"
    BACKFILL_FAILED = "backfill_failed"

    # User logging
    LOG = "log"
