"""Event type and log level enumerations."""

from __future__ import annotations

from enum import Enum


class EventType(Enum):
    """Enumeration of all framework lifecycle event types.

    Organized by scope:

    - **Asset lifecycle**: started/completed/failed/canceled (managed by Runner).
    - **Asset execution**: the ``data()`` call itself.
    - **Destination I/O**: individual read/write operations.
    - **Run / Backfill**: higher-level orchestration.
    - **User logging**: messages emitted via ``context.logger``.
    """

    # Hooks (recorded by the hook evaluator)
    HOOK_FIRED = "hook_fired"
    HOOK_FAILED = "hook_failed"

    # Asset lifecycle (managed by Runner)
    ASSET_QUEUED = "asset_queued"
    ASSET_STARTED = "asset_started"
    ASSET_COMPLETED = "asset_completed"
    ASSET_FAILED = "asset_failed"
    ASSET_CANCELED = "asset_canceled"

    # Asset execution (the data() call)
    ASSET_EXEC_STARTED = "asset_exec_started"
    ASSET_EXEC_COMPLETED = "asset_exec_completed"
    ASSET_EXEC_FAILED = "asset_exec_failed"

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
