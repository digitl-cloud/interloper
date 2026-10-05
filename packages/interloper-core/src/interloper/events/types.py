"""Event type and log level enumerations."""

from __future__ import annotations

from enum import Enum


class EventType(Enum):
    """Every framework lifecycle event type.

    Each event carries ``id``, ``type``, ``timestamp`` and ``metadata``. The
    metadata noted on a member comes on top of the run-level metadata
    (``run_id``, ``backfill_id``, anything the caller passed) that every
    event inherits:

    - operation events carry ``component_id``, ``component_kind``,
      ``component_key``, ``source_id``, ``partition_or_window`` and
      ``message``;
    - asset data events add ``qualified_key`` to those;
    - destination I/O events add ``destination_key``;
    - failures add ``error`` and, when the operation captures tracebacks,
      ``traceback``.

    The backfill and hook types are defined here so every consumer shares
    one vocabulary, but the scheduler produces them, not the core runners.
    """

    HOOK_FIRED = "hook_fired"
    """A hook's ``fire()`` ran. Recorded by the hook evaluator."""
    HOOK_FAILED = "hook_failed"
    """A hook's ``fire()`` raised. Recorded by the hook evaluator."""

    OPERATION_QUEUED = "operation_queued"
    """At run start, once for every enabled operation."""
    OPERATION_STARTED = "operation_started"
    """The operation was submitted to the runner."""
    OPERATION_COMPLETED = "operation_completed"
    """``execute()`` returned."""
    OPERATION_FAILED = "operation_failed"
    """``execute()`` raised."""
    OPERATION_RETRIED = "operation_retried"
    """``execute()`` raised and the operation's retry policy granted another attempt."""
    OPERATION_CANCELED = "operation_canceled"
    """An upstream operation failed, or the run ended early (fail-fast, machinery abort) before submission."""

    ASSET_DATA_STARTED = "asset_data_started"
    """Before the asset's ``data()`` call."""
    ASSET_DATA_COMPLETED = "asset_data_completed"
    """After ``data()`` returned."""
    ASSET_DATA_FAILED = "asset_data_failed"
    """``data()`` raised."""

    DEST_READ_STARTED = "dest_read_started"
    """Before reading an upstream dependency from a destination."""
    DEST_READ_COMPLETED = "dest_read_completed"
    """The upstream read returned."""
    DEST_READ_FAILED = "dest_read_failed"
    """The upstream read raised."""
    DEST_WRITE_STARTED = "dest_write_started"
    """Before writing the asset's result to a destination."""
    DEST_WRITE_COMPLETED = "dest_write_completed"
    """The write returned."""
    DEST_WRITE_FAILED = "dest_write_failed"
    """The write raised."""

    RUN_STARTED = "run_started"
    """The walk begins. Carries ``partition_or_window`` and ``message``."""
    RUN_COMPLETED = "run_completed"
    """Every operation completed or was skipped."""
    RUN_FAILED = "run_failed"
    """At least one operation failed, or the walk itself broke; ``error`` is set for walk failures only."""

    BACKFILL_STARTED = "backfill_started"
    """A multi-partition backfill began."""
    BACKFILL_COMPLETED = "backfill_completed"
    """Every partition of the backfill ran."""
    BACKFILL_FAILED = "backfill_failed"
    """The backfill ended with failed partitions."""

    LOG = "log"
    """A message from ``context.logger.<level>(...)`` or an ``EventLogger``; carries ``level`` and ``message``."""
