"""``interloper launch``: execute a platform run by its id, as a launcher or a container runner starts it.

The process holds no database. With the run's token (`RunSettings`,
``INTERLOPER_RUN_TOKEN``) it loads what the run executes from the API at
``server.internal_url`` (`RunSpec`: the DAG, its connections resolved, the
run's partition and the metadata every event carries) and reports the run's
events back there (`EventRecorder` over `EventBatch.post`). A run ended
elsewhere, or one that can no longer report, exits the process.

``--operation-id <id>`` executes one operation of the run for its host, the
process that executes the run and started this one in its own container (the
Docker and Kubernetes runners): it reports only what happens inside the
operation, since the host owns its lifecycle.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from functools import partial

logger = logging.getLogger(__name__)


def register(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``launch`` command.

    Args:
        subparsers: The root subparsers action to attach to.
    """
    parser = subparsers.add_parser(
        "launch",
        help="Execute a platform run by its id, as a launcher starts it",
        description="Execute a platform run by its id: its DAG, partition and metadata are loaded from the API and "
        "its events reported back, with the run's token (INTERLOPER_RUN_TOKEN), as the launcher or container runner "
        "that started the process sets it. The exit status is non-zero when the run cannot be loaded, is lost, or "
        "did not complete.",
    )
    parser.add_argument("run_id", help="The platform run to execute")
    parser.add_argument(
        "--operation-id",
        default=None,
        help="Execute only this operation of the run, once, its upstreams read rather than executed, for the host "
        "that executes the run",
    )
    parser.set_defaults(handler=_cmd_launch)


def _cmd_launch(args: argparse.Namespace) -> None:
    """Load a platform run from the API, execute it, and report its events back.

    Args:
        args: Parsed CLI arguments, carrying ``run_id`` and ``operation_id``.

    Raises:
        SystemExit: Without the run's token, when the run cannot be loaded,
            is lost, or did not complete.
    """
    from interloper.cli.commands.execution import configure_logging, execute, for_operation, leave_failure
    from interloper.dag import DAG
    from interloper.errors import RunnerError, format_exception
    from interloper.events import Event, EventBatch, EventRecorder, EventType
    from interloper.rest import HTTPBearerAuth, RESTClient
    from interloper.runner import Runner
    from interloper.runner.spec import RunSpec
    from interloper.settings import AppSettings

    configure_logging()
    settings = AppSettings.get()
    if not settings.run.token:
        raise SystemExit(
            "Error: launch executes a platform run, which needs the run's token (INTERLOPER_RUN_TOKEN) "
            "and the API it belongs to (server.internal_url)."
        )
    client = RESTClient(settings.server.internal_url, auth=HTTPBearerAuth(settings.run.token), timeout=30)
    recorder = EventRecorder(
        partial(EventBatch.post, client=client, run_id=args.run_id),
        run_id=args.run_id,
        heartbeat_interval=settings.reaper.heartbeat_interval,
        heartbeat_timeout=settings.reaper.heartbeat_timeout,
        on_lost=_exit_lost,
        component_id=args.operation_id,
    )

    try:
        try:
            spec = RunSpec.from_api(client, args.run_id, timeout=settings.reaper.heartbeat_timeout / 2)
        except RunnerError as error:
            raise SystemExit(f"Error: {error}") from error
        dag, window = DAG.from_spec(spec.dag), spec.window
        if args.operation_id is not None:
            dag, window = for_operation(dag, window, args.operation_id)
        runner = Runner.from_settings(settings.runner)
    except (Exception, SystemExit) as error:
        reason = str(error) if isinstance(error, SystemExit) else format_exception(error)
        if args.operation_id is not None:
            leave_failure(args.operation_id, reason)
        else:
            ending = {"run_id": args.run_id, "error": reason, "message": f"Run failed: {reason}"}
            with recorder:
                recorder(Event(type=EventType.RUN_FAILED, metadata=ending))
        raise

    execute(runner, dag, window, spec.metadata, operation_id=args.operation_id, recorder=recorder)


def _exit_lost() -> None:
    """Exit this run's process at once: its run was ended elsewhere, or can no longer report.

    The process exists for this one run, and an operation blocked in sync
    code cannot be interrupted any other way. The recorder has delivered
    what it held before calling this.
    """
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(1)
