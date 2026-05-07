"""Runtime selection logic for ``interloper app``."""

from __future__ import annotations

import argparse
import socket
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from interloper.settings import AppSettings


def apply_cli_overrides(
    args: argparse.Namespace,
    settings: AppSettings,
) -> AppSettings:
    """Apply CLI-provided overrides into settings.

    Args:
        args: Parsed CLI arguments.
        settings: Loaded AppSettings instance.

    Returns:
        New AppSettings with CLI overrides applied.
    """
    server = settings.server
    if getattr(args, "host", None) is not None:
        server = server.model_copy(update={"host": args.host})
    if getattr(args, "port", None) is not None:
        server = server.model_copy(update={"port": args.port})
    if getattr(args, "api", None) is not None:
        server = server.model_copy(update={"enabled": bool(args.api)})

    cron = settings.cron
    if getattr(args, "cron", None) is not None:
        cron = cron.model_copy(update={"enabled": bool(args.cron)})

    worker = settings.worker
    if getattr(args, "worker", None) is not None:
        worker = worker.model_copy(update={"enabled": bool(args.worker)})

    reaper = settings.reaper
    if getattr(args, "reaper", None) is not None:
        reaper = reaper.model_copy(update={"enabled": bool(args.reaper)})

    return settings.model_copy(
        update={
            "server": server,
            "cron": cron,
            "worker": worker,
            "reaper": reaper,
        }
    )


def resolve_api_port(
    *,
    settings: AppSettings,
    run_api: bool,
    dev_mode: bool,
    explicit_port_arg: int | None,
) -> int:
    """Resolve effective API port for service startup.

    In dev mode, when no explicit ``--port`` is provided, the API binds to a
    random free port to avoid clashing with Nuxt on ``settings.server.port``.

    Returns:
        Effective API port for the API server.
    """
    if run_api and dev_mode and explicit_port_arg is None:
        return _find_free_port()
    return settings.server.port


def _find_free_port() -> int:
    """Bind to port 0 and let the OS assign a free port.

    Returns:
        An available TCP port number.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", 0))
        return sock.getsockname()[1]
