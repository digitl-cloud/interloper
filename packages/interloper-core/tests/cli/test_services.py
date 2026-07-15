"""Tests for interloper.cli.services."""

import os
import signal
import socket
import subprocess
import time

from interloper.cli.services import _kill_process_group, _port_in_use


def test_port_in_use_detects_listener() -> None:
    """A live listener on the port is detected."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]
        assert _port_in_use(port) is True


def test_port_in_use_detects_ipv6_only_listener() -> None:
    """A listener bound only to ``::1`` (how Nuxt dev binds on macOS) is detected."""
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as server:
        server.bind(("::1", 0))
        server.listen(1)
        port = server.getsockname()[1]
        assert _port_in_use(port) is True


def test_port_in_use_free_port() -> None:
    """A free port reports not in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    assert _port_in_use(port) is False


def test_kill_process_group_kills_children() -> None:
    """The group kill reaches grandchildren that terminate() would orphan.

    Raises:
        AssertionError: If the grandchild survives the group kill.
    """
    # Mimics pnpm → nuxt: a shell whose child would survive a plain terminate().
    proc = subprocess.Popen(
        ["/bin/sh", "-c", "sleep 30 & echo $!; wait"],
        stdout=subprocess.PIPE,
        start_new_session=True,
    )
    assert proc.stdout is not None
    child_pid = int(proc.stdout.readline())

    _kill_process_group(proc, signal.SIGKILL)
    proc.wait(timeout=5)

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            os.kill(child_pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    raise AssertionError(f"grandchild {child_pid} still alive after killpg")


def test_kill_process_group_ignores_exited_process() -> None:
    """Killing an already-exited process is a no-op."""
    proc = subprocess.Popen(["/bin/sh", "-c", "exit 0"], start_new_session=True)
    proc.wait(timeout=5)
    _kill_process_group(proc, signal.SIGTERM)
