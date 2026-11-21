"""Tests for ``interloper_app.dev.process``."""

from __future__ import annotations

import signal
import subprocess
import sys
import time
from pathlib import Path

from interloper_app.dev.process import ProcessGroup


def test_a_command_reads_the_bytes_it_is_given(tmp_path: Path):
    group = ProcessGroup()
    out = tmp_path / "out"

    group.start([sys.executable, "-c", f"import sys; open({str(out)!r}, 'w').write(sys.stdin.read())"], stdin=b"spec")

    assert group.wait() == 0
    assert out.read_text() == "spec"
    assert group.running is False


def test_stop_ends_the_whole_group_and_is_idempotent():
    group = ProcessGroup()
    group.start(["sh", "-c", "sleep 30 & sleep 30"])
    assert group.running

    group.stop(timeout=5)

    assert group.running is False
    group.stop()


def test_nothing_started_waits_for_nothing():
    assert ProcessGroup().wait() == 0


def test_a_group_ignoring_the_stop_is_killed_after_the_timeout():
    group = ProcessGroup()
    group.start(["sh", "-c", "trap '' TERM; sleep 30"])
    time.sleep(0.3)

    group.stop(timeout=0.5)

    assert group.running is False


def test_signalling_a_group_that_already_ended_is_harmless():
    process = subprocess.Popen(["true"], start_new_session=True)
    process.wait()

    ProcessGroup._signal(process, signal.SIGTERM)
