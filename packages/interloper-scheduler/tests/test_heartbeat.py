"""Tests for the run heartbeat (``interloper_scheduler.heartbeat``)."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

from interloper_scheduler.heartbeat import RunHeartbeat


class _Runs:
    """Answers ``heartbeat`` from a script, one answer per beat, the last one repeating."""

    def __init__(self, *answers: bool | Exception) -> None:
        self._answers = list(answers)
        self.beats = 0

    def heartbeat(self, run_id: UUID) -> bool:
        self.beats += 1
        answer = self._answers[0] if len(self._answers) == 1 else self._answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def _heartbeat(runs: _Runs, on_lost: Callable[[], None] | None, *, timeout: float = 1.0) -> RunHeartbeat:
    store: Any = SimpleNamespace(runs=runs)
    return RunHeartbeat(store, uuid4(), interval=0.01, timeout=timeout, on_lost=on_lost)


def _until(condition: Callable[[], bool], seconds: float = 2.0) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.005)
    return False


def test_a_running_run_keeps_beating_and_is_never_lost() -> None:
    runs, lost = _Runs(True), threading.Event()

    with _heartbeat(runs, lost.set):
        assert _until(lambda: runs.beats >= 3)

    assert not lost.is_set()


def test_a_run_ended_elsewhere_is_lost_and_stops_beating() -> None:
    runs, lost = _Runs(True, False), threading.Event()

    with _heartbeat(runs, lost.set):
        assert lost.wait(2)
        beats = runs.beats
        time.sleep(0.05)

    assert runs.beats == beats


def test_a_run_that_cannot_beat_for_half_the_timeout_is_lost() -> None:
    runs, lost = _Runs(ConnectionError("database unreachable")), threading.Event()
    started = time.monotonic()

    with _heartbeat(runs, lost.set, timeout=0.2):
        assert lost.wait(2)

    assert time.monotonic() - started >= 0.1


def test_a_transient_failure_is_retried() -> None:
    runs, lost = _Runs(ConnectionError("blip"), True), threading.Event()

    with _heartbeat(runs, lost.set, timeout=10):
        assert _until(lambda: runs.beats >= 3)

    assert not lost.is_set()


def test_without_on_lost_a_lost_run_only_stops_beating() -> None:
    runs = _Runs(False)

    with _heartbeat(runs, None):
        assert _until(lambda: runs.beats == 1)
        time.sleep(0.05)

    assert runs.beats == 1
