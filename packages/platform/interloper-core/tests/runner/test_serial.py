"""Tests for ``interloper.runner.serial``."""

import interloper as il


def test_a_serial_runner_runs_one_operation_at_a_time():
    """Its capacity is one slot, and its run is the in-process run."""
    runner = il.SerialRunner()

    assert runner.capacity == 1
    assert il.SerialRunner.Run is il.AsyncRunner.Run
