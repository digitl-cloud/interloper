"""Tests for ``interloper_docker.runner``."""

from __future__ import annotations

import datetime as dt
import json

import interloper as il
from interloper.partitioning import TimeGranularity, TimePartition, TimePartitionWindow

from interloper_docker.runner import DockerRunner

SPEC = {"items": []}


def _runner() -> DockerRunner:
    return DockerRunner(image="img", reraise=False, fail_fast=False)


class TestBuildCommand:
    def test_no_window_adds_no_flags(self):
        command = _runner()._build_command(SPEC, None, "r1")
        assert command == ["interloper", "run", "--format", "inline", "--run-id=r1", json.dumps(SPEC)]

    def test_one_partition_is_a_date(self):
        command = _runner()._build_command(SPEC, TimePartition(dt.date(2026, 1, 1)).window, "r1")
        assert command[-2:] == ["--date", "2026-01-01"]

    def test_an_hourly_partition_keeps_its_hour(self):
        partition = TimePartition(dt.datetime(2026, 1, 1, 13), TimeGranularity.HOUR)
        assert _runner()._build_command(SPEC, partition.window, "r1")[-2:] == ["--date", "2026-01-01T13"]

    def test_several_partitions_are_a_range(self):
        window = TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 3))
        command = _runner()._build_command(SPEC, window, "r1")
        assert command[-4:] == ["--start-date", "2026-01-01", "--end-date", "2026-01-03"]

    def test_an_unpartitioned_operation_gets_no_window(self):
        @il.asset
        def static() -> list:
            return []

        operation = static()
        assert operation.effective_window(TimePartition(dt.date(2026, 1, 1)).window) is None
