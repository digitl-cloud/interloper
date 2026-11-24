"""Tests for ``interloper_scheduler.controller``."""

from __future__ import annotations

import logging
import time

from interloper_scheduler import controller
from interloper_scheduler.controller import Controller


class _StubLoopController(Controller):
    def __init__(self) -> None:
        super().__init__(poll_interval=0)

    def _tick(self) -> None:
        self.stop()


class _FailingLoopController(Controller):
    def __init__(self) -> None:
        super().__init__(poll_interval=0)
        self.ticks = 0

    def _tick(self) -> None:
        self.ticks += 1
        if self.ticks >= 2:
            self.stop()
        raise RuntimeError("boom")


class TestTickLiveness:
    def test_successful_tick_records_timestamp(self):
        before = time.time()
        _StubLoopController().start()
        assert controller._last_tick["_stubloop"] >= before

    def test_failing_ticks_never_record(self):
        controller._last_tick.pop("_failingloop", None)
        _FailingLoopController().start()
        assert "_failingloop" not in controller._last_tick

    def test_loop_name_derivation(self):
        assert _StubLoopController()._loop_name == "_stubloop"

    def test_gauge_reports_per_loop(self, metric_reader):
        _StubLoopController().start()
        data = metric_reader.get_metrics_data()
        points = [
            (point.attributes.get("loop"), point.value)
            for rm in data.resource_metrics
            for sm in rm.scope_metrics
            for metric in sm.metrics
            if metric.name == "interloper.scheduler.tick"
            for point in metric.data.data_points
        ]
        loops = dict(points)
        assert "_stubloop" in loops
        assert loops["_stubloop"] > 0


class _InterruptingController(Controller):
    """Raises ``KeyboardInterrupt`` from its tick, as Ctrl-C would."""

    def __init__(self) -> None:
        super().__init__(poll_interval=0)

    def _tick(self) -> None:
        raise KeyboardInterrupt


class TestKeyboardInterrupt:
    """Ctrl-C ends the loop cleanly rather than surfacing a traceback."""

    def test_the_loop_returns_and_says_so(self, caplog):
        with caplog.at_level("INFO", logger="interloper_scheduler.controller"):
            _InterruptingController().start()

        assert "Shutting down _InterruptingController" in caplog.text


class _ReadyProbe(Controller):
    """Records whether it was ready while it ticked, then stops."""

    def __init__(self) -> None:
        super().__init__(poll_interval=0)
        self.ready_while_ticking: bool | None = None

    def _tick(self) -> None:
        self.stop()
        self.ready_while_ticking = self.ready


class TestReady:
    def test_a_controller_is_ready_once_its_loop_runs(self):
        probe = _ReadyProbe()
        assert probe.ready is False

        probe.start()

        assert probe.ready_while_ticking is True

    def test_starting_is_a_debug_line(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="interloper_scheduler.controller"):
            _ReadyProbe().start()

        (record,) = [record for record in caplog.records if record.getMessage().startswith("Starting")]
        assert record.levelno == logging.DEBUG
