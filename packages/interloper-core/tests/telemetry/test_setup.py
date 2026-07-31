"""Tests for ``interloper.telemetry.setup``."""

from __future__ import annotations

import sys

import pytest

from interloper.settings import TelemetrySettings
from interloper.telemetry import setup
from interloper.telemetry.setup import _exporter_kwargs, _parse_headers, init_telemetry, shutdown_telemetry


@pytest.fixture(autouse=True)
def _reset_state():
    yield
    shutdown_telemetry()


class TestInitTelemetry:
    def test_disabled_is_a_noop(self):
        assert init_telemetry(TelemetrySettings(enabled=False), role="cli") is False
        assert setup._initialized is False

    def test_missing_sdk_warns_and_stays_noop(self, monkeypatch, caplog):
        # A None sys.modules entry makes the SDK import raise ImportError,
        # simulating an install without the otel extra.
        monkeypatch.setitem(sys.modules, "opentelemetry.sdk.resources", None)
        with caplog.at_level("WARNING", logger="interloper.telemetry.setup"):
            assert init_telemetry(TelemetrySettings(enabled=True), role="cli") is False
        assert "interloper[otel]" in caplog.text
        assert setup._initialized is False

    def test_enabled_is_idempotent(self):
        # Both signals off: initialization completes without installing
        # global providers (which are set-once per process).
        settings = TelemetrySettings(enabled=True, traces=False, metrics=False)
        assert init_telemetry(settings, role="run") is True
        assert init_telemetry(settings, role="run") is True
        assert setup._initialized is True

    def test_shutdown_without_init_is_safe(self):
        shutdown_telemetry()
        assert setup._initialized is False

    def test_instrument_fastapi_is_a_noop_when_uninitialized(self):
        setup.instrument_fastapi(object())  # must not raise or touch the app


class TestExporterConfig:
    def test_parse_headers(self):
        assert _parse_headers("") is None
        assert _parse_headers("a=1,b=2") == {"a": "1", "b": "2"}
        assert _parse_headers("token=abc=def") == {"token": "abc=def"}

    def test_kwargs_only_carry_configured_fields(self):
        assert _exporter_kwargs(TelemetrySettings()) == {}
        kwargs = _exporter_kwargs(TelemetrySettings(endpoint="http://collector:4317", headers="x=y"))
        assert kwargs == {"endpoint": "http://collector:4317", "headers": {"x": "y"}}
