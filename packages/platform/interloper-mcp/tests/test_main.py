"""Tests for ``interloper_mcp.main``."""

from __future__ import annotations

import sys
from typing import Any

import pytest
from interloper.settings import LogSettings

from interloper_mcp import main as main_module


@pytest.fixture
def served(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Stub what serving needs, recording how the console was set up and what was served.

    Args:
        monkeypatch: Fixture used to swap the collaborators.

    Returns:
        The console's settings and the transport served.
    """
    recorded: dict[str, Any] = {}

    class FakeServer:
        def run(self, transport: str, **kwargs: Any) -> None:
            recorded["transport"] = transport

    monkeypatch.setattr(sys, "argv", ["interloper-mcp"])
    monkeypatch.setattr(main_module.Catalog, "from_settings", classmethod(lambda cls: main_module.Catalog()))
    monkeypatch.setattr(main_module.Store, "from_settings", classmethod(lambda cls, catalog=None: object()))
    monkeypatch.setattr(main_module, "init_context", lambda store, catalog: None)
    monkeypatch.setattr(main_module, "create_mcp_server", lambda public_url, store: FakeServer())
    monkeypatch.setattr(main_module, "http_options", lambda settings: {})
    monkeypatch.setattr(
        main_module, "configure_console_logging", lambda **kwargs: recorded.update(log_format=kwargs["log_format"])
    )
    return recorded


def test_the_server_logs_in_the_one_console_format(served: dict[str, Any]) -> None:
    main_module.main()

    assert served == {"log_format": "text", "transport": "streamable-http"}


def test_the_server_logs_json_when_the_settings_ask(served: dict[str, Any]) -> None:
    LogSettings.activate(LogSettings(format="json"))

    main_module.main()

    assert served["log_format"] == "json"
