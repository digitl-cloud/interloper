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
        def streamable_http_app(self, **kwargs: Any) -> str:
            return "streamable-http app"

    def serve(app: str, **kwargs: Any) -> None:
        recorded["served"] = app
        recorded["uvicorn"] = kwargs

    monkeypatch.setattr(sys, "argv", ["interloper-mcp"])
    monkeypatch.setattr(main_module.Catalog, "from_settings", classmethod(lambda cls: main_module.Catalog()))
    monkeypatch.setattr(main_module.Store, "from_settings", classmethod(lambda cls, catalog=None: object()))
    monkeypatch.setattr(main_module, "init_context", lambda store, catalog: None)
    monkeypatch.setattr(main_module, "create_mcp_server", lambda public_url, store: FakeServer())
    monkeypatch.setattr(main_module, "http_options", lambda settings: {})
    monkeypatch.setattr(main_module.uvicorn, "run", serve)
    monkeypatch.setattr(
        main_module, "configure_console_logging", lambda **kwargs: recorded.update(log_format=kwargs["log_format"])
    )
    return recorded


def test_the_server_logs_in_the_one_console_format(served: dict[str, Any]) -> None:
    main_module.main()

    assert (served["log_format"], served["served"]) == ("text", "streamable-http app")


def test_uvicorn_adds_no_handler_or_access_line_of_its_own(served: dict[str, Any]) -> None:
    """Uvicorn's loggers follow the console's rule, so JSON logs hold no plain text line per request."""
    main_module.main()

    assert {name: served["uvicorn"][name] for name in ("log_config", "log_level", "access_log")} == {
        "log_config": None,
        "log_level": None,
        "access_log": False,
    }


def test_the_server_logs_json_when_the_settings_ask(served: dict[str, Any]) -> None:
    LogSettings.activate(LogSettings(format="json"))

    main_module.main()

    assert served["log_format"] == "json"
