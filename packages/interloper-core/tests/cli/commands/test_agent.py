"""Tests for ``interloper.cli.commands.agent``."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import google.adk.cli.fast_api as adk_fast_api
import interloper_agent.context
import pytest
import uvicorn

from interloper.catalog import Catalog
from interloper.cli.commands import agent as agent_command


def _parse(*argv: str) -> argparse.Namespace:
    """Parse ``agent`` argv through the real registration.

    Args:
        argv: The argument vector after the ``interloper`` program name.

    Returns:
        The parsed namespace.
    """
    parser = argparse.ArgumentParser(prog="interloper")
    agent_command.register(parser.add_subparsers(dest="command"))
    return parser.parse_args(argv)


class TestRegister:
    """Parser wiring for the ``agent`` command."""

    def test_defaults_bind_to_loopback(self) -> None:
        args = _parse("agent")

        assert (args.host, args.port) == ("127.0.0.1", 8000)
        assert args.handler is agent_command._cmd_agent
        assert args.requires == ["interloper_agent"]

    def test_host_and_port_are_overridable(self) -> None:
        args = _parse("agent", "--host", "0.0.0.0", "--port", "9000")

        assert (args.host, args.port) == ("0.0.0.0", 9000)


class TestCmdAgent:
    """``interloper agent`` boots the ADK web UI against the initialized context."""

    def test_initializes_the_agent_context_before_serving(self, monkeypatch: pytest.MonkeyPatch) -> None:
        recorded: dict[str, Any] = {}
        app = object()

        monkeypatch.setattr(Catalog, "from_settings", classmethod(lambda cls: Catalog()))
        monkeypatch.setattr(
            interloper_agent.context,
            "init",
            lambda database_url, catalog: recorded.update(init=(database_url, catalog)),
        )
        monkeypatch.setattr(
            adk_fast_api,
            "get_fast_api_app",
            lambda **kwargs: (recorded.update(adk=kwargs), app)[1],
        )
        monkeypatch.setattr(uvicorn, "run", lambda served, **kwargs: recorded.update(uvicorn=(served, kwargs)))

        agent_command._cmd_agent(_parse("agent", "--host", "0.0.0.0", "--port", "9100"))

        assert recorded["init"][0].startswith("postgresql")
        assert isinstance(recorded["init"][1], Catalog)

        # ADK's AgentLoader lists agents by scanning a directory, so it must
        # be handed the parent of the interloper_agent package.
        agents_dir = Path(recorded["adk"]["agents_dir"])
        assert (agents_dir / "interloper_agent").is_dir()
        assert recorded["adk"]["web"] is True
        assert (recorded["adk"]["host"], recorded["adk"]["port"]) == ("0.0.0.0", 9100)

        served, uvicorn_kwargs = recorded["uvicorn"]
        assert served is app
        assert (uvicorn_kwargs["host"], uvicorn_kwargs["port"]) == ("0.0.0.0", 9100)
