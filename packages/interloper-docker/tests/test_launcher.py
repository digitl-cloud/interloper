"""The launcher forwards configured env vars into every run container.

Runs hydrate connections, so the container needs the same runtime-resolved
credentials (e.g. the in-house OAuth provider trio) the API resolves from
its environment; ``forward_env`` copies them from the launcher's own env.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from interloper.catalog.base import Catalog

import interloper_docker.launcher as launcher_module
from interloper_docker.launcher import DockerLauncher


def _launcher(monkeypatch: pytest.MonkeyPatch, **kwargs: Any) -> DockerLauncher:
    monkeypatch.setattr(launcher_module.docker, "from_env", lambda: object())
    return DockerLauncher(
        catalog=Catalog(),
        postgres_host="db",
        postgres_port=5432,
        postgres_user="user",
        postgres_password="password",
        postgres_database="interloper",
        **kwargs,
    )


def test_forward_env_copies_set_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    """Named variables set in the launcher's env reach the container; unset ones are skipped."""
    monkeypatch.setenv("INTERLOPER_FACEBOOK_CLIENT_ID", "abc")
    monkeypatch.delenv("INTERLOPER_FACEBOOK_CLIENT_SECRET", raising=False)
    launcher = _launcher(
        monkeypatch,
        forward_env=["INTERLOPER_FACEBOOK_CLIENT_ID", "INTERLOPER_FACEBOOK_CLIENT_SECRET"],
    )

    environment = launcher._build_environment()

    assert environment["INTERLOPER_FACEBOOK_CLIENT_ID"] == "abc"
    assert "INTERLOPER_FACEBOOK_CLIENT_SECRET" not in environment


def test_no_forward_env_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without ``forward_env`` the environment stays the built-in allowlist."""
    monkeypatch.setenv("INTERLOPER_FACEBOOK_CLIENT_ID", "abc")
    launcher = _launcher(monkeypatch)

    assert "INTERLOPER_FACEBOOK_CLIENT_ID" not in launcher._build_environment()


class _Container:
    short_id = "abc123"

    def __init__(self, state: dict[str, Any], *, reload_fails: bool = False) -> None:
        self.attrs = {"State": state}
        self.status = state.get("Status")
        self._reload_fails = reload_fails

    def reload(self) -> None:
        if self._reload_fails:
            raise RuntimeError("daemon unreachable")


def _diagnose(monkeypatch: pytest.MonkeyPatch, container: _Container | None) -> str | None:
    launcher = _launcher(monkeypatch)

    def get(_name: str) -> _Container:
        if container is None:
            raise RuntimeError("no such container")
        return container

    launcher._client = SimpleNamespace(containers=SimpleNamespace(get=get))
    return launcher.diagnose(uuid4())


@pytest.mark.parametrize(
    "container",
    [None, _Container({"Status": "running"}, reload_fails=True), _Container({"Status": "running"})],
)
def test_a_running_or_unreadable_container_has_no_diagnosis(
    monkeypatch: pytest.MonkeyPatch, container: _Container | None
) -> None:
    assert _diagnose(monkeypatch, container) is None


def test_a_stopped_container_reports_why(monkeypatch: pytest.MonkeyPatch) -> None:
    """The diagnosis carries the exit code, the OOM kill and the daemon's error."""
    container = _Container({"Status": "dead", "ExitCode": 137, "OOMKilled": True, "Error": "killed"})

    diagnosis = _diagnose(monkeypatch, container)

    assert diagnosis == "container status=dead exit_code=137 OOMKilled error=killed"


def test_the_heartbeat_settings_reach_the_run_container(monkeypatch: pytest.MonkeyPatch) -> None:
    environment = _launcher(monkeypatch, heartbeat_interval=5, heartbeat_timeout=45)._build_environment()

    assert environment["INTERLOPER_REAPER_HEARTBEAT_INTERVAL"] == "5"
    assert environment["INTERLOPER_REAPER_HEARTBEAT_TIMEOUT"] == "45"
