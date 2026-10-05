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
from interloper_scheduler.launcher import LaunchStatus

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


def _describe(monkeypatch: pytest.MonkeyPatch, container: _Container | None) -> Any:
    launcher = _launcher(monkeypatch)

    def get(_name: str) -> _Container:
        if container is None:
            raise RuntimeError("no such container")
        return container

    launcher._client = SimpleNamespace(containers=SimpleNamespace(get=get))
    return launcher.describe_run(uuid4())


@pytest.mark.parametrize(
    ("container", "status"),
    [
        (None, LaunchStatus.NOT_FOUND),
        (_Container({"Status": "running"}, reload_fails=True), LaunchStatus.NOT_FOUND),
        (_Container({"Status": "running"}), LaunchStatus.RUNNING),
        (_Container({"Status": "exited", "ExitCode": 0}), LaunchStatus.SUCCEEDED),
        (_Container({"Status": "exited", "ExitCode": 137}), LaunchStatus.FAILED),
    ],
)
def test_describe_run_maps_the_container_state(
    monkeypatch: pytest.MonkeyPatch, container: _Container | None, status: LaunchStatus
) -> None:
    """The reaper reads the container's state as one launch status."""
    assert _describe(monkeypatch, container).status is status


def test_a_failed_container_reports_why(monkeypatch: pytest.MonkeyPatch) -> None:
    """The failure carries the exit code, the OOM kill and the daemon's error."""
    container = _Container({"Status": "dead", "ExitCode": 137, "OOMKilled": True, "Error": "killed"})

    error = _describe(monkeypatch, container).error

    assert error == "Container abc123 status=dead exit_code=137 OOMKilled error=killed"
