"""Tests for ``interloper_docker.launcher``: one container per run, its spec as its argument, no database in it."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from interloper.catalog import Catalog
from interloper.dag import DAGSpec
from interloper_db import RunStatus
from interloper_db.models import Run

import interloper_docker.launcher as launcher_module
from interloper_docker.launcher import DockerLauncher


def _launcher(monkeypatch: pytest.MonkeyPatch, **kwargs: Any) -> DockerLauncher:
    monkeypatch.setattr(launcher_module.docker, "from_env", lambda: object())
    return DockerLauncher(catalog=Catalog(), internal_url="http://api", **kwargs)


def test_forward_env_copies_set_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    """Named variables set in the launcher's env reach the container; unset ones are skipped."""
    monkeypatch.setenv("INTERLOPER_FACEBOOK_CLIENT_ID", "abc")
    monkeypatch.delenv("INTERLOPER_FACEBOOK_CLIENT_SECRET", raising=False)
    launcher = _launcher(
        monkeypatch,
        forward_env=["INTERLOPER_FACEBOOK_CLIENT_ID", "INTERLOPER_FACEBOOK_CLIENT_SECRET"],
    )

    environment = launcher._build_environment("ilr_x")

    assert environment["INTERLOPER_FACEBOOK_CLIENT_ID"] == "abc"
    assert "INTERLOPER_FACEBOOK_CLIENT_SECRET" not in environment


def test_no_forward_env_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without ``forward_env`` the environment stays the built-in allowlist."""
    monkeypatch.setenv("INTERLOPER_FACEBOOK_CLIENT_ID", "abc")
    launcher = _launcher(monkeypatch)

    assert "INTERLOPER_FACEBOOK_CLIENT_ID" not in launcher._build_environment("ilr_x")


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
    environment = _launcher(monkeypatch, heartbeat_interval=5, heartbeat_timeout=45)._build_environment("ilr_x")

    assert environment["INTERLOPER_REAPER_HEARTBEAT_INTERVAL"] == "5"
    assert environment["INTERLOPER_REAPER_HEARTBEAT_TIMEOUT"] == "45"


class _CreatedContainer:
    """A created container recording whether it started and whether it was removed."""

    short_id = "abc123"

    def __init__(self, *, start_fails: bool = False) -> None:
        self.started = False
        self.removed = False
        self._start_fails = start_fails

    def start(self) -> None:
        if self._start_fails:
            raise RuntimeError("no such image")
        self.started = True

    def remove(self, force: bool = False) -> None:
        self.removed = force


def test_the_container_runs_the_spec_given_as_its_argument(monkeypatch: pytest.MonkeyPatch) -> None:
    launcher = _launcher(monkeypatch)
    created: list[dict[str, Any]] = []
    container = _CreatedContainer()

    def create(**kwargs: Any) -> _CreatedContainer:
        created.append(kwargs)
        return container

    launcher._client = SimpleNamespace(containers=SimpleNamespace(create=create))
    run = Run(id=uuid4(), org_id=uuid4(), status=RunStatus.DISPATCHED)

    launcher.launch(run, DAGSpec(items=[]), "ilr_x")

    (kwargs,) = created
    assert kwargs["command"][:4] == ["interloper", "run", "--format", "inline"]
    assert json.loads(kwargs["command"][4]) == {"items": []}
    assert kwargs["environment"]["INTERLOPER_RUN_TOKEN"] == "ilr_x"
    assert kwargs["environment"]["INTERLOPER_SERVER_INTERNAL_URL"] == "http://api"
    assert not [name for name in kwargs["environment"] if name.startswith("INTERLOPER_POSTGRES")]
    assert container.started


def test_a_container_that_cannot_start_is_removed_with_its_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    launcher = _launcher(monkeypatch)
    container = _CreatedContainer(start_fails=True)
    launcher._client = SimpleNamespace(containers=SimpleNamespace(create=lambda **kwargs: container))

    with pytest.raises(RuntimeError, match="no such image"):
        launcher.launch(Run(id=uuid4(), org_id=uuid4(), status=RunStatus.DISPATCHED), DAGSpec(items=[]), "ilr_x")

    assert container.removed
