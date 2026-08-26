"""The launcher forwards configured env vars into every run container.

Runs hydrate connections, so the container needs the same runtime-resolved
credentials (e.g. the in-house OAuth provider trio) the API resolves from
its environment; ``forward_env`` copies them from the launcher's own env.
"""

from __future__ import annotations

from typing import Any

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
