"""Tests for ``interloper_scheduler.services``."""

from __future__ import annotations

from typing import Any

import pytest
from interloper.catalog import Catalog
from interloper.settings import AppSettings, RenewalSettings

import interloper_scheduler.services as services_module
from interloper_scheduler import scheduler_services


@pytest.fixture(autouse=True)
def _no_launcher(monkeypatch: pytest.MonkeyPatch) -> None:
    """Build a stand-in launcher instead of reading the launcher settings."""
    monkeypatch.setattr(services_module.Launcher, "from_settings", classmethod(lambda cls, *args, **kwargs: object()))


def _build(settings: AppSettings | None = None, **toggles: bool) -> dict[str, Any]:
    """Build the services with a stand-in store.

    Args:
        settings: The settings; defaults apply when ``None``.
        **toggles: The ``cron``, ``worker`` and ``reaper`` flags.

    Returns:
        The built services, by name.
    """
    return scheduler_services(settings or AppSettings(), store=object(), catalog=Catalog(), **toggles)  # ty: ignore[invalid-argument-type]


def test_cron_brings_hooks_and_renewal():
    """Cron runs with the hook and renewal controllers, the other cluster singletons."""
    assert set(_build(cron=True, worker=False, reaper=False)) == {"cron", "hooks", "renewal"}


def test_renewal_follows_its_setting():
    """Renewal stays off when its section disables it."""
    settings = AppSettings(renewal=RenewalSettings(enabled=False))

    assert set(_build(settings, cron=True, worker=False, reaper=False)) == {"cron", "hooks"}


def test_worker_and_reaper_share_one_launcher():
    """The queue worker and the reaper dispatch and cancel through the same launcher."""
    built = _build(cron=False, worker=True, reaper=True)

    assert set(built) == {"worker", "reaper"}
    assert built["worker"]._launcher is built["reaper"]._launcher


def test_nothing_enabled_builds_nothing():
    """With every toggle off there is nothing to run."""
    assert _build(cron=False, worker=False, reaper=False) == {}
