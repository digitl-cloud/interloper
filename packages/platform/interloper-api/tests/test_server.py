"""Tests for ``interloper_api.server``."""

from __future__ import annotations

import gc
import socket
import threading
from collections.abc import Iterator

import pytest
import sqlalchemy
from interloper.catalog import Catalog
from interloper.settings import ServerSettings
from interloper.utils import Supervisor
from interloper_db import Store

from interloper_api.server import ApiServer
from interloper_api.settings import ApiSettings


def _store() -> Store:
    return Store(Catalog(), engine=sqlalchemy.create_engine("sqlite://"))


@pytest.fixture(autouse=True)
def _unfreeze() -> Iterator[None]:
    """Hand the test worker's objects back to the collector after a test that started a server."""
    yield
    gc.unfreeze()


def test_start_freezes_what_the_process_holds_before_it_serves(monkeypatch: pytest.MonkeyPatch):
    """A server starts serving with everything loaded so far out of the collector's reach."""
    server = ApiServer.from_settings(ApiSettings.get(), store=_store(), catalog=Catalog(), port=3101)
    frozen_when_serving: list[int] = []
    monkeypatch.setattr(server._server, "run", lambda: frozen_when_serving.append(gc.get_freeze_count()))

    server.start()

    assert frozen_when_serving and frozen_when_serving[0] > 0


def test_binds_the_settings_host_and_the_given_port():
    """The bind host comes from ``server.host``; the port is the one the command chose."""
    ServerSettings.activate(ServerSettings(host="127.0.0.1", port=3000))
    settings = ApiSettings.get()

    server = ApiServer.from_settings(settings, store=_store(), catalog=Catalog(), port=3101)

    assert (server._server.config.host, server._server.config.port) == ("127.0.0.1", 3101)


def test_uvicorn_logs_no_lines_of_its_own_at_info():
    """Uvicorn's loggers follow the library rule, and its access log gives way to the API's own request line."""
    server = ApiServer.from_settings(ApiSettings.get(), store=_store(), catalog=Catalog(), port=3101)

    assert server._server.config.log_level is None
    assert server._server.config.access_log is False


def test_ready_once_uvicorn_listens():
    """The server is ready when uvicorn has bound its socket, not when its thread starts."""
    server = ApiServer.from_settings(ApiSettings.get(), store=_store(), catalog=Catalog(), port=3101)
    assert server.ready is False

    server._server.started = True

    assert server.ready is True


def test_stop_asks_uvicorn_to_exit():
    """``stop`` lets uvicorn finish its requests and return from ``start``."""
    server = ApiServer.from_settings(ApiSettings.get(), store=_store(), catalog=Catalog(), port=3101)

    server.stop()

    assert server._server.should_exit is True


def test_a_bound_port_fails_the_process_instead_of_hanging():
    """Uvicorn exits its thread when it cannot bind; the supervisor turns that into a failed process."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as occupant:
        occupant.bind(("127.0.0.1", 0))
        occupant.listen(1)
        port = occupant.getsockname()[1]
        ServerSettings.activate(ServerSettings(host="127.0.0.1", port=port))
        settings = ApiSettings.get()
        server = ApiServer.from_settings(settings, store=_store(), catalog=Catalog(), port=port)
        outcome: list[object] = []

        def run() -> None:
            try:
                Supervisor({"api": server}, grace=1.0).run()
            except SystemExit as error:
                outcome.append(error.code)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        thread.join(timeout=15)

    assert not thread.is_alive(), "the API process hung on a port it could not bind"
    assert outcome == [1]
