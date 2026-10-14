"""Tests for ``interloper_api.server``."""

from __future__ import annotations

import socket
import threading

from interloper.catalog import Catalog
from interloper.settings import AppSettings, ServerSettings
from interloper.utils import Supervisor

from interloper_api import ApiServer


def test_binds_the_settings_host_and_the_given_port():
    """The bind host comes from ``server.host``; the port is the one the command chose."""
    settings = AppSettings(server=ServerSettings(host="127.0.0.1", port=3000))

    server = ApiServer.from_settings(settings, store=None, catalog=Catalog(), port=3101)  # ty: ignore[invalid-argument-type]

    assert (server._server.config.host, server._server.config.port) == ("127.0.0.1", 3101)


def test_stop_asks_uvicorn_to_exit():
    """``stop`` lets uvicorn finish its requests and return from ``start``."""
    server = ApiServer.from_settings(AppSettings(), store=None, catalog=Catalog(), port=3101)  # ty: ignore[invalid-argument-type]

    server.stop()

    assert server._server.should_exit is True


def test_a_bound_port_fails_the_process_instead_of_hanging():
    """Uvicorn exits its thread when it cannot bind; the supervisor turns that into a failed process."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as occupant:
        occupant.bind(("127.0.0.1", 0))
        occupant.listen(1)
        port = occupant.getsockname()[1]
        settings = AppSettings(server=ServerSettings(host="127.0.0.1", port=port))
        server = ApiServer.from_settings(settings, store=None, catalog=Catalog(), port=port)  # ty: ignore[invalid-argument-type]
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
