"""Tests for ``interloper_app.dev.port``."""

from __future__ import annotations

import socket
from typing import Any

import pytest

from interloper_app.dev.port import Port


def test_a_free_port_answers_nothing():
    port = Port.free()

    assert port.answers is False
    assert port.in_use is False
    assert port.origin == f"http://127.0.0.1:{port.number}"


def test_a_listener_answers_and_is_in_use():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(8)
        port = Port(listener.getsockname()[1])

        assert port.answers is True
        assert port.in_use is True


def test_an_ipv6_only_listener_is_in_use():
    """A listener bound only to ``::1``, as a Nuxt dev server binds on macOS, is found."""
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as listener:
        listener.bind(("::1", 0))
        listener.listen(8)

        assert Port(listener.getsockname()[1]).in_use is True


def test_a_stack_the_host_lacks_is_skipped(monkeypatch: pytest.MonkeyPatch):
    def unsupported(family: int, kind: int) -> Any:
        raise OSError("address family not supported")

    monkeypatch.setattr(socket, "socket", unsupported)

    assert Port(1).in_use is False
