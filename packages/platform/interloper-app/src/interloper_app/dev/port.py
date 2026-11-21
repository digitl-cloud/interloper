"""A port on the loopback interface, the only address ``interloper dev`` and what it starts listen on."""

from __future__ import annotations

import socket
from dataclasses import dataclass
from typing import ClassVar


@dataclass(frozen=True)
class Port:
    """A TCP port of ``127.0.0.1``, so nothing ``dev`` serves is reachable from another machine.

    Attributes:
        number: The port number.
    """

    HOST: ClassVar[str] = "127.0.0.1"

    number: int

    @classmethod
    def free(cls) -> Port:
        """Pick a port nothing listens on.

        Returns:
            The port.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind((cls.HOST, 0))
            return cls(probe.getsockname()[1])

    @property
    def origin(self) -> str:
        """The origin of what is served on this port.

        Returns:
            ``http://127.0.0.1:<number>``.
        """
        return f"http://{self.HOST}:{self.number}"

    @property
    def answers(self) -> bool:
        """Whether something accepts connections on this port of ``127.0.0.1``.

        Returns:
            True once something listens there.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.settimeout(0.5)
            return probe.connect_ex((self.HOST, self.number)) == 0

    @property
    def in_use(self) -> bool:
        """Whether this port is taken on either loopback, IPv4 or IPv6.

        Both stacks are probed: a Nuxt dev server binds only ``::1`` on macOS,
        so an IPv4 probe alone would miss it.

        Returns:
            True when a connection to it succeeds on either loopback.
        """
        for family, address in ((socket.AF_INET, self.HOST), (socket.AF_INET6, "::1")):
            try:
                with socket.socket(family, socket.SOCK_STREAM) as probe:
                    probe.settimeout(0.5)
                    if probe.connect_ex((address, self.number)) == 0:
                        return True
            except OSError:
                continue
        return False
