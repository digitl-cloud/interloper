"""Shared Slack transport fake.

Every Slack call goes through the connection's ``client``, so the fake swaps
the ``RESTClient`` that property builds for one on a ``MockTransport`` —
base URL, bearer auth and pagination stay real, only the wire is faked.
"""

from __future__ import annotations

import json as jsonlib
from typing import Any

import httpx
import pytest

from interloper_slack import connection as connection_module


class FakeSlack:
    """Records Slack requests and serves scripted responses.

    Responses are consumed in order; once the script runs out every further
    call gets a bare ``{"ok": true}``, so a test only scripts what it asserts.
    """

    def __init__(self) -> None:
        """Start with an empty script and no recorded traffic."""
        self.requests: list[httpx.Request] = []
        self._script: list[httpx.Response] = []

    # -- Scripting -------------------------------------------------------------

    def ok(self, **payload: Any) -> FakeSlack:
        """Queue a successful response carrying *payload*.

        Returns:
            The fake, for chaining.
        """
        return self.raw(httpx.Response(200, json={"ok": True, **payload}))

    def error(self, code: str) -> FakeSlack:
        """Queue a Slack rejection — HTTP 200 with ``ok: false``.

        Returns:
            The fake, for chaining.
        """
        return self.raw(httpx.Response(200, json={"ok": False, "error": code}))

    def raw(self, response: httpx.Response) -> FakeSlack:
        """Queue an arbitrary response (transport failures, malformed bodies).

        Returns:
            The fake, for chaining.
        """
        self._script.append(response)
        return self

    # -- Inspection ------------------------------------------------------------

    @property
    def endpoints(self) -> list[str]:
        """The Slack endpoints called, in order.

        Returns:
            The request paths, oldest first.
        """
        return [request.url.path.removeprefix("/api/") for request in self.requests]

    def params(self, index: int = 0) -> dict[str, str]:
        """The query params of the *index*-th request.

        Returns:
            The params as a plain dict.
        """
        return dict(self.requests[index].url.params)

    def json_body(self, index: int = 0) -> dict[str, Any]:
        """The JSON body of the *index*-th request.

        Returns:
            The decoded JSON body.
        """
        return jsonlib.loads(self.requests[index].content)

    def auth(self, index: int = 0) -> str | None:
        """The Authorization header of the *index*-th request.

        Returns:
            The header value, or ``None`` when absent.
        """
        return self.requests[index].headers.get("Authorization")

    def timeout(self, index: int = 0) -> Any:
        """The timeout httpx resolved for the *index*-th request.

        Returns:
            The resolved timeout.
        """
        return self.requests[index].extensions.get("timeout")

    # -- Transport -------------------------------------------------------------

    def handle(self, request: httpx.Request) -> httpx.Response:
        """Record *request* and answer with the next scripted response.

        Returns:
            The next scripted response.
        """
        self.requests.append(request)
        return self._script.pop(0) if self._script else httpx.Response(200, json={"ok": True})


@pytest.fixture
def slack(monkeypatch: pytest.MonkeyPatch) -> FakeSlack:
    """Give the connection a real RESTClient wired to a recording transport.

    Returns:
        The recording fake the test scripts and inspects.
    """
    fake = FakeSlack()
    real = connection_module.RESTClient

    def factory(*args: Any, **kwargs: Any) -> Any:
        return real(*args, transport=httpx.MockTransport(fake.handle), **kwargs)

    monkeypatch.setattr(connection_module, "RESTClient", factory)
    return fake
