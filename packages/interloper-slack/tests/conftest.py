"""Shared Slack transport fake.

``post`` / ``apost`` take the caller's client, so the fake intercepts client
*construction* — what ``SlackHook.fire`` and the connection's providers do
internally — and serves scripted responses off a ``MockTransport``. One fake
covers both colours, since ``MockTransport`` handles sync and async alike.
"""

from __future__ import annotations

import json as jsonlib
from typing import Any
from urllib.parse import parse_qsl

import httpx
import pytest


class FakeSlack:
    """Records Slack requests and serves scripted responses.

    Responses are consumed in order; once the script runs out every further
    call gets a bare ``{"ok": true}``, so a test only scripts what it asserts.
    """

    def __init__(self) -> None:
        """Start with an empty script and no recorded traffic."""
        self.requests: list[httpx.Request] = []
        self.client_kwargs: list[dict[str, Any]] = []
        self._script: list[httpx.Response] = []

    # -- Scripting -------------------------------------------------------------

    def ok(self, **payload: Any) -> FakeSlack:
        """Queue a successful response carrying *payload*."""
        return self.raw(httpx.Response(200, json={"ok": True, **payload}))

    def error(self, code: str) -> FakeSlack:
        """Queue a Slack rejection — HTTP 200 with ``ok: false``."""
        return self.raw(httpx.Response(200, json={"ok": False, "error": code}))

    def raw(self, response: httpx.Response) -> FakeSlack:
        """Queue an arbitrary response (transport failures, malformed bodies)."""
        self._script.append(response)
        return self

    # -- Inspection ------------------------------------------------------------

    @property
    def endpoints(self) -> list[str]:
        """The Slack endpoints called, in order."""
        return [request.url.path.removeprefix("/api/") for request in self.requests]

    def json_body(self, index: int = 0) -> dict[str, Any]:
        """The JSON body of the *index*-th request."""
        return jsonlib.loads(self.requests[index].content)

    def form_body(self, index: int = 0) -> dict[str, str]:
        """The form-encoded body of the *index*-th request."""
        return dict(parse_qsl(self.requests[index].content.decode()))

    def auth(self, index: int = 0) -> str | None:
        """The Authorization header of the *index*-th request."""
        return self.requests[index].headers.get("Authorization")

    # -- Transport -------------------------------------------------------------

    def handle(self, request: httpx.Request) -> httpx.Response:
        """Record *request* and answer with the next scripted response."""
        self.requests.append(request)
        return self._script.pop(0) if self._script else httpx.Response(200, json={"ok": True})


@pytest.fixture
def slack(monkeypatch: pytest.MonkeyPatch) -> FakeSlack:
    """Route every httpx client this package builds through a recording transport."""
    fake = FakeSlack()

    for attr in ("Client", "AsyncClient"):
        real = getattr(httpx, attr)

        # Bind the real class per iteration: patching httpx in place means a
        # late lookup would find the factory and recurse.
        def factory(_real: Any = real, **kwargs: Any) -> Any:
            fake.client_kwargs.append(kwargs)
            return _real(transport=httpx.MockTransport(fake.handle))

        monkeypatch.setattr(httpx, attr, factory)

    return fake
