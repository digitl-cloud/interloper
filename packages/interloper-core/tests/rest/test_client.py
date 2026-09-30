"""Tests for ``interloper.rest.client``."""

from __future__ import annotations

import httpx2

from interloper.rest.client import AsyncRESTClient


class TestAsyncRESTClient:
    """The async counterpart shares RESTClient's construction surface."""

    async def test_applies_base_url_headers_and_params(self):
        seen: dict[str, str | None] = {}

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen["path"] = request.url.path
            seen["token"] = request.headers.get("Access-Token")
            seen["a"] = request.url.params.get("a")
            return httpx2.Response(200, json={"ok": True})

        client = AsyncRESTClient(
            "https://api.test",
            headers={"Access-Token": "tok"},
            transport=httpx2.MockTransport(handler),
        )
        async with client:
            response = await client.get("/things", params={"a": "1"})

        assert response.json() == {"ok": True}
        assert seen == {"path": "/things", "token": "tok", "a": "1"}
