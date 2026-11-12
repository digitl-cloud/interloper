"""Tests for ``interloper_mcp.server``: how the server introduces itself to a client.

Authentication and the protected-resource metadata are covered in
``tests/test_auth.py``, the tools in ``tests/test_tools.py``.
"""

from __future__ import annotations

from importlib.metadata import version

from mcp import Client
from mcp.types import Icon

from interloper_mcp.server import create_mcp_server


async def test_the_server_introduces_itself_with_the_apps_title_url_and_icon():
    server = create_mcp_server("https://app.example.com")

    async with Client(server) as client:
        info = client.server_info

    assert info is not None
    assert info.name == "interloper"
    assert info.title == "Interloper"
    assert info.version == version("interloper-mcp")
    assert info.website_url == "https://app.example.com"
    assert info.icons == [Icon(src="https://app.example.com/icon.svg", mime_type="image/svg+xml", sizes=["any"])]
