"""Tests for ``interloper_mcp.server``: how the server introduces itself to a client.

Authentication and the protected-resource metadata are covered in
``tests/test_auth.py``, the tools in ``tests/test_tools.py``.
"""

from __future__ import annotations

from importlib.metadata import version

from interloper.settings import AppSettings, McpSettings, ServerSettings
from mcp import Client
from mcp.types import Icon

from interloper_mcp.server import create_mcp_server


async def test_the_server_introduces_itself_with_the_apps_title_url_and_icon():
    AppSettings.activate(AppSettings(server=ServerSettings(external_url="https://app.example.com/")))
    try:
        server = create_mcp_server(McpSettings())
    finally:
        AppSettings.clear_active()

    async with Client(server) as client:
        info = client.server_info

    assert info is not None
    assert info.name == "interloper"
    assert info.title == "Interloper"
    assert info.version == version("interloper-mcp")
    assert info.website_url == "https://app.example.com"
    assert info.icons == [Icon(src="https://app.example.com/icon.svg", mime_type="image/svg+xml", sizes=["any"])]


async def test_without_an_external_url_the_server_points_at_the_local_app():
    AppSettings.activate(AppSettings(server=ServerSettings(external_url="", port=3100)))
    try:
        server = create_mcp_server(McpSettings())
    finally:
        AppSettings.clear_active()

    async with Client(server) as client:
        info = client.server_info

    assert info is not None
    assert info.website_url == "http://localhost:3100"
    assert info.icons == [Icon(src="http://localhost:3100/icon.svg", mime_type="image/svg+xml", sizes=["any"])]
