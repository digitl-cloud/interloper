"""The MCP server's settings section."""

from __future__ import annotations

from typing import ClassVar

from interloper.settings import Settings
from pydantic import Field


class McpSettings(Settings):
    """MCP server settings (``interloper-mcp``).

    ``token`` only applies to the stdio transport, which authenticates once
    at startup.
    """

    public: ClassVar[frozenset[str]] = frozenset({"host", "port"})

    host: str = Field(default="0.0.0.0", description="Bind host.")
    port: int = Field(default=3001, description="Bind port.")
    token: str = Field(default="", description="Personal access token the stdio transport authenticates with.")
