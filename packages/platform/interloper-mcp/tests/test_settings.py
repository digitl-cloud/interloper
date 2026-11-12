"""Tests for ``interloper_mcp.settings``."""

import pytest

from interloper_mcp.settings import McpSettings


@pytest.fixture(autouse=True)
def _no_ambient_yaml(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """Run each test where no ``interloper.yaml`` can be discovered."""
    monkeypatch.chdir(tmp_path)


def test_the_mcp_package_declares_its_section():
    """The section is read from the ``mcp`` block and the ``INTERLOPER_MCP_`` variables."""
    assert McpSettings.section() == "mcp"
    assert McpSettings.declared()["mcp"].extension == "mcp"
    assert {name.partition(".")[0] for name in McpSettings.public} <= set(McpSettings.model_fields)


def test_mcp_settings_env_override(monkeypatch: pytest.MonkeyPatch):
    """INTERLOPER_MCP_* env vars reach the mcp section."""
    monkeypatch.setenv("INTERLOPER_MCP_PORT", "3999")

    assert McpSettings.get().port == 3999
