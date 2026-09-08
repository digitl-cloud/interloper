"""Tests for interloper_agent.agent."""

import datetime
from typing import cast

from google.adk.agents.base_agent import BaseAgent
from google.adk.agents.readonly_context import ReadonlyContext

from interloper_agent import agent as agent_module


def _tool_names(agent: BaseAgent) -> set[str]:
    """The names an agent's registered tools carry.

    Args:
        agent: The agent whose tool list is read.

    Returns:
        One name per registered tool: a plain function's ``__name__``, or a
        tool object's own ``name`` (an ``AgentTool``, say).
    """
    tools = getattr(agent, "tools", [])
    return {getattr(tool, "__name__", None) or getattr(tool, "name", "") for tool in tools}


def test_with_current_time_appends_now():
    provider = agent_module.with_current_time("BASE")
    text = provider(cast(ReadonlyContext, None))
    now = datetime.datetime.now(datetime.timezone.utc)
    assert text.startswith("BASE\n")
    assert f"Current date and time: {now:%Y-%m-%d}" in text
    assert "Compute relative timestamps from it" in text


def test_all_agents_carry_the_current_time():
    agents = [
        agent_module.root_agent,
        agent_module.catalog_agent,
        agent_module.collection_agent,
        agent_module.lineage_agent,
        agent_module.scheduling_agent,
        agent_module.analytics_agent,
    ]
    for agent in agents:
        instruction = agent.instruction
        assert not isinstance(instruction, str), f"{agent.name} has a static instruction"
        text = instruction(cast(ReadonlyContext, None))
        assert isinstance(text, str)
        assert "Current date and time:" in text


def test_the_collection_agent_registers_the_relation_write_tools():
    # The toolkit's only write functions: exposed here, never on the
    # deliberately read-only MCP server.
    assert {"bind_relation", "unbind_relation"} <= _tool_names(agent_module.collection_agent)
