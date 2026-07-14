"""Tests for interloper_agent.agent."""

import datetime
from typing import cast

from google.adk.agents.readonly_context import ReadonlyContext

from interloper_agent import agent as agent_module


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
