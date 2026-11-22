"""Interloper Agent: the assistant, on pydantic-ai, over the shared toolkit.

The package is imported whenever the API reads its ``agent`` settings
section, enabled or not, so it imports nothing here: the agent is
`interloper_agent.agent`, built only when the section enables it.
"""
