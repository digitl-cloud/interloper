"""Package-first import: google.adk must enter through interloper_agent.

The package __init__ contains google-adk's self-inflicted BaseAgentConfig
deprecation noise at the import site; test modules importing google.adk
directly would otherwise trigger it first, before the containment runs.
"""

import interloper_agent  # noqa: F401
