import warnings

with warnings.catch_warnings():
    # google-adk's own config classes subclass their deprecated BaseAgentConfig,
    # tripping four DeprecationWarnings on first import (their bug, present through
    # 2.4.0 and main). Import it once here with exactly that message contained, so
    # no consumer of this package sees the noise — while a genuine use of the
    # deprecated class in our code would still warn at its own definition site.
    warnings.filterwarnings("ignore", message="BaseAgentConfig is deprecated", category=DeprecationWarning)
    import google.adk.agents  # noqa: F401

from interloper_agent import agent
from interloper_agent.context import init, set_catalog, set_store

__all__ = ["agent", "init", "set_catalog", "set_store"]
