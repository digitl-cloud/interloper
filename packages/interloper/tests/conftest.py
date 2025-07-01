"""This module contains the pytest configuration."""


def pytest_configure():
    """Configure pytest."""
    import sys
    from unittest.mock import MagicMock

    class DummyBus:
        def subscribe(self, *a, **kw):
            pass

        def unsubscribe(self, *a, **kw):
            pass

        def event(self, *a, **kw):
            def decorator(fn):
                return fn

            return decorator

    # Patch the module before any imports
    if "interloper.events.bus" in sys.modules:
        sys.modules["interloper.events.bus"].get_event_bus = lambda: DummyBus()
    else:
        # Create a mock module if it doesn't exist yet
        mock_module = MagicMock()
        mock_module.get_event_bus = lambda: DummyBus()
        sys.modules["interloper.events.bus"] = mock_module
