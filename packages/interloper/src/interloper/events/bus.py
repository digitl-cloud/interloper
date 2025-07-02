"""This module contains the EventBus and Subscription classes."""
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from functools import wraps
from queue import Queue
from threading import RLock, Thread
from typing import Any

from interloper.events.event import Event, EventType
from interloper.execution.state import ExecutionStatus

DEFAULT_BUS_NAME = "_default_"

_bus = {}
_lock = RLock()


class Subscription:
    """A subscription to an event bus."""

    def __init__(
        self,
        on_event: Callable[[Event], None],
        is_async: bool = False,
    ) -> None:
        """Initialize the subscription.

        Args:
            on_event: The callback to execute when an event is received.
            is_async: Whether to process events asynchronously.
        """
        self.on_event = on_event
        self._is_async = is_async

        if is_async:
            self._queue = Queue[Event]()
            self._thread = Thread(target=self._process_events, daemon=True)
            self._thread.start()

    def _process_events(self) -> None:
        while True:
            event = self._queue.get()
            if event is None:
                break
            self.on_event(event)

    def __del__(self) -> None:
        """Clean up the subscription."""
        if self._is_async:
            self._thread.join()


class EventBus:
    """A simple event bus."""

    def __init__(self, name: str):
        """Initialize the event bus.

        Args:
            name: The name of the event bus.
        """
        self.name = name
        self._subscriptions: set[Subscription] = set()

    def subscribe(
        self,
        callback: Callable[[Event], None],
        is_async: bool = False,
    ) -> None:
        """Subscribe to the event bus.

        Args:
            callback: The callback to execute when an event is received.
            is_async: Whether to process events asynchronously.
        """
        self._subscriptions.add(Subscription(callback, is_async))

    def unsubscribe(self, callback: Callable[[Event], None]) -> None:
        """Unsubscribe from the event bus.

        Args:
            callback: The callback to unsubscribe.
        """
        for subscription in self._subscriptions:
            if subscription.on_event == callback:
                self._subscriptions.remove(subscription)
                break

    def publish(self, event: Event) -> None:
        """Publish an event to the event bus.

        Args:
            event: The event to publish.
        """
        for subscriber in self._subscriptions:
            if subscriber._is_async:
                subscriber._queue.put(event)
            else:
                subscriber.on_event(event)

    def event(self, type: EventType) -> Callable:
        """A decorator or context manager to publish events.

        Args:
            type: The type of event to publish.

        Returns:
            A decorator or context manager.
        """

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                self.publish(Event(type, ExecutionStatus.RUNNING))
                try:
                    result = func(*args, **kwargs)
                    self.publish(Event(type, ExecutionStatus.SUCCESSFUL))
                    return result
                except Exception as e:
                    self.publish(Event(type, ExecutionStatus.FAILED, error=e))
                    raise

            return wrapper

        @contextmanager
        def context_manager() -> Iterator[None]:
            self.publish(Event(type, ExecutionStatus.RUNNING))
            try:
                yield
                self.publish(Event(type, ExecutionStatus.SUCCESSFUL))
            except Exception as e:
                self.publish(Event(type, ExecutionStatus.FAILED, error=e))
                raise

        return context_manager() if not callable(type) else decorator(type)


def get_event_bus(name: str | None = None) -> EventBus:
    """Get an event bus by name.

    Args:
        name: The name of the event bus.

    Returns:
        The event bus.
    """
    if name is None:
        name = DEFAULT_BUS_NAME

    with _lock:
        if name not in _bus:
            _bus[name] = EventBus(name)
        return _bus[name]
