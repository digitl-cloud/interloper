import threading
from abc import ABC, abstractmethod
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from queue import Queue
from typing import Any


class EventStatus(Enum):
    START = "START"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"

    def __str__(self) -> str:
        return self.value


@dataclass
class Event:
    observable: "Observable"
    name: str
    status: EventStatus | None = None

    def with_status(self, status: EventStatus) -> "Event":
        return Event(self.observable, self.name, status)

    @staticmethod
    def wrap(
        func: Callable | None = None,
        *,
        name: str | None = None,
    ) -> Callable:
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(self: Observable, *args: Any, **kwargs: Any) -> Any:
                if not isinstance(self, Observable):
                    raise TypeError("observable_event decorator can only be used with Observable classes")

                event = Event(self, name or func.__name__, EventStatus.START)
                self.notify_observers(event)
                try:
                    result = func(self, *args, **kwargs)
                    self.notify_observers(event.with_status(EventStatus.SUCCESS))
                    return result
                except Exception as e:
                    self.notify_observers(event.with_status(EventStatus.FAILURE))
                    raise e

            return wrapper

        if func is None:
            return decorator
        return decorator(func)


class Observer(ABC):
    def __init__(self, is_async: bool = False) -> None:
        self._is_async = is_async
        self._queue = Queue()
        if is_async:
            self._thread = threading.Thread(target=self._process_events, daemon=True)
            self._thread.start()

    def _process_events(self) -> None:
        while True:
            event = self._queue.get()
            if event is None:  # Shutdown signal
                break
            self.on_event(event)

    def __del__(self) -> None:
        if self._is_async:
            self._queue.put(None)  # Signal shutdown
            self._thread.join()

    @abstractmethod
    def on_event(self, event: "Event") -> None: ...


class Observable:
    def __init__(self) -> None:
        self._observers: list[Observer] = []

    def add_observer(self, observer: Observer) -> None:
        if not isinstance(observer, Observer):
            raise TypeError("Observer must implement the Observer interface")
        self._observers.append(observer)

    def remove_observer(self, observer: Observer) -> None:
        self._observers.remove(observer)

    def notify_observers(self, event: "Event") -> None:
        for observer in self._observers:
            if observer._is_async:
                # In async mode, queue the event for processing in the background thread
                observer._queue.put(event)
            else:
                # In sync mode, process events immediately in the current thread
                observer.on_event(event)

    def emit_event(self, name: str, status: EventStatus | None = None) -> None:
        event = Event(self, name, status)
        self.notify_observers(event)

    @contextmanager
    def event_context(self, name: str) -> Generator[None]:
        event = Event(self, name, EventStatus.START)
        self.notify_observers(event)
        try:
            yield
        except Exception:
            self.notify_observers(event.with_status(EventStatus.FAILURE))
            raise
        else:
            self.notify_observers(event.with_status(EventStatus.SUCCESS))
