from abc import ABC, abstractmethod
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from typing import Any


class Observer(ABC):
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
            observer.on_event(event)


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
    def emit(observable: "Observable", name: str) -> None:
        event = Event(observable, name)
        observable.notify_observers(event)

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

    @staticmethod
    @contextmanager
    def context(observable: "Observable", name: str) -> Generator[None]:
        if not isinstance(observable, Observable):
            raise TypeError("Event context can only be used with Observable classes")
        event = Event(observable, name, EventStatus.START)
        observable.notify_observers(event)
        try:
            yield
        except Exception:
            observable.notify_observers(event.with_status(EventStatus.FAILURE))
            raise
        else:
            observable.notify_observers(event.with_status(EventStatus.SUCCESS))
