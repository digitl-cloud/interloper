import threading
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from queue import Queue
from typing import Any


class ExecutionStep(Enum):
    ASSET_EXECUTION = "ASSET_EXECUTION"
    ASSET_NORMALIZATION = "ASSET_NORMALIZATION"
    ASSET_WRITING = "ASSET_WRITING"
    PIPELINE_MATERIALIZATION = "PIPELINE_MATERIALIZATION"
    PIPELINE_BACKFILL = "PIPELINE_BACKFILL"

    def __str__(self) -> str:
        return self.value


class ExecutionStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"

    def __str__(self) -> str:
        return self.value


@dataclass
class Event:
    observable: "Observable | None"
    step: ExecutionStep
    status: ExecutionStatus | None = None
    error: Exception | None = None

    def __enter__(self) -> None:
        if self.observable is None or not isinstance(self.observable, Observable):
            raise ValueError("Event must be bound to an observable before use")

        self.status = ExecutionStatus.RUNNING
        self.observable.notify_observers(Event(self.observable, self.step, self.status, self.error))

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.observable is None or not isinstance(self.observable, Observable):
            raise ValueError("Event must be bound to an observable before use")

        self.status = ExecutionStatus.FAILURE if exc_type else ExecutionStatus.SUCCESS
        self.error = exc_val if exc_type else None
        self.observable.notify_observers(Event(self.observable, self.step, self.status, self.error))


class Observer(ABC):
    def __init__(self, is_async: bool = False) -> None:
        self._is_async = is_async
        self._queue = Queue()
        if is_async:
            # TODO: review whether we should use daemon threads
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
    def __init__(self, observers: list[Observer] | None = None) -> None:
        self._observers: list[Observer] = observers or []

    def add_observer(self, observer: Observer) -> None:
        if not isinstance(observer, Observer):
            raise TypeError("Observer must implement the Observer interface")
        self._observers.append(observer)

    def remove_observer(self, observer: Observer) -> None:
        self._observers.remove(observer)

    def notify_observers(self, event: Event) -> None:
        for observer in self._observers:
            if observer._is_async:
                # In async mode, queue the event for processing in the background thread
                observer._queue.put(event)
            else:
                # In sync mode, process events immediately in the current thread
                observer.on_event(event)

    def emit(self, name: str, step: ExecutionStep, status: ExecutionStatus) -> None:
        event = Event(self, step, status)
        self.notify_observers(event)

    @staticmethod
    def event(
        func: Callable | None = None,
        *,
        step: ExecutionStep,
    ) -> Callable:
        """
        Decorator
        """

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(self: Observable, *args: Any, **kwargs: Any) -> Any:
                if not isinstance(self, Observable):
                    raise TypeError("observable_event decorator can only be used with Observable classes")

                with Event(self, step):
                    return func(self, *args, **kwargs)

            return wrapper

        if func is None:
            return decorator
        return decorator(func)
