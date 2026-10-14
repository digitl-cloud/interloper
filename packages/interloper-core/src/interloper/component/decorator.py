"""Decorator: the one way a decorator builds a class of its kind."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Generic, TypeVar, overload

from interloper.serializable import ClassBody, Serializable

SerializableT = TypeVar("SerializableT", bound=Serializable)
SubclassT = TypeVar("SubclassT", bound=Serializable)


class Decorator(Generic[SerializableT]):
    """A decorator building classes of one kind, used bare or with settings.

    Called with a target (a class or a function), it reads the target into a
    `ClassBody` and builds a new class of its kind from it with the settings it
    carries. Called with settings, it returns a decorator carrying them, so the
    parenthesised form needs no nested function:

    ```py
    asset = Decorator(Asset)

    @asset
    def rows(): ...

    @asset(tags=["Report"], relations={"destinations": [BigQueryDestination]})
    def users_stats(): ...
    ```

    What the settings may be is the kind's own: its public ClassVars, its
    field defaults and, for a component, ``relations=``. A class that already
    extends the kind is the one consulted, so a subclass's own settings
    (``oauth`` on an ``OAuthConnection``) are accepted.
    """

    def __init__(self, anchor: type[SerializableT], **settings: Any) -> None:
        """Make a decorator for one kind.

        Args:
            anchor: The kind the decorator builds.
            **settings: The settings every class it builds receives.
        """
        self.anchor = anchor
        self.settings = settings

    @overload
    def __call__(self, target: type[SubclassT], /) -> type[SubclassT]: ...
    @overload
    def __call__(self, target: type | Callable[..., Any], /) -> type[SerializableT]: ...
    @overload
    def __call__(self, /, *, relations: dict[str, Any] | None = ..., **overrides: Any) -> Decorator[SerializableT]: ...
    def __call__(self, target: Any = None, /, **settings: Any) -> Any:
        """Build a class from a target, or return a decorator carrying more settings.

        Args:
            target: The decorated class or function; ``None`` in the
                parenthesised form.
            **settings: More settings, added to the ones this decorator carries.

        Returns:
            The built class, or the decorator carrying the settings.
        """
        settings = {**self.settings, **settings}
        if target is None:
            return Decorator(self.anchor, **settings)
        body = ClassBody.of(target, self.anchor)
        return (body.base or self.anchor).build_class(body, **settings)
