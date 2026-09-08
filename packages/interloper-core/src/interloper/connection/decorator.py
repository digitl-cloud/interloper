"""Decorator for creating Connection subclasses from plain classes."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar, overload

from interloper.component.decorator import declare, decorate
from interloper.component.relation import Relation
from interloper.connection.base import Connection, OAuthConnection

# Bounded TypeVar so that classes already extending Connection preserve their
# specific type through the decorator.  Plain classes fall through to the
# fallback overload and get type[Connection].
ConnectionT = TypeVar("ConnectionT", bound=Connection)


@overload
def connection(cls: type[ConnectionT], /) -> type[ConnectionT]: ...
@overload
def connection(cls: type, /) -> type[Connection]: ...
@overload
def connection(
    *,
    relations: dict[str, Any] = ...,
    **overrides: Any,
) -> Callable[[type[ConnectionT]], type[ConnectionT]]: ...
def connection(
    cls: type | None = None,
    /,
    *,
    relations: dict[str, Any] | None = None,
    **overrides: Any,
) -> type[Connection] | Callable[[type], type[Connection]]:
    """Create a Connection subclass from a decorated class.

    Can be used bare or with arguments::

        @connection
        class MyConnection:
            host: str = "localhost"
            port: int = 5432
            username: str
            password: str

        @connection(key="custom", name="Custom Connection")
        class OtherConnection:
            url: str

    Class-level traits, identity (key, name, icon, tags) and behavior
    (oauth), belong in the decorator; the class body declares fields::

        @connection(
            name="Amazon Ads",
            oauth=OAuthConfig("amazon", scope="advertising::campaign_management"),
        )
        class AmazonAdsConnection(OAuthConnection):
            location: str = SelectField(...)

    The decorated class's annotations and attributes become the Connection
    subclass body.  Since Connection extends ``BaseSettings``, fields can
    still be loaded from environment variables.

    Args:
        cls: The decorated class when used bare; ``None`` when called with
            arguments, which returns the decorator instead.
        relations: Relation name to a
            :class:`~interloper.component.relation.Relation`, a component class
            (the shorthand for a relation on it), or a list of component
            classes narrowing the relation the decorated class declares under
            that name. Explicit declarations win over the class annotations.
        **overrides: Definition metadata and behaviour: the public ClassVars
            and field defaults the decorated class declares, or
            :class:`~interloper.connection.base.Connection` itself for a plain
            class (``key``, ``name``, ``icon``, ``tags``, ``auto_renew``, and
            ``oauth`` on an
            :class:`~interloper.connection.base.OAuthConnection`); see the
            class. An unknown name is a ``TypeError`` at decoration.

    Returns:
        A Connection subclass.  Building it fails with a TypeError if
        ``oauth.fields`` maps token response keys to model fields the
        class does not declare.
    """
    if cls is not None:
        return decorate(Connection, cls, build=_build_connection, relations=relations, **overrides)

    def wrapper(cls: type) -> type[Connection]:
        # `oauth` is only a ClassVar of OAuthConnection, so the routing would
        # otherwise report it as an unknown name on a plain Connection; say
        # what is actually wrong instead.
        if "oauth" in overrides and not (isinstance(cls, type) and issubclass(cls, OAuthConnection)):
            raise TypeError(f"{cls.__name__}: oauth=... requires subclassing OAuthConnection, not Connection.")
        return decorate(Connection, cls, build=_build_connection, relations=relations, **overrides)

    return wrapper


# -- Internals -----------------------------------------------------------------


def _build_connection(
    cls: type,
    *,
    classvars: dict[str, Any],
    fields: dict[str, Any],
    relations: dict[str, Relation],
) -> type[Connection]:
    """Build the Connection subclass and declare the decorator's relations on it.

    Args:
        cls: The decorated class.
        classvars: Class-level attributes to stamp on the built class.
        fields: Field default overrides for the built class.
        relations: Relations the decorator declares, name to relation.

    Returns:
        A Connection subclass.
    """
    return declare(Connection.build_class(cls, classvars=classvars, fields=fields), relations)
